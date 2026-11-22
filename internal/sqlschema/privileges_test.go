package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// privilege is one grant or revoked grant as these rows compare it: who, what,
// and on which object, with the target spelled by [schemamodel.Grant.TargetKey]
// so a routine's argument types are part of it.
type privilege struct {
	Role       string
	Target     string
	Privileges []string
	WithOption bool
}

func privileges(grants []schemamodel.Grant) []privilege {
	if len(grants) == 0 {
		return nil
	}
	out := make([]privilege, 0, len(grants))
	for _, grant := range grants {
		out = append(out, privilege{
			Role: grant.Role, Target: grant.TargetKey(), Privileges: grant.Privileges, WithOption: grant.WithOption,
		})
	}
	return out
}

// TestRead_GrantAndRevoke_HappyPath pins how a schema file's GRANT and REVOKE
// statements compose. A schema file is a script: the later statement about one
// privilege of one grantee on one object wins, and a REVOKE that follows no
// GRANT still says the privilege is absent, because PostgreSQL hands out
// privileges nobody granted -- EXECUTE to PUBLIC on a new function, and what
// ALTER DEFAULT PRIVILEGES gives a role on a new table.
func TestRead_GrantAndRevoke_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantGrants  []privilege
		wantRevoked []privilege
	}{
		{
			name: "the SECURITY DEFINER pair wpmgr writes",
			sql: "REVOKE ALL ON FUNCTION purge_workspace(uuid) FROM PUBLIC;\n" +
				"GRANT EXECUTE ON FUNCTION purge_workspace(uuid) TO wpmgr_app;",
			wantGrants:  []privilege{{Role: "wpmgr_app", Target: "ROUTINE purge_workspace(uuid)", Privileges: []string{"EXECUTE"}}},
			wantRevoked: []privilege{{Role: "PUBLIC", Target: "ROUTINE purge_workspace(uuid)", Privileges: []string{"ALL"}}},
		},
		{
			name: "a table revoke with no grant before it is kept, one privilege each",
			sql:  "REVOKE INSERT, UPDATE, DELETE ON plugin_signatures FROM wpmgr_app;",
			wantRevoked: []privilege{
				{Role: "wpmgr_app", Target: "TABLE plugin_signatures", Privileges: []string{"INSERT"}},
				{Role: "wpmgr_app", Target: "TABLE plugin_signatures", Privileges: []string{"UPDATE"}},
				{Role: "wpmgr_app", Target: "TABLE plugin_signatures", Privileges: []string{"DELETE"}},
			},
		},
		{
			name:        "a revoke after a grant takes the privilege out of it",
			sql:         "GRANT SELECT, INSERT ON t TO r;\nREVOKE INSERT ON t FROM r;",
			wantGrants:  []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
			wantRevoked: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"INSERT"}}},
		},
		{
			name:       "a grant after a revoke leaves the privilege held",
			sql:        "REVOKE SELECT ON t FROM r;\nGRANT SELECT ON t TO r;",
			wantGrants: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
		{
			name:        "REVOKE ALL on a table is the absence of every privilege on it",
			sql:         "REVOKE ALL PRIVILEGES ON TABLE t FROM r;",
			wantRevoked: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"ALL"}}},
		},
		{
			name:        "REVOKE ALL takes every earlier grant on the table and its columns",
			sql:         "GRANT SELECT, INSERT ON t TO r;\nGRANT UPDATE (a) ON t TO r;\nGRANT SELECT ON u TO r;\nREVOKE ALL ON t FROM r;",
			wantGrants:  []privilege{{Role: "r", Target: "TABLE u", Privileges: []string{"SELECT"}}},
			wantRevoked: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"ALL"}}},
		},
		{
			name:        "a grant after REVOKE ALL is the one privilege held",
			sql:         "REVOKE ALL ON t FROM r;\nGRANT SELECT ON t TO r;",
			wantGrants:  []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
			wantRevoked: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"ALL"}}},
		},
		{
			name:        "REVOKE ALL after GRANT ALL",
			sql:         "GRANT ALL ON t TO r;\nREVOKE ALL ON t FROM r;",
			wantRevoked: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"ALL"}}},
		},
		{
			name: "GRANT OPTION FOR ALL clears the option on what the file granted",
			sql:  "GRANT SELECT, INSERT ON t TO r WITH GRANT OPTION;\nREVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantGrants: []privilege{
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"INSERT"}},
			},
		},
		{
			name:       "GRANT OPTION FOR clears the option and keeps the privilege",
			sql:        "GRANT SELECT ON t TO r WITH GRANT OPTION;\nREVOKE GRANT OPTION FOR SELECT ON t FROM r;",
			wantGrants: []privilege{{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}}},
		},
		{
			name: "GRANT OPTION FOR ALL beside a column grant without the option",
			sql:  "GRANT SELECT ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t TO r;\nREVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantGrants: []privilege{
				{Role: "r", Target: "TABLE t (name)", Privileges: []string{"UPDATE"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
			},
		},
		{
			name: "GRANT OPTION FOR a privilege the column grant does not carry",
			sql: "GRANT SELECT ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR SELECT ON t FROM r;",
			wantGrants: []privilege{
				{Role: "r", Target: "TABLE t (name)", Privileges: []string{"UPDATE"}, WithOption: true},
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
			},
		},
		{
			name:       "GRANT OPTION FOR on the columns that carry it",
			sql:        "GRANT UPDATE (name) ON t TO r WITH GRANT OPTION;\nREVOKE GRANT OPTION FOR UPDATE (name) ON t FROM r;",
			wantGrants: []privilege{{Role: "r", Target: "TABLE t (name)", Privileges: []string{"UPDATE"}}},
		},
		{
			name: "GRANT OPTION FOR ALL after the option is revoked on the columns",
			sql: "GRANT SELECT ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR UPDATE (name) ON t FROM r;\nREVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantGrants: []privilege{
				{Role: "r", Target: "TABLE t (name)", Privileges: []string{"UPDATE"}},
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
			},
		},
		{
			name: "GRANT OPTION FOR ALL beside a column grant on a table in another schema",
			sql: "GRANT SELECT ON a.t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON b.t TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON a.t FROM r;",
			wantGrants: []privilege{
				{Role: "r", Target: "TABLE b.t (name)", Privileges: []string{"UPDATE"}, WithOption: true},
				{Role: "r", Target: "TABLE a.t", Privileges: []string{"SELECT"}},
			},
		},
		{
			name: "GRANT OPTION FOR ALL beside a column grant on a quoted table of another case",
			sql: "GRANT SELECT ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON \"T\" TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantGrants: []privilege{
				{Role: "r", Target: "TABLE T (name)", Privileges: []string{"UPDATE"}, WithOption: true},
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
			},
		},
		{
			name: "GRANT OPTION FOR ALL beside another role's column grant",
			sql: "GRANT SELECT ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t TO other WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantGrants: []privilege{
				{Role: "other", Target: "TABLE t (name)", Privileges: []string{"UPDATE"}, WithOption: true},
				{Role: "r", Target: "TABLE t", Privileges: []string{"SELECT"}},
			},
		},
		{
			name:       "GRANT ALL on a routine is EXECUTE",
			sql:        "GRANT ALL ON PROCEDURE archive(integer) TO r;",
			wantGrants: []privilege{{Role: "r", Target: "ROUTINE archive(integer)", Privileges: []string{"EXECUTE"}}},
		},
		{
			name:        "PUBLIC in lower case and quoted lower case is the keyword",
			sql:         "revoke execute on function f() from public;\nGRANT EXECUTE ON FUNCTION g() TO \"public\";",
			wantGrants:  []privilege{{Role: "PUBLIC", Target: "ROUTINE g()", Privileges: []string{"EXECUTE"}}},
			wantRevoked: []privilege{{Role: "PUBLIC", Target: "ROUTINE f()", Privileges: []string{"EXECUTE"}}},
		},
		{
			name:       "FUNCTION and ROUTINE name the same routine",
			sql:        "REVOKE EXECUTE ON FUNCTION f(uuid) FROM PUBLIC;\nGRANT EXECUTE ON ROUTINE f(UUID) TO PUBLIC;",
			wantGrants: []privilege{{Role: "PUBLIC", Target: "ROUTINE f(uuid)", Privileges: []string{"EXECUTE"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(privileges(database.Grants), qt.DeepEquals, test.wantGrants)
			c.Assert(privileges(database.RevokedGrants), qt.DeepEquals, test.wantRevoked)
		})
	}
}

// TestRead_GrantAndRevoke_FailurePath pins the compositions refused rather than
// approximated. Each has a reading the model cannot state: a privilege held
// without its grant option when the file never granted it, and one privilege
// taken out of an ALL whose members depend on the server version.
func TestRead_GrantAndRevoke_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "GRANT OPTION FOR with no grant in the file",
			sql:     "REVOKE GRANT OPTION FOR SELECT ON t FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR SELECT ON TABLE t FROM r names a grant this schema does not make: .*`,
		},
		{
			name:    "GRANT OPTION FOR ALL with no grant in the file",
			sql:     "REVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR ALL ON TABLE t FROM r names a grant this schema does not make: .*`,
		},
		{
			name:    "REVOKE ALL on a column after a grant on the whole table",
			sql:     "GRANT SELECT ON t TO r;\nREVOKE ALL (a) ON t FROM r;",
			wantErr: `REVOKE ALL \(a\) ON t FROM r follows a GRANT on the whole table, which covers every column, .*`,
		},
		{
			name:    "a column revoke after GRANT ALL on the whole table",
			sql:     "GRANT ALL ON t TO r;\nREVOKE SELECT (a) ON t FROM r;",
			wantErr: `REVOKE SELECT \(a\) ON t FROM r follows a GRANT of SELECT on the whole table, which covers every column, .*`,
		},
		{
			name:    "a revoke of one privilege after GRANT ALL on a table",
			sql:     "GRANT ALL ON t TO r;\nREVOKE TRUNCATE ON t FROM r;",
			wantErr: `REVOKE ON TABLE t FROM r follows a GRANT ALL on it, .*name the privileges in the GRANT`,
		},
		{
			name: "GRANT OPTION FOR ALL on a table whose columns carry the option",
			sql: "GRANT SELECT ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR ALL ON TABLE t FROM r takes the option from grants on the whole table, and an ` +
				`earlier GRANT UPDATE \(name\) ON t gives it on columns, which keep it; declare that GRANT without ` +
				`WITH GRANT OPTION, or revoke the option on those columns before this REVOKE`,
		},
		{
			name: "GRANT OPTION FOR a privilege a column grant also carries",
			sql: "GRANT UPDATE ON t TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR UPDATE ON t FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR UPDATE ON TABLE t FROM r takes the option from grants on the whole table, and an ` +
				`earlier GRANT UPDATE \(name\) ON t gives it on columns, which keep it; declare that GRANT without ` +
				`WITH GRANT OPTION`,
		},
		{
			name: "GRANT OPTION FOR ALL on a qualified table whose bare name carries the option on columns",
			sql: "GRANT SELECT ON public.t6 TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON t6 TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON public.t6 FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR ALL ON TABLE public.t6 FROM r takes the option from grants on the whole table, ` +
				`and an earlier GRANT UPDATE \(name\) ON t6 gives it on columns, which keep it; .*`,
		},
		{
			name: "GRANT OPTION FOR ALL on a bare table whose qualified name carries the option on columns",
			sql: "GRANT SELECT ON t6 TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON public.t6 TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t6 FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR ALL ON TABLE t6 FROM r takes the option from grants on the whole table, ` +
				`and an earlier GRANT UPDATE \(name\) ON public.t6 gives it on columns, which keep it; .*`,
		},
		{
			name: "GRANT OPTION FOR ALL on a table whose columns carry the option under a quoted name",
			sql: "GRANT SELECT ON t6 TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON \"t6\" TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t6 FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR ALL ON TABLE t6 FROM r takes the option from grants on the whole table, .*`,
		},
		{
			name: "GRANT OPTION FOR ALL on a table whose columns carry the option under another case",
			sql: "GRANT SELECT ON t6 TO r WITH GRANT OPTION;\nGRANT UPDATE (name) ON T6 TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR ALL ON t6 FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR ALL ON TABLE t6 FROM r takes the option from grants on the whole table, .*`,
		},
		{
			name: "GRANT OPTION FOR a privilege a column grant of ALL carries",
			sql: "GRANT UPDATE ON t3 TO r WITH GRANT OPTION;\nGRANT ALL (name) ON t3 TO r WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR UPDATE ON t3 FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR UPDATE ON TABLE t3 FROM r takes the option from grants on the whole table, ` +
				`and an earlier GRANT ALL \(name\) ON t3 gives it on columns, which keep it; declare that GRANT without ` +
				`WITH GRANT OPTION`,
		},
		{
			name:    "GRANT OPTION FOR on columns after a grant on the whole table",
			sql:     "GRANT UPDATE ON t TO r WITH GRANT OPTION;\nREVOKE GRANT OPTION FOR UPDATE (name) ON t FROM r;",
			wantErr: `REVOKE GRANT OPTION FOR UPDATE \(name\) ON t FROM r follows a GRANT of UPDATE on the whole table, .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(database.Grants, qt.IsNil)
		})
	}
}

// TestRead_RevokeAll_HappyPath pins that REVOKE ALL reads as ALL from every
// source platform. What ALL names differs from one server to the next -- on
// MySQL 8.4.13 it covers ALTER, DROP and INDEX, which no PostgreSQL table
// privilege is called -- so a list written here would leave the rest held.
func TestRead_RevokeAll_HappyPath(t *testing.T) {
	tests := []struct {
		platform string
		sql      string
		want     []privilege
	}{
		{platform: platform.Postgres, sql: "REVOKE ALL ON t1 FROM r;", want: []privilege{{Role: "r", Target: "TABLE t1", Privileges: []string{"ALL"}}}},
		{platform: platform.MySQL, sql: "REVOKE ALL ON t1 FROM r;", want: []privilege{{Role: "r", Target: "TABLE t1", Privileges: []string{"ALL"}}}},
		{platform: platform.MariaDB, sql: "REVOKE ALL PRIVILEGES ON t1 FROM r;", want: []privilege{{Role: "r", Target: "TABLE t1", Privileges: []string{"ALL"}}}},
		{platform: platform.SQLServer, sql: "REVOKE ALL ON t1 FROM r;", want: []privilege{{Role: "r", Target: "TABLE t1", Privileges: []string{"ALL"}}}},
	}

	for _, test := range tests {
		t.Run(test.platform, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.platform)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Grants, qt.HasLen, 0)
			c.Assert(privileges(database.RevokedGrants), qt.DeepEquals, test.want)
		})
	}
}
