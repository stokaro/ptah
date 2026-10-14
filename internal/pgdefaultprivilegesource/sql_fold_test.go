package pgdefaultprivilegesource_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

func sqlIntent(c *qt.C, db schemamodel.Database) *pgdefaultprivilege.Desired {
	c.Helper()
	objects := must.Must(db.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	value, ok := objects[0].Value.(*pgdefaultprivilege.Desired)
	c.Assert(ok, qt.IsTrue)
	return value
}

func TestSQL_OrderedChangesDescribeTheFinalACL(t *testing.T) {
	tests := []struct {
		name    string
		clauses []string
		want    pgdefaultprivilege.Desired
	}{
		{name: "grant then revoke", clauses: []string{"GRANT SELECT, INSERT ON TABLES TO reader", "REVOKE INSERT ON TABLES FROM reader"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"INSERT"}}},
		{name: "revoke alone", clauses: []string{"REVOKE EXECUTE ON FUNCTIONS FROM reader"}, want: pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}},
		{name: "revoke then grant", clauses: []string{"REVOKE SELECT ON TABLES FROM reader", "GRANT SELECT ON TABLES TO reader"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{name: "revoke all then grant", clauses: []string{"REVOKE ALL ON SEQUENCES FROM reader", "GRANT USAGE ON SEQUENCES TO reader"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}}, Revoked: []string{"SELECT", "UPDATE"}}},
		{name: "option revoke keeps access", clauses: []string{"GRANT SELECT ON TABLES TO reader WITH GRANT OPTION", "REVOKE GRANT OPTION FOR SELECT ON TABLES FROM reader"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{name: "public keyword case", clauses: []string{"GRANT USAGE ON TYPES TO public", "REVOKE USAGE ON TYPES FROM PUBLIC"}, want: pgdefaultprivilege.Desired{Revoked: []string{"USAGE"}}},
		{name: "plain grant keeps options", clauses: []string{"GRANT SELECT ON TABLES TO reader WITH GRANT OPTION", "GRANT SELECT ON TABLES TO reader"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
		{name: "all with specific option", clauses: []string{"GRANT ALL ON TABLES TO reader", "GRANT SELECT ON TABLES TO reader WITH GRANT OPTION"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}, {Privilege: "SELECT", WithOption: true}}}},
		{name: "clear all options", clauses: []string{"GRANT ALL ON TABLES TO reader WITH GRANT OPTION", "REVOKE GRANT OPTION FOR ALL ON TABLES FROM reader"}, want: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}},
		{name: "revoke all clears explicit grants", clauses: []string{"GRANT SELECT ON TABLES TO reader WITH GRANT OPTION", "REVOKE ALL ON TABLES FROM reader"}, want: pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var text strings.Builder
			for _, clause := range test.clauses {
				text.WriteString(alterDefault + clause + ";\n")
			}
			c.Assert(sqlIntent(c, readSQL(c, text.String())), qt.DeepEquals, test.want.Canonical())
		})
	}
}

func TestSQL_ChangesReachEarlierFiles(t *testing.T) {
	c := qt.New(t)
	base := readSQL(c, alterDefault+"GRANT SELECT, INSERT ON TABLES TO reader WITH GRANT OPTION;")
	before := sqlIntent(c, base)
	next, _, err := sqlschema.ReadWith(c.Context(), []byte(alterDefault+"REVOKE INSERT ON TABLES FROM reader;\n"+alterDefault+"REVOKE GRANT OPTION FOR SELECT ON TABLES FROM reader;"), "postgres", sqlschema.NewDocument(&base), sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(next.FeatureObjects.All()), qt.HasLen, 0)
	c.Assert(sqlIntent(c, base), qt.DeepEquals, (&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"INSERT"}}).Canonical())
	c.Assert(before, qt.DeepEquals, (&pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT", WithOption: true}}}).Canonical())
}

// A later SQL file changes the ACL accumulated from earlier files. Other
// identities remain available, including another class under the same grantor.
func TestSQL_FoldsLaterFilesByDefaultPrivilegeIdentity(t *testing.T) {
	type declaration struct {
		class string
		value pgdefaultprivilege.Desired
	}
	for _, test := range []struct {
		name, earlier, later string
		want                 []declaration
	}{
		{name: "later revoke", earlier: "GRANT SELECT, INSERT ON TABLES TO reader", later: "REVOKE INSERT ON TABLES FROM reader", want: []declaration{{"TABLES", pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"INSERT"}}}}},
		{name: "later grant", earlier: "REVOKE SELECT, INSERT ON TABLES FROM reader", later: "GRANT SELECT ON TABLES TO reader", want: []declaration{{"TABLES", pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"INSERT"}}}}},
		{name: "revoke all", earlier: "GRANT SELECT, INSERT ON TABLES TO reader", later: "REVOKE ALL ON TABLES FROM reader", want: []declaration{{"TABLES", pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}}}},
		{name: "another class", earlier: "GRANT SELECT ON TABLES TO reader", later: "REVOKE USAGE ON SEQUENCES FROM reader", want: []declaration{{"TABLES", pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}, {"SEQUENCES", pgdefaultprivilege.Desired{Revoked: []string{"USAGE"}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			base := readSQL(c, alterDefault+test.earlier+";")
			next, _, err := sqlschema.ReadWith(t.Context(), []byte(alterDefault+test.later+";"), "postgres", sqlschema.NewDocument(&base), sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			merged := must.Must(base.FeatureObjects.Merge(next.FeatureObjects))
			var want []schemaext.Object
			for _, declaration := range test.want {
				ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: declaration.class, Grantee: "reader"}, identifier.ForDialect("postgres")))
				want = append(want, schemaext.Object{Ref: ref, Value: declaration.value.Canonical()})
			}
			c.Assert(must.Must(merged.All()), qt.ContentEquals, want)
		})
	}
}
