package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestGrant_TargetKey pins which grants name the same object. The routine
// rows carry the weight: PostgreSQL overloads a routine name by its argument
// types, so two overloads must not share a key, while FUNCTION, PROCEDURE and
// ROUTINE are three spellings of one target and must.
func TestGrant_TargetKey(t *testing.T) {
	tests := []struct {
		name  string
		grant schemamodel.Grant
		want  string
	}{
		{name: "a table", grant: schemamodel.Grant{OnTable: "users"}, want: "TABLE users"},
		{name: "a schema", grant: schemamodel.Grant{OnSchema: "app"}, want: "SCHEMA app"},
		{name: "a sequence", grant: schemamodel.Grant{OnSequence: "app.order_seq"}, want: "SEQUENCE app.order_seq"},
		{
			name:  "a function, arguments folded for case and spacing",
			grant: schemamodel.Grant{OnRoutine: "purge", RoutineArguments: " UUID ,  Text ", RoutineKind: "FUNCTION"},
			want:  "ROUTINE purge(uuid, text)",
		},
		{
			name:  "a procedure shares the key of the same routine named as a function",
			grant: schemamodel.Grant{OnRoutine: "purge", RoutineArguments: "uuid, text", RoutineKind: "PROCEDURE"},
			want:  "ROUTINE purge(uuid, text)",
		},
		{name: "a routine with no arguments", grant: schemamodel.Grant{OnRoutine: "tick"}, want: "ROUTINE tick()"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.grant.TargetKey(), qt.Equals, test.want)
		})
	}
}

// TestValidateRevokedGrants_HappyPath pins the declarations that do not
// contradict each other: a grant and a revoke that differ in role, object,
// privilege or dialect say two different things, and both hold.
func TestValidateRevokedGrants_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		db   *schemamodel.Database
	}{
		{name: "nil", db: nil},
		{
			name: "the wpmgr pair: PUBLIC revoked, the app role granted",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "uuid"}},
				RevokedGrants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "uuid"}},
			},
		},
		{
			name: "another overload",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "uuid"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "text"}},
			},
		},
		{
			name: "another privilege on the same table",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"DELETE"}, OnTable: "t"}},
			},
		},
		{
			name: "disjoint dialect scopes",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Dialects: []string{"postgres"}}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Dialects: []string{"mysql"}}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.ValidateRevokedGrants(test.db), qt.IsNil)
		})
	}
}

// TestValidateRevokedGrants_FailurePath pins the contradictions refused. A
// declarative source has no statement order, so keeping either side would
// drop the other without a word.
func TestValidateRevokedGrants_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		db      *schemamodel.Database
		wantErr string
	}{
		{
			name: "one privilege both granted and revoked",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT", "INSERT"}, OnTable: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"INSERT"}, OnTable: "t"}},
			},
			wantErr: `INSERT on TABLE t is both granted to and revoked from "app"; declare one or the other`,
		},
		{
			name: "a grant of ALL contradicts a revoke of any privilege",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"ALL"}, OnTable: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"TRUNCATE"}, OnTable: "t"}},
			},
			wantErr: `TRUNCATE on TABLE t is both granted to and revoked from "app"; declare one or the other`,
		},
		{
			name: "the same routine spelled as a function and a routine",
			db: &schemamodel.Database{
				Grants: []schemamodel.Grant{{
					Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "UUID", RoutineKind: "ROUTINE",
				}},
				RevokedGrants: []schemamodel.Grant{{
					Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "f", RoutineArguments: "uuid", RoutineKind: "FUNCTION",
				}},
			},
			wantErr: `EXECUTE on ROUTINE f\(uuid\) is both granted to and revoked from "PUBLIC"; declare one or the other`,
		},
		{
			name: "an unscoped grant reaches a scoped revoke",
			db: &schemamodel.Database{
				Grants:        []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t", Dialects: []string{"postgres"}}},
			},
			wantErr: `SELECT on TABLE t is both granted to and revoked from "app"; declare one or the other`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.ValidateRevokedGrants(test.db), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestMerge_RefusesAGrantAnotherSourceRevokes drives the validation through
// [schemamodel.Merge], the path every multi-source Go schema takes, so a
// validation nothing calls cannot pass for one that is in effect.
func TestMerge_RefusesAGrantAnotherSourceRevokes(t *testing.T) {
	c := qt.New(t)
	granted := &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t"}}}
	revoked := &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "t"}}}

	merged, err := schemamodel.Merge(granted, revoked)

	c.Assert(err, qt.ErrorMatches, `SELECT on TABLE t is both granted to and revoked from "app"; declare one or the other`)
	c.Assert(merged, qt.IsNil)
}
