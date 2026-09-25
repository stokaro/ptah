package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_RoutineGrantsAndRevokes_HappyPath pins the YAML spelling of a
// privilege on a function or procedure, named with its argument types as the
// Go annotations name it, and of a revoke: the pair mosamlife/wpmgr's schema
// file writes for a SECURITY DEFINER function.
func TestParse_RoutineGrantsAndRevokes_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `grants:
  app_purges:
    role: wpmgr_app
    privileges: [EXECUTE]
    on_function: purge_workspace(uuid)
  ops_archives:
    role: ops
    privileges: [EXECUTE]
    on_procedure: app.archive(integer, text)
  reader_orders:
    role: reader
    privileges: [USAGE]
    on_sequence: order_seq
revokes:
  public_purges:
    role: PUBLIC
    privileges: [EXECUTE]
    on_function: purge_workspace(uuid)
  app_signatures:
    role: wpmgr_app
    privileges: [INSERT, UPDATE, DELETE]
    on_table: plugin_signatures
`

	db, err := yamlschema.Parse([]byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "wpmgr_app", Privileges: []string{"EXECUTE"}, OnRoutine: "purge_workspace", RoutineArguments: "uuid", RoutineKind: "FUNCTION"},
		{Role: "ops", Privileges: []string{"EXECUTE"}, OnRoutine: "app.archive", RoutineArguments: "integer, text", RoutineKind: "PROCEDURE"},
		{Role: "reader", Privileges: []string{"USAGE"}, OnSequence: "order_seq"},
	})
	c.Assert(db.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "wpmgr_app", Privileges: []string{"INSERT", "UPDATE", "DELETE"}, OnTable: "plugin_signatures"},
		{Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "purge_workspace", RoutineArguments: "uuid", RoutineKind: "FUNCTION"},
	})
}

// TestParse_RoutineGrantsAndRevokes_FailurePath pins the entries refused.
func TestParse_RoutineGrantsAndRevokes_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "a function without its argument types",
			document: "grants:\n  g:\n    role: app\n    privileges: [EXECUTE]\n    on_function: purge\n",
			wantErr:  `grant "g" on_function "purge" needs the argument types in parentheses, as in purge\(uuid\): PostgreSQL tells overloaded routines apart by them`,
		},
		{
			name:     "a revoke naming no privilege",
			document: "revokes:\n  r:\n    role: app\n    on_table: t\n",
			wantErr:  `revoke "r" requires role and privileges`,
		},
		{
			name: "one privilege granted and revoked",
			document: "grants:\n  g:\n    role: app\n    privileges: [EXECUTE]\n    on_function: purge(uuid)\n" +
				"revokes:\n  r:\n    role: app\n    privileges: [EXECUTE]\n    on_function: purge(UUID)\n",
			wantErr: `parse YAML schema: EXECUTE on ROUTINE purge\(uuid\) is both granted to and revoked from "app"; declare one or the other`,
		},
		{
			name:     "a grant option on a revoke",
			document: "revokes:\n  r:\n    role: app\n    privileges: [SELECT]\n    on_table: t\n    with_option: true\n",
			wantErr:  `(?s)parse YAML schema: .*with_option.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse([]byte(test.document))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
