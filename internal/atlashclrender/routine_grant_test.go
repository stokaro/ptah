package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// purgeFunction is a function the document can declare, in schema app.
func purgeFunction(parameters string) schemamodel.Function {
	return schemamodel.Function{
		Name: "app.purge", Parameters: parameters, Returns: "integer", Language: "sql", Body: "SELECT 1",
	}
}

// TestRenderedRoutinePrivilegeRoundTrips carries a grant and a revoke on a
// function through `schema inspect > out.hcl` and back. The routine is named
// by `for` and its argument types by `args`, because PostgreSQL overloads a
// routine name by them. The reference is `function.<label>` where the document
// declares one routine under the label, and a quoted name otherwise -- the one
// spelling the Atlas community binary does not evaluate against a block.
func TestRenderedRoutinePrivilegeRoundTrips(t *testing.T) {
	tests := []struct {
		name      string
		functions []schemamodel.Function
		wantFor   string
		wantKind  string
	}{
		{
			name:      "a declared function is referenced by its block",
			functions: []schemamodel.Function{purgeFunction("p uuid")},
			wantFor:   "for = function.purge",
			wantKind:  "FUNCTION",
		},
		{
			name:     "an undeclared function is a quoted name",
			wantFor:  `for = "app.purge"`,
			wantKind: "ROUTINE",
		},
		{
			name:      "two overloads under one label are a quoted name",
			functions: []schemamodel.Function{purgeFunction("p uuid"), purgeFunction("p text")},
			wantFor:   `for = "app.purge"`,
			wantKind:  "ROUTINE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := inspectedTable("public")
			db.Schemas = []schemamodel.Schema{{Name: "public"}, {Name: "app"}}
			db.Functions = test.functions
			db.Grants = []schemamodel.Grant{{
				Role: "app_role", Privileges: []string{"EXECUTE"}, OnRoutine: "app.purge", RoutineArguments: "uuid",
				RoutineKind: "FUNCTION",
			}}
			db.RevokedGrants = []schemamodel.Grant{{
				Role: "PUBLIC", Privileges: []string{"EXECUTE"}, OnRoutine: "app.purge", RoutineArguments: "uuid",
				RoutineKind: "FUNCTION",
			}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(string(result.Data), qt.Contains, test.wantFor)
			c.Assert(string(result.Data), qt.Contains, `args = ["uuid"]`)
			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(routineTargets(parsed.Grants), qt.DeepEquals, []string{"app_role EXECUTE " + test.wantKind + " app.purge(uuid)"})
			c.Assert(routineTargets(parsed.RevokedGrants), qt.DeepEquals, []string{"PUBLIC EXECUTE " + test.wantKind + " app.purge(uuid)"})
		})
	}
}

func routineTargets(grants []schemamodel.Grant) []string {
	out := make([]string, 0, len(grants))
	for _, grant := range grants {
		out = append(out, grant.Role+" "+grant.Privileges[0]+" "+grant.RoutineKind+" "+
			grant.OnRoutine+"("+grant.RoutineArguments+")")
	}
	return out
}

// TestParseRoutinePrivilege_FailurePath pins the blocks refused: a routine
// named without its argument types, and argument types on a target that is not
// a routine.
func TestParseRoutinePrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "a function without args",
			body:    "permission {\n  to = \"r\"\n  for = function.purge\n  privileges = [\"EXECUTE\"]\n}\n",
			wantErr: `(?s).*permission on function purge needs args, its argument types: a routine's identity includes them.*`,
		},
		{
			name:    "args on a table",
			body:    "revoke {\n  from = \"r\"\n  for = table.t\n  args = [\"uuid\"]\n  privileges = [\"SELECT\"]\n}\n",
			wantErr: `(?s).*revoke args need a function or procedure target.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := "schema \"public\" {}\n\ntable \"t\" {\n  schema = schema.public\n  column \"id\" {\n    type = int\n  }\n}\n\n" + test.body

			parsed, err := atlashcl.Parse([]byte(document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(parsed, qt.IsNil)
		})
	}
}
