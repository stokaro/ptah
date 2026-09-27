package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestClassifySchemaDiff_TheDatabasesOfAWholeServer grades a dropped database
// as destructive, since every object in it goes with it, a changed character
// set or collation as a warning, and a created database as safe
// (stokaro/ptah#3789).
func TestClassifySchemaDiff_TheDatabasesOfAWholeServer(t *testing.T) {
	c := qt.New(t)

	findings := safety.ClassifySchemaDiff(&difftypes.SchemaDiff{
		SchemasAdded:    []schemamodel.Schema{{Name: "r9"}},
		SchemasRemoved:  []string{"r3", "r4"},
		SchemasModified: []difftypes.SchemaChange{{Name: "r1", Collate: "latin1_swedish_ci"}},
	})

	c.Assert(findings, qt.DeepEquals, []safety.Finding{
		{Category: "schemas_removed", Count: 2, Severity: safety.Destructive},
		{Category: "schemas_modified", Count: 1, Severity: safety.Warning},
		{Category: "schemas_added", Count: 1, Severity: safety.Safe},
	})
}

// TestClassify_DropDatabaseIsDestructive grades the statement that drops a
// whole database the way a table drop is graded.
func TestClassify_DropDatabaseIsDestructive(t *testing.T) {
	tests := []string{"DROP DATABASE `r3`", "drop schema r3"}
	for _, statement := range tests {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(safety.Classify(ast.NewRawSQL(statement)), qt.Equals, safety.Destructive)
		})
	}
}
