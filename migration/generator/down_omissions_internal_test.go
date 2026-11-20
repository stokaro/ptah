package generator

// White-box testing required: the down file is rendered by
// renderGeneratedDownMigrationSQL, which the exported generator reaches only
// through a migration directory and a live database; downMigrationSQL drives
// the same production function from a diff.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A migration that drops a table whose live definition carries a comment
// rolls back to a CREATE TABLE. SQLite has no table comment, so the
// down file leaves it out, and says so as the up file does.
func TestGenerateDownMigration_CarriesWhatItsStatementsLeaveOut(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "TABLE", Comment: "audit log",
		Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true}},
	}}}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, current, "sqlite", must.Must(builtin.New())))

	down, err := downMigrationSQL(t.Context(), must.Must(builtin.New()), diff, nil, current, "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, "-- Direction: DOWN\n\n-- sqlite: table t: comment=audit log was skipped (unsupported)\n\n-- audit log\nCREATE TABLE")
}
