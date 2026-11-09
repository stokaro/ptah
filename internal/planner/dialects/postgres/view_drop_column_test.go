package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestDropColumnUnderSelectStarViewRebuildsTheView ensures a column that the
// live view captured through SELECT * is released before DROP COLUMN.
func TestDropColumnUnderSelectStarViewRebuildsTheView(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables:            []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields:            []schemamodel.Field{{StructName: "T", Name: "id", Type: "integer", Nullable: true}},
		Views:             []schemamodel.View{{Name: "v", Body: "SELECT * FROM t", Comment: "kept"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "mv", Body: "SELECT id FROM v"}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "YES"},
			{Name: "note", DataType: "text", UDTName: "text", IsNullable: "YES"},
		}}},
		Views:    []catalog.View{{Name: "v", Schema: "public", Body: "SELECT t.id, t.note FROM t", Comment: "old"}},
		MatViews: []catalog.MaterializedView{{Name: "mv", Schema: "public", Body: "SELECT id FROM v"}},
	}
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	diff.IndexesAdded = difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_mv_id", Fields: []string{"id"}}, TableName: "mv"}}
	diff.GrantsRemoved = []difftypes.GrantRef{{Role: "reader", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "v"}}
	diff.GrantOptionsRevoked = []difftypes.GrantRef{{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "v", WithOption: true}}
	diff.GrantsAdded = []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v"}}
	diff.CurrentGrants = []difftypes.GrantRef{
		{Role: "reader", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "v"},
		{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "v", WithOption: true},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "postgres")
	c.Assert(err, qt.IsNil)
	plan := strings.Join(statements, "\n")
	dropView := indexOfStatement(statements, "DROP VIEW")
	dropColumn := indexOfStatement(statements, "DROP COLUMN")
	createView := indexOfStatement(statements, "CREATE VIEW")
	c.Assert(dropView >= 0 && dropView < dropColumn && dropColumn < createView, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(indexOfStatement(statements, "REVOKE DELETE"), qt.Equals, -1)
	c.Assert(indexOfStatement(statements, "GRANT SELECT") > createView, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(indexOfStatement(statements, "REVOKE GRANT OPTION") > createView, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(indexOfStatement(statements, "COMMENT ON VIEW") > createView, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	createMaterialized := indexOfStatement(statements, "CREATE MATERIALIZED VIEW")
	c.Assert(createMaterialized > createView, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(indexOfStatement(statements, "CREATE INDEX") > createMaterialized, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
}
