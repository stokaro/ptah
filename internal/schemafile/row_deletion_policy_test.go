package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// A YDB table with a TTL, written as HCL by `schema inspect` and loaded back
// as the desired state, plans nothing for the TTL: HCL cannot spell one, so
// its silence is not a request to remove it. Measured on YDB 26.2.1.14, the
// round trip through `ptah-compat schema inspect` and `schema apply` planned
// `ALTER TABLE ... RESET (TTL)` without the loader's record.
func TestAnHCLDocumentKeepsTheTableTTL(t *testing.T) {
	c := qt.New(t)
	policy := &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}
	inspected := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Events", Name: "events", PrimaryKey: []string{"id"},
			RowDeletionPolicy: policy}},
		Fields: []schemamodel.Field{
			{StructName: "Events", Name: "id", Type: "Uint64", Primary: true},
			{StructName: "Events", Name: "ts", Type: "Timestamp", Nullable: true},
		},
	}
	rendered, err := atlashclrender.RenderInspectedForAtlasCLI(inspected, platform.YDB, "")
	c.Assert(err, qt.IsNil)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, rendered.Data, 0o600), qt.IsNil)

	desired, err := schemafile.LoadPath(path, schemafile.Options{})
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{
		Tables: []catalog.Table{{Name: "events", Type: "TABLE", RowDeletionPolicy: policy, Columns: []catalog.Column{
			{Name: "id", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "ts", DataType: "Timestamp", ColumnType: "Timestamp", IsNullable: "YES", OrdinalPosition: 2},
		}}},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.TablesModified, qt.HasLen, 0)
}
