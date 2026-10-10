package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// A YDB table with a TTL, written as HCL by `schema inspect` and loaded back
// as the desired state, plans nothing for the TTL: HCL cannot spell one, so
// the document claims no knowledge of it, and its silence is not a request to
// remove it. Measured on YDB 26.2.1.14, the round trip through `ptah-compat
// schema inspect` and `schema apply` once planned `ALTER TABLE ... RESET
// (TTL)`.
func TestAnHCLDocumentKeepsTheTableTTL(t *testing.T) {
	c := qt.New(t)
	policy := ydbschema.TTL{Column: "ts", Interval: "P30D"}
	declared := must.Must(must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: policy})).WithTargetScope(ydbschema.TTLKind, platform.YDB))
	observed := must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedTTL{Policy: policy})).WithTargetScope(ydbschema.TTLKind, platform.YDB))
	inspected := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Events", Name: "events", PrimaryKey: []string{"id"},
			Facets: declared}},
		Fields: []schemamodel.Field{
			{StructName: "Events", Name: "id", Type: "Uint64", Primary: true},
			{StructName: "Events", Name: "ts", Type: "Timestamp", Nullable: true},
		},
	}
	rendered, err := atlashclrender.RenderInspectedForAtlasCLI(inspected, platform.YDB, "")
	c.Assert(err, qt.IsNil)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, rendered.Data, 0o600), qt.IsNil)

	desired, err := schemafile.LoadPath(path, schemafile.Options{YAML: builtintest.Runtime().YAML()})
	c.Assert(err, qt.IsNil)
	read := must.Must(ydbcoordination.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	read = must.Must(read.Combine(must.Must(ydbschema.TTLCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{
		FeatureCoverage: read,
		Tables: []catalog.Table{{Name: "events", Type: "TABLE", Facets: observed, Columns: []catalog.Column{
			{Name: "id", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "ts", DataType: "Timestamp", ColumnType: "Timestamp", IsNullable: "YES", OrdinalPosition: 2},
		}}},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.TablesModified, qt.HasLen, 0)
}
