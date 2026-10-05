package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestYDBIndexPartitioning_HCLRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name string
		spec *ast.IndexPartitioningSpec
	}{
		{"configured", &ast.IndexPartitioningSpec{BySize: new(true), PartitionSizeMB: 512, ByLoad: new(true), MinPartitions: 4, MaxPartitions: 16, ReadReplicas: "ANY_AZ:2"}},
		{"explicitly disabled", &ast.IndexPartitioningSpec{BySize: new(false), ByLoad: new(false), MinPartitions: 1, ReadReplicas: "ANY_AZ:0"}},
		{"omitted", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			original := &schemamodel.Database{
				Tables:  []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}}},
				Fields:  []schemamodel.Field{{StructName: "Item", Name: "id", Type: "Int64", Primary: true}, {StructName: "Item", Name: "kind", Type: "Utf8", Nullable: true}},
				Indexes: []schemamodel.Index{{StructName: "Item", TableName: "items", Name: "by_kind", Type: "GLOBAL SYNC", Fields: []string{"kind"}, Partitioning: test.spec}},
			}
			rendered, err := atlashclrender.RenderInspectedForAtlasCLI(original, "ydb", "")
			c.Assert(err, qt.IsNil)
			parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Indexes, qt.HasLen, 1)
			c.Assert(parsed.Indexes[0].Partitioning, qt.DeepEquals, test.spec)
			current := goschematodb.ToDBSchema(original, "ydb")
			caps := capability.YDB262()
			diff, err := schemadiff.CompareWithDatabaseInfo(parsed, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil)
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, "ydb", planner.Options{Capabilities: caps})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
