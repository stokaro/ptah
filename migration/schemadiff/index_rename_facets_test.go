package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/ydbindex"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// renamedIndexSchema is table items with one index on price, named name and
// declaring settings as the YDB owner's facet, or none for nil.
func renamedIndexSchema(name string, settings *ydbschema.IndexPartitioning) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "Int64", Primary: true},
			{StructName: "Item", Name: "price", Type: "Int64", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "Item", TableName: "items", Name: name, Type: "GLOBAL SYNC", Fields: []string{"price"},
			Facets: must.Must(ydbindex.WithPartitioning(schemaext.Facets{}, settings))}},
	}
}

// observedIndexSettings is held as the read's value, nil for none.
func observedIndexSettings(held *ydbschema.IndexPartitioning) *ydbschema.ObservedIndexPartitioning {
	if held == nil {
		return nil
	}
	return &ydbschema.ObservedIndexPartitioning{IndexPartitioning: *held}
}

// TestCompare_RenamedIndexComparesItsSettingsUnderItsNewName pairs an index
// the declaration renames and compares its settings as one index's, under the
// name the plan gives it: read under its old name, what the database holds
// belongs to an index the declaration does not have, and the change would be
// lost. A rename that keeps the settings is no change of them.
func TestCompare_RenamedIndexComparesItsSettingsUnderItsNewName(t *testing.T) {
	tests := []struct {
		name           string
		declared, held *ydbschema.IndexPartitioning
		want           int
	}{
		{name: "settings changed on the way", declared: &ydbschema.IndexPartitioning{MinPartitions: 4},
			held: &ydbschema.IndexPartitioning{MinPartitions: 3}, want: 1},
		// Nothing held is known only from what the read recorded about the
		// index under its old name.
		{name: "settings declared over the defaults", declared: &ydbschema.IndexPartitioning{MinPartitions: 4}, want: 1},
		{name: "settings kept", declared: &ydbschema.IndexPartitioning{MinPartitions: 3},
			held: &ydbschema.IndexPartitioning{MinPartitions: 3}, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current := must.Must(goschematodb.ToDBSchema(t.Context(), renamedIndexSchema("old_price", test.held), "ydb", runtime))
			// As a read records it: the index it returned is known, any other is not.
			current.FeatureCoverage = must.Must(ydbschema.IndexPartitioningCoverage(schemaext.Observed,
				schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned global indexes have inspected settings"},
				[]schemaext.SubjectCoverage{{Kind: ydbschema.IndexPartitioningKind, Knowledge: schemaext.Knowledge{State: schemaext.Complete},
					Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).IndexParts("", "items", "old_price")}}))

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), renamedIndexSchema("by_price", test.declared), current,
				catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262()}, nil, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "items", From: "old_price", To: "by_price"}})
			c.Assert(diff.TablesModified, qt.HasLen, test.want)
			var changes []schemaext.ChangeRecord
			for _, table := range diff.TablesModified {
				changes = append(changes, table.FeatureChanges...)
			}
			c.Assert(changes, qt.HasLen, test.want)
			for _, change := range changes {
				c.Assert(change.Subject.Name.Source, qt.Equals, "by_price")
				c.Assert(change.Value.(*ydbdiff.IndexPartitioning).Before, qt.DeepEquals, observedIndexSettings(test.held))
			}
		})
	}
}
