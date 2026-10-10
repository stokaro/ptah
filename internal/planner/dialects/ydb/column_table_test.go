package ydb_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/migration/schemadiff/difftypes"
)

func columnRetention() *ydbschema.DesiredColumnStore {
	return &ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, TTL: &ydbschema.TieredTTL{Column: "ts", Tiers: []ydbschema.TTLTier{{Interval: "P1D", ExternalSource: "/local/ext/bucket"}, {Interval: "P7D"}}}}}
}

// Replacing a source must detach an unchanged policy too. The source cannot
// be dropped while a column table's eviction tier references it, so the
// policy is reset before the drop or the replacement and set again after the
// source exists. The policy names the source by its absolute path, which the
// plan reads against the database it runs in.
func TestGenerateMigrationAST_ColumnTTLSourceReplacement(t *testing.T) {
	set := "ALTER TABLE `events` SET (TTL = Interval(\"PT86400S\") TO EXTERNAL DATA SOURCE `/local/ext/bucket`, Interval(\"PT604800S\") DELETE ON `ts`)"
	tests := []struct {
		name    string
		replace bool
		want    []string
	}{
		{name: "dropped and created again", want: []string{"ALTER TABLE `events` RESET (TTL)", "DROP EXTERNAL DATA SOURCE `ext/bucket`",
			"CREATE EXTERNAL DATA SOURCE `ext/bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n    LOCATION = 'https://s3.example.test/other/',\n    AUTH_METHOD = 'NONE'\n)",
			set, ""}},
		{name: "replaced in place", replace: true, want: []string{"ALTER TABLE `events` RESET (TTL)",
			"CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n    LOCATION = 'https://s3.example.test/other/',\n    AUTH_METHOD = 'NONE'\n)",
			set, ""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				CurrentDatabasePath: "/local",
				DeclaredTables:      []schemamodel.Table{{Name: "events", Facets: must.Must(schemaext.NewFacets(columnRetention()))}},
				FeatureChanges:      []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, movedBucket())},
			}
			got := render(c, externalPlanCaps(test.replace).With(capability.TieredTTL, true), diff)
			c.Assert(strings.Split(got, ";\n"), qt.DeepEquals, test.want)
		})
	}
}

// capturedColumnRemoval removes a column table whose eviction tier names the
// data source /local/ext/bucket.
func capturedColumnRemoval() difftypes.TableRemoval {
	removal := difftypes.TableRemoval{Name: "events"}
	removal.Current.Table.Name = "events"
	removal.Current.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnStore{ColumnStore: columnRetention().ColumnStore}))
	removal.Current.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, nil))
	return removal
}

// A removed column table whose eviction tier reads a data source the plan
// drops is dropped first, since YDB keeps no source a table's TTL still names.
// The table's captured policy names the source.
func TestGenerateMigrationAST_ColumnTableRemovedBeforeItsSource(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		CurrentDatabasePath: "/local",
		TablesRemoved:       difftypes.TableRemovals{capturedColumnRemoval()},
		FeatureChanges:      []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, nil)},
	}
	got := render(c, externalPlanCaps(false).With(capability.TieredTTL, true), diff)
	c.Assert(got, qt.Equals, "DROP TABLE `events`;\nDROP EXTERNAL DATA SOURCE `ext/bucket`;\n")
}

// A removed column table whose eviction tier reads a data source the plan
// recreates is dropped before the recreation, which drops the source first.
func TestGenerateMigrationAST_ColumnTableRemovedBeforeItsSourceIsRecreated(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		CurrentDatabasePath: "/local",
		TablesRemoved:       difftypes.TableRemovals{capturedColumnRemoval()},
		FeatureChanges:      []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, movedBucket())},
	}
	got := render(c, externalPlanCaps(false).With(capability.TieredTTL, true), diff)
	c.Assert(statementHeads(got), qt.DeepEquals, []string{"DROP TABLE `events`;", "DROP EXTERNAL DATA SOURCE `ext/bucket`;",
		"CREATE EXTERNAL DATA SOURCE `ext/bucket`"})
}

// Both TTL forms use the same server setting. Resetting the old row policy
// after installing the eviction policy would silently remove the new one.
func TestGenerateMigrationAST_ColumnTTLFromRowPolicy(t *testing.T) {
	c := qt.New(t)
	declaration := itemsDeclaration(field("ts", "TIMESTAMP", true))
	declaration.Table.Facets = must.Must(schemaext.NewFacets(columnRetention()))
	declaration.Table.PrimaryKey = []string{"ts", "id"}
	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("items")
	store := &ydbdiff.ColumnStore{Before: &ydbschema.ObservedColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 4}}, After: columnRetention()}
	diff := addTTLChange(modified(t, difftypes.TableDiff{
		TableName: "items", Desired: declaration,
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: store}},
	}), nil, &ydbschema.TTL{Column: "ts", Interval: "P7D"})
	got := render(c, capability.YDB262().With(capability.TieredTTL, true), diff)
	c.Assert(got, qt.Equals, "ALTER TABLE `items` RESET (TTL);\nALTER TABLE `items` SET (TTL = Interval(\"PT86400S\") TO EXTERNAL DATA SOURCE `/local/ext/bucket`, Interval(\"PT604800S\") DELETE ON `ts`);\n")
}
