package clickhouse_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// observedIndexCoverage is what the reader establishes: complete storage and
// skipping-index settings for the tables and indexes it returns.
func observedIndexCoverage() schemaext.Coverage {
	var kinds []schemaext.KindCoverage
	for _, model := range must.Must(builtin.New()).Codecs().Definitions() {
		if (model.Kind == chschema.TableKind || model.Kind == chschema.IndexKind) && model.Representation == schemaext.Observed {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
	}
	return must.Must(schemaext.NewCoverage(schemaext.Observed, kinds, nil))
}

func indexedCatalog(settings *chschema.ObservedIndex) *catalog.Database {
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
	return &catalog.Database{FeatureCoverage: observedIndexCoverage(), Tables: []catalog.Table{{
		Name: "events", Facets: must.Must(schemaext.NewFacets(storage)),
		Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"}, {Name: "payload", DataType: "String", ColumnType: "String", IsNullable: "NO"}},
	}}, Indexes: []catalog.Index{{
		Name: "idx_payload", TableName: "events", Columns: []string{"lower(payload)"},
		Facets: must.Must(must.Must(schemaext.NewFacets(settings)).WithTargetScope(chschema.IndexKind, "clickhouse")),
	}}}
}

func indexedSource(index schemamodel.Index) *schemamodel.Database {
	index.Name, index.StructName, index.TableName, index.Fields = "idx_payload", "Event", "events", []string{"lower(payload)"}
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields:  []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}, {Name: "payload", StructName: "Event", Type: "String"}},
		Indexes: []schemamodel.Index{index},
	}
}

// ClickHouse changes neither the type nor the granularity of a skipping index
// in place. The plan replaces the index with its captured expression, and the
// rollback replaces it again with the captured settings.
func TestSkippingIndexSettingsChangePlansBothDirections(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	before := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
	source := indexedSource(schemamodel.Index{Overrides: map[string]map[string]string{"clickhouse": {"type": "set(100)", "granularity": "4"}}})
	current := indexedCatalog(before)
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE `events` DROP INDEX `idx_payload`",
		"ALTER TABLE `events` ADD INDEX `idx_payload` lower(payload) TYPE set(100) GRANULARITY 4",
	})
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
	})
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	drop, add := strings.Index(reverse, "DROP INDEX `idx_payload`"), strings.Index(reverse, "ADD INDEX `idx_payload` lower(payload) TYPE minmax GRANULARITY 1;")
	c.Assert(drop >= 0 && drop < add, qt.IsTrue, qt.Commentf("%s", reverse))
	c.Assert(reverse, qt.Contains, "does not restore materialized index data")
	c.Assert(plan.Reverse.Recovery, qt.HasLen, 1)
	c.Assert(current.Indexes[0].Facets, qt.DeepEquals, indexedCatalog(before).Indexes[0].Facets)
}

// Settings that already hold, stated or left out, plan nothing: an omitted
// setting keeps what the index has rather than asking for a default.
func TestSkippingIndexSettingsThatHoldPlanNothing(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties map[string]map[string]string
	}{
		{"stated", map[string]map[string]string{"clickhouse": {"type": "set(100)", "granularity": "4"}}},
		{"left out", nil},
		{"granularity left out", map[string]map[string]string{"clickhouse": {"type": "set(100)"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			source := indexedSource(schemamodel.Index{Overrides: test.properties})
			diff, err := schemadiff.CompareWithDialect(t.Context(), source, indexedCatalog(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}), "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// A requested default is a change when the index holds something else.
func TestSkippingIndexSettingsDefaultRequestReplacesTheIndex(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := indexedSource(schemamodel.Index{Overrides: map[string]map[string]string{"clickhouse": {"granularity.state": "default"}}})
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, indexedCatalog(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}), "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE `events` DROP INDEX `idx_payload`",
		"ALTER TABLE `events` ADD INDEX `idx_payload` lower(payload) TYPE set(100) GRANULARITY 1",
	})
}

// When the common comparison already replaces the index, its new definition
// carries the desired settings, and the owner adds no second replacement. A
// declared condition is one such trigger: ClickHouse reports none, so the
// common comparison rebuilds the index.
func TestSkippingIndexSettingsJoinACommonReplacement(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := indexedSource(schemamodel.Index{Condition: "payload != ''", Overrides: map[string]map[string]string{"clickhouse": {"type": "set(100)", "granularity": "4"}}})
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, indexedCatalog(&chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}), "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	drops := slices.DeleteFunc(slices.Clone(statements), func(statement string) bool { return !strings.Contains(statement, "DROP INDEX") })
	adds := slices.DeleteFunc(slices.Clone(statements), func(statement string) bool { return !strings.Contains(statement, "ADD INDEX") })
	c.Assert(drops, qt.HasLen, 1)
	c.Assert(adds, qt.DeepEquals, []string{"ALTER TABLE `events` ADD INDEX `idx_payload` lower(payload) TYPE set(100) GRANULARITY 4"})
}

// The common comparison matches ClickHouse indexes by name, so a settings
// change can arrive beside a changed key. The replacement builds the declared
// index, and the rollback restores the captured one, so neither direction
// leaves the index on a key nobody declared.
func TestSkippingIndexSettingsChangeAppliesTheDeclaredKey(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := indexedSource(schemamodel.Index{Overrides: map[string]map[string]string{"clickhouse": {"granularity": "4"}}})
	source.Indexes[0].Fields = []string{"id", "payload"}
	current := indexedCatalog(&chschema.ObservedIndex{IndexType: "minmax", Granularity: 1})
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE `events` DROP INDEX `idx_payload`",
		"ALTER TABLE `events` ADD INDEX `idx_payload` (id, payload) TYPE minmax GRANULARITY 4",
	})
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
	})
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(reverse, qt.Contains, "ADD INDEX `idx_payload` lower(payload) TYPE minmax GRANULARITY 1;")
}

// A SQL statement that leaves GRANULARITY out asks for ClickHouse's one
// granule, the value a server replaying the statement reports. Comparing the
// statement against an index with another granularity therefore plans the
// change, and against one granule it plans nothing, whichever way the schema
// reaches the comparison.
func TestSQLSkippingIndexWithoutGranularityMeansOneGranule(t *testing.T) {
	for _, test := range []struct {
		name        string
		granularity uint64
		want        []string
	}{
		{name: "the server holds another granularity", granularity: 4, want: []string{
			"ALTER TABLE `events` DROP INDEX `idx_payload`",
			"ALTER TABLE `events` ADD INDEX `idx_payload` lower(payload) TYPE minmax GRANULARITY 1",
		}},
		{name: "the server holds one granule", granularity: 1, want: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			source, _, err := sqlschema.Read([]byte("CREATE TABLE events (id UInt64, payload String, "+
				"INDEX idx_payload lower(payload) TYPE minmax) ENGINE = MergeTree ORDER BY tuple();"), "clickhouse")
			c.Assert(err, qt.IsNil)
			current := indexedCatalog(&chschema.ObservedIndex{IndexType: "minmax", Granularity: test.granularity})
			current.Tables[0].Facets = must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}))
			diff, err := schemadiff.CompareWithDialect(t.Context(), &source, current, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// The same statement as the current side of a file-to-file comparison stands
// for an index with one granule.
func TestSQLSkippingIndexWithoutGranularityIsOneGranuleAsTheCurrentDocument(t *testing.T) {
	for _, test := range []struct {
		name        string
		granularity string
		changed     bool
	}{
		{name: "one granule", granularity: "1", changed: false},
		{name: "four granules", granularity: "4", changed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current, _, err := sqlschema.Read([]byte("CREATE TABLE events (id UInt64, payload String, "+
				"INDEX idx_payload lower(payload) TYPE minmax) ENGINE = MergeTree ORDER BY tuple();"), "clickhouse")
			c.Assert(err, qt.IsNil)
			desired, _, err := sqlschema.Read([]byte("CREATE TABLE events (id UInt64, payload String, "+
				"INDEX idx_payload lower(payload) TYPE minmax GRANULARITY "+test.granularity+") ENGINE = MergeTree ORDER BY tuple();"), "clickhouse")
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareSchemas(t.Context(), &desired, &current, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.HasChanges(), qt.Equals, test.changed)
		})
	}
}
