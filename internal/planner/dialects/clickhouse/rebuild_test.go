package clickhouse_test

import (
	"cmp"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// rebuildFixture is a live `events` table sorted by id with the columns id,
// name and ver, and a declaration that starts from the same clauses. A test
// edits the declared clauses and fields and compares.
type rebuildFixture struct {
	before  *chschema.ObservedTable
	after   *chschema.DesiredTable
	fields  []schemamodel.Field
	indexes []schemamodel.Index
	current []catalog.Column
	// extraTables are tables the declaration holds beside events, and
	// heldTables are tables the database holds beside it that the declaration
	// does not name.
	extraTables []string
	heldTables  []string
	// heldComment is the comment of the tables in heldTables.
	heldComment string
	// table is the name of the table, events when empty.
	table string
	// ownerMissing leaves the table out of the database, which the
	// declaration creates.
	ownerMissing bool
}

func (f *rebuildFixture) name() string {
	return cmp.Or(f.table, "events")
}

func newRebuildFixture() *rebuildFixture {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id", Settings: "index_granularity = 8192"}
	return &rebuildFixture{
		before: before,
		after:  before.Desired(),
		fields: []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "name", StructName: "Event", Type: "String"},
			{Name: "ver", StructName: "Event", Type: "UInt32"},
		},
		current: []catalog.Column{
			{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "name", DataType: "String", ColumnType: "String", IsNullable: "NO"},
			{Name: "ver", DataType: "UInt32", ColumnType: "UInt32", IsNullable: "NO"},
		},
	}
}

func (f *rebuildFixture) plan(c *qt.C, opts planext.Options) ([]string, error) {
	c.Helper()
	runtime := must.Must(builtin.New())
	source := &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: f.name(), StructName: "Event", Facets: must.Must(schemaext.NewFacets(f.after))}},
		Fields:  f.fields,
		Indexes: f.indexes,
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: f.name(), Facets: must.Must(schemaext.NewFacets(f.before)), Columns: f.current,
	}}}
	if f.ownerMissing {
		current.Tables = nil
	}
	for _, name := range f.extraTables {
		source.Tables = append(source.Tables, schemamodel.Table{Name: name, StructName: "Extra"})
		source.Fields = append(source.Fields, schemamodel.Field{Name: "id", StructName: "Extra", Type: "UInt64", Primary: true})
	}
	for _, name := range f.heldTables {
		current.Tables = append(current.Tables, catalog.Table{Name: name, Comment: f.heldComment, Facets: must.Must(schemaext.NewFacets(f.before)), Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"}}})
	}
	diff, err := schemadiff.CompareWithDialect(c.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, "clickhouse", opts)
}

func allowRebuild() planext.Options {
	return planext.Options{AllowTableRebuild: true, Statements: planext.StatementStyleExact}
}

// The guard asks the server which objects depend on the table: the
// materialized views that read it and the dictionaries that load from it.
func TestRebuild_DependentsGuardReadsTheServersDependencies(t *testing.T) {
	c := qt.New(t)
	f := newRebuildFixture()
	f.after.Engine.Value = "ReplacingMergeTree(ver)"

	statements, err := f.plan(c, allowRebuild())

	c.Assert(err, qt.IsNil)
	c.Assert(statements[0], qt.Contains, "ARRAY JOIN arrayConcat(arrayZip(dependencies_database, dependencies_table), "+
		"arrayZip(loading_dependent_database, loading_dependent_table)) AS dependency WHERE database = currentDatabase() AND name = 'events'")
	c.Assert(statements[0], qt.Contains, "WHERE engine = 'MaterializedView' AND replaceAll(extractGroups(create_table_query, ")
	c.Assert(statements[0], qt.Contains, "WHERE engine = 'Buffer' AND extractGroups(engine_full, ")
}

// The pattern runs in the server's extractGroups(), which is RE2, the engine
// Go's regexp implements, so the pattern the plan carries is compiled here as
// the server compiles it, over create_table_query as 24.10 and 26.10 write it.
func TestRebuild_DependentsGuardFindsAMaterializedViewsTarget(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantGroup []string
	}{
		{
			name:      "a plain target",
			query:     "CREATE MATERIALIZED VIEW m.mv_to TO m.t (`id` UInt64, `k` UInt64) AS SELECT id, k FROM m.src",
			wantGroup: []string{"m", "t"},
		},
		{
			name:      "names in backticks",
			query:     "CREATE MATERIALIZED VIEW `my-db`.`mv x` TO `my-db`.`t-1` (`id` UInt64) AS SELECT id FROM `my-db`.s",
			wantGroup: []string{"`my-db`", "`t-1`"},
		},
		{
			name:      "a view that reads the table writes elsewhere",
			query:     "CREATE MATERIALIZED VIEW m.mv_from TO m.agg (`k` UInt64, `n` UInt64) AS SELECT k, count() AS n FROM m.t GROUP BY k",
			wantGroup: []string{"m", "agg"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			pattern := guardPattern(c, "create_table_query")

			found := pattern.FindStringSubmatch(test.query)

			c.Assert(found, qt.HasLen, 3)
			c.Assert(found[1:], qt.DeepEquals, test.wantGroup)
		})
	}
}

// A materialized view with its own engine writes to no other table, and a
// refreshable one owns the rows of the table it writes to, so neither matches.
func TestRebuild_DependentsGuardSkipsViewsThatDoNotFeedTheTable(t *testing.T) {
	queries := []string{
		"CREATE MATERIALIZED VIEW `my-db`.mv_inner (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
			"SETTINGS index_granularity = 8192 AS SELECT id FROM `my-db`.t",
		"CREATE MATERIALIZED VIEW `my-db`.r1 REFRESH EVERY 1 HOUR TO `my-db`.`t-1` (`id` UInt64) " +
			"DEFINER = ptah_user SQL SECURITY DEFINER AS SELECT CAST('1', 'UInt64') AS id",
		"CREATE MATERIALIZED VIEW `my-db`.r2 REFRESH EVERY 1 HOUR APPEND TO `my-db`.`t-1` (`id` UInt64) AS SELECT CAST('1', 'UInt64') AS id",
	}
	c := qt.New(t)
	pattern := guardPattern(c, "create_table_query")

	for _, query := range queries {
		c.Assert(pattern.FindStringSubmatch(query), qt.IsNil, qt.Commentf("%s", query))
	}
}

// A Buffer table's engine_full names its destination as two string literals,
// as 24.10 and 26.10 write it whatever spelling created the table.
func TestRebuild_DependentsGuardFindsABuffersDestination(t *testing.T) {
	tests := []struct {
		name       string
		engineFull string
		wantGroup  []string
	}{
		{
			name:       "a destination",
			engineFull: "Buffer('my-db', 't-1', 1, 10, 100, 10000, 1000000, 10000000, 100000000)",
			wantGroup:  []string{"my-db", "t-1"},
		},
		{
			name:       "no destination",
			engineFull: "Buffer('my-db', '', 1, 10, 100, 10000, 1000000, 10000000, 100000000)",
			wantGroup:  []string{"my-db", ""},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			pattern := guardPattern(c, "engine_full")

			found := pattern.FindStringSubmatch(test.engineFull)

			c.Assert(found, qt.HasLen, 3)
			c.Assert(found[1:], qt.DeepEquals, test.wantGroup)
		})
	}
}

// guardPattern plans a rebuild and reads the pattern its guard hands to
// extractGroups() over column out of the first statement, undoing the
// ClickHouse string escapes.
func guardPattern(c *qt.C, column string) *regexp.Regexp {
	c.Helper()
	f := newRebuildFixture()
	f.after.Engine.Value = "ReplacingMergeTree(ver)"
	statements, err := f.plan(c, allowRebuild())
	c.Assert(err, qt.IsNil)
	found := regexp.MustCompile(`extractGroups\(` + column + `, '((?:[^'\\]|''|\\.)*)'\)`).FindStringSubmatch(statements[0])
	c.Assert(found, qt.HasLen, 2, qt.Commentf("no extractGroups(%s) in %s", column, statements[0]))
	literal := strings.NewReplacer("''", "'", `\\`, `\`).Replace(found[1])
	return must.Must(regexp.Compile(literal))
}

func TestRebuild_HappyPath(t *testing.T) {
	t.Run("an engine change keeps the rows through a holding table", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.HasLen, 7)
		c.Assert(statements[0], qt.Matches, "(?s)^-- ClickHouse table rebuild of events.*")
		c.Assert(statements[0], qt.Contains, "SELECT toUInt8(concat('cannot create events again: these depend on it: '")
		c.Assert(statements[0], qt.Contains, "HAVING count() > 0")
		c.Assert(statements[1], qt.Contains, "CREATE TABLE ptah_rebuild_events (")
		c.Assert(statements[1], qt.Contains, "ENGINE = MergeTree ORDER BY (tuple())")
		c.Assert(statements[1], qt.Contains, "COMMENT 'ptah rebuild holding table for events'")
		c.Assert(statements[2:4], qt.DeepEquals, []string{
			"INSERT INTO `ptah_rebuild_events` (`id`, `name`, `ver`) SELECT `id`, `name`, `ver` FROM `events`",
			"DROP TABLE `events` SYNC",
		})
		c.Assert(statements[4], qt.Contains, "CREATE TABLE events (")
		c.Assert(statements[4], qt.Contains, "ENGINE = ReplacingMergeTree(ver) ORDER BY (id)")
		c.Assert(statements[5:], qt.DeepEquals, []string{
			"INSERT INTO `events` (`id`, `name`, `ver`) SELECT `id`, `name`, `ver` FROM `ptah_rebuild_events` SETTINGS max_partitions_per_insert_block = 0",
			"DROP TABLE `ptah_rebuild_events` SYNC",
		})
	})

	t.Run("the plan says it is not atomic, in every statement style", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.PartitionBy.Value = "toYYYYMM(d)"
		f.fields = append(f.fields, schemamodel.Field{Name: "d", StructName: "Event", Type: "Date"})
		f.current = append(f.current, catalog.Column{Name: "d", DataType: "Date", ColumnType: "Date", IsNullable: "NO"})

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(strings.Join(statements, "\n"), qt.Contains, "WARNING: the steps are not atomic, and rows written to the table while they run are lost")
		c.Assert(statements[0], qt.Contains, "table's PARTITION BY")
		c.Assert(statements[0], qt.Contains, "If the copy into ptah_rebuild_events fails, or the last DROP TABLE fails, events is whole and ptah_rebuild_events holds a copy of every row.")
		c.Assert(statements[0], qt.Contains, "Do not copy back, which doubles the rows.")
		c.Assert(statements[0], qt.Contains, "If a statement fails after DROP TABLE events and events is not created again, ptah_rebuild_events holds the only copy.")
		c.Assert(statements[0], qt.Contains, "Then run INSERT INTO events (`id`, `name`, `ver`, `d`) SELECT `id`, `name`, `ver`, `d` FROM ptah_rebuild_events.")
		c.Assert(statements[0], qt.Contains, "Ptah refuses every plan that would drop ptah_rebuild_events until this is done.")
	})

	t.Run("a column the table gains takes its default and a column it loses is not copied", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields = []schemamodel.Field{
			{Name: "id", StructName: "Event", Type: "UInt64"},
			{Name: "ver", StructName: "Event", Type: "UInt32"},
			{Name: "note", StructName: "Event", Type: "String", Default: "n"},
		}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		joined := strings.Join(statements, "\n")
		c.Assert(joined, qt.Contains, "Columns the table gains take their defaults: note")
		c.Assert(joined, qt.Contains, "Data of the columns the table loses is not carried over: name. A rename without a rename hint is a drop and an add")
		c.Assert(joined, qt.Contains, "INSERT INTO `ptah_rebuild_events` (`id`, `ver`) SELECT `id`, `ver` FROM `events`")
		c.Assert(joined, qt.Contains, "INSERT INTO `events` (`id`, `ver`) SELECT `id`, `ver` FROM `ptah_rebuild_events` SETTINGS max_partitions_per_insert_block = 0")
		c.Assert(joined, qt.Contains, "note String DEFAULT 'n'")
	})

	t.Run("a kept column keeps its default in the new table and has none in the holding table", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields[1].Default = "x"
		f.current[1].ColumnDefault = new("'x'")

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[1], qt.Not(qt.Contains), "DEFAULT")
		c.Assert(statements[4], qt.Contains, "name String DEFAULT 'x'")
	})

	t.Run("a column whose type changes is cast where the value is checked", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields[2].Type = "UInt16"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[2], qt.Equals,
			"INSERT INTO `ptah_rebuild_events` (`id`, `name`, `ver`) SELECT `id`, `name`, accurateCast(`ver`, 'UInt16') FROM `events`")
		c.Assert(statements[5], qt.Equals,
			"INSERT INTO `events` (`id`, `name`, `ver`) SELECT `id`, `name`, `ver` FROM `ptah_rebuild_events` SETTINGS max_partitions_per_insert_block = 0")
	})

	t.Run("a sorting key that does not start with the old one follows into the primary key", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.before = &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id, name", PrimaryKey: "id, name", Settings: "index_granularity = 8192"}
		f.after = f.before.Desired()
		f.after.OrderBy.Value = "name, id"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[4], qt.Contains, "ORDER BY (name, id) PRIMARY KEY (name, id)")
	})

	t.Run("a skipping index is created with the table, before its rows", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.indexes = []schemamodel.Index{{Name: "name_idx", StructName: "Event", Fields: []string{"name"}}}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		at := func(match func(string) bool) int { return slices.IndexFunc(statements, match) }
		create := at(func(s string) bool { return strings.HasPrefix(s, "CREATE TABLE events") })
		index := at(func(s string) bool { return strings.Contains(s, "ADD INDEX `name_idx`") })
		copyBack := at(func(s string) bool { return strings.HasPrefix(s, "INSERT INTO `events`") })
		c.Assert(create >= 0 && create < index && index < copyBack, qt.IsTrue, qt.Commentf("%q", statements))
	})

	t.Run("a table that changes in place is not rebuilt, with or without the request", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.TTL.Value = "toDateTime(id) + INTERVAL 1 DAY"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.DeepEquals, []string{"ALTER TABLE events MODIFY TTL toDateTime(id) + INTERVAL 1 DAY"})
	})
}

func TestRebuild_FailurePath(t *testing.T) {
	const ask = "asked with --allow-table-rebuild: the steps are not atomic, and rows written to the table while they run are lost"
	tests := []struct {
		name    string
		change  func(*rebuildFixture)
		wantErr string
	}{
		{
			name:    "an engine change names the request",
			change:  func(f *rebuildFixture) { f.after.Engine.Value = "ReplacingMergeTree(ver)" },
			wantErr: `unsupported feature: ClickHouse has no ALTER TABLE that changes a table's ENGINE; Ptah creates the table again and keeps its rows when ` + ask,
		},
		{
			name:    "a PARTITION BY change names the request",
			change:  func(f *rebuildFixture) { f.after.PartitionBy.Value = "id % 4" },
			wantErr: `unsupported feature: ClickHouse has no ALTER TABLE that changes a table's PARTITION BY; .*` + ask,
		},
		{
			name: "a settings change the server fixes at creation names the request",
			change: func(f *rebuildFixture) {
				f.after.Settings.Value = "index_granularity = 4096"
			},
			wantErr: `unsupported feature: ClickHouse fixes table setting index_granularity when the table is created; .*` + ask,
		},
		{
			name: "a reordered sorting key names the request",
			change: func(f *rebuildFixture) {
				f.before = &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id, name", PrimaryKey: "id", Settings: "index_granularity = 8192"}
				f.after = f.before.Desired()
				f.after.OrderBy.Value = "name, id"
			},
			wantErr: `unsupported feature: MODIFY ORDER BY can only drop the last expressions .*` + ask,
		},
		{
			name: "a sorting-key expression over a column the table has names the request",
			change: func(f *rebuildFixture) {
				f.before = &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id, name", PrimaryKey: "id", Settings: "index_granularity = 8192"}
				f.after = f.before.Desired()
				f.after.OrderBy.Value = "id, ver, name"
			},
			wantErr: `unsupported feature: column "ver" is already in the table, and ClickHouse adds to the sorting key only expressions over columns the same ALTER TABLE adds; .*` + ask,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newRebuildFixture()
			test.change(f)

			statements, err := f.plan(c, planext.Options{Statements: planext.StatementStyleExact})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr+`.*`)
			c.Assert(statements, qt.IsNil)
		})
	}

	t.Run("the request is named in the words the surface asks with", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"

		statements, err := f.plan(c, planext.Options{TableRebuildRequest: "PTAH_ALLOW_TABLE_REBUILD=1"})

		c.Assert(err, qt.ErrorMatches, `(?s).*when asked with PTAH_ALLOW_TABLE_REBUILD=1: .*`)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("a declaration that keeps none of the columns is refused with the request", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields = []schemamodel.Field{{Name: "other", StructName: "Event", Type: "UInt64"}}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `(?s).*cannot create table events again: the declaration keeps none of its columns.*`)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("a table of the declaration has the holding table's name", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.extraTables = []string{"ptah_rebuild_events"}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `(?s).*cannot create table events again: the declaration has a table named ptah_rebuild_events, where the rebuild keeps the rows.*`)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("the database holds a table of the holding table's name", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.heldTables = []string{"ptah_rebuild_events"}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `(?s).*cannot create table events again: the database holds a table named ptah_rebuild_events, where the rebuild keeps the rows;.*`)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("a declared primary key the new sorting key contradicts is refused", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.before = &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id, name", PrimaryKey: "id, name", Settings: "index_granularity = 8192"}
		f.after = f.before.Desired()
		f.after.OrderBy.Value = "name, id"
		f.after.PrimaryKey.Value = "id"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorMatches, `(?s).*PRIMARY KEY must be a prefix of ORDER BY.*`)
		c.Assert(statements, qt.IsNil)
	})
}

// The reverse of a rebuild is a rebuild, planned from the table the database
// holds, so a migration with a down direction is made with the request and
// refused without it.
func TestRebuild_BothDirections(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	f := newRebuildFixture()
	f.after.Engine.Value = "ReplacingMergeTree(ver)"
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(f.after))}},
		Fields: f.fields,
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: "events", Facets: must.Must(schemaext.NewFacets(f.before)), Columns: f.current,
	}}}
	diff, err := schemadiff.CompareWithDialect(c.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	options := generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
		Policy: generator.BidirectionalPlanPolicy{AllowTableRebuild: true, Statements: planext.StatementStyleExact},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), options)

	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Contains, "ENGINE = ReplacingMergeTree(ver)")
	c.Assert(reverse, qt.Contains, "DROP TABLE `events` SYNC;")
	c.Assert(reverse, qt.Contains, "ENGINE = MergeTree ORDER BY (id)")
	c.Assert(plan.Reverse.Recovery, qt.Not(qt.HasLen), 0)

	options.Policy.AllowTableRebuild = false
	refused, err := generator.PlanBidirectionalSchemaDiff(c.Context(), options)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*when asked with --allow-table-rebuild.*`)
	c.Assert(refused, qt.IsNil)
}

func TestRebuild_Replicated(t *testing.T) {
	tests := []struct {
		name   string
		change func(*rebuildFixture)
		engine string
	}{
		{
			name: "the table is replicated now",
			change: func(f *rebuildFixture) {
				f.before.Engine = "ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')"
				f.after = f.before.Desired()
				f.after.PartitionBy.Value = "id % 4"
			},
			engine: "ReplicatedMergeTree",
		},
		{
			name: "the declaration makes it replicated",
			change: func(f *rebuildFixture) {
				f.after.Engine.Value = "ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')"
			},
			engine: "ReplicatedMergeTree",
		},
		{
			name: "a shared engine",
			change: func(f *rebuildFixture) {
				f.after.Engine.Value = "SharedReplacingMergeTree"
			},
			engine: "SharedReplacingMergeTree",
		},
	}
	for _, test := range tests {
		for _, allow := range []bool{false, true} {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				f := newRebuildFixture()
				test.change(f)

				statements, err := f.plan(c, planext.Options{AllowTableRebuild: allow, Statements: planext.StatementStyleExact})

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, `(?s).*Ptah does not create table events again, because its engine `+test.engine+`.* is replicated\. `+
					`The rebuild is not safe there\. The holding table is local\. The copy reads one replica, which may lag\. `+
					`DROP TABLE \.\.\. SYNC removes one replica\. The new table meets the metadata the other replicas keep at the same Keeper path\. `+
					`Create the table again by hand, on every replica\..*`)
				c.Assert(statements, qt.IsNil)
			})
		}
	}
}

// A holding table a rebuild left holds the rows of the table when the rebuild
// stopped after the DROP TABLE, so no plan drops it, whatever else it holds.
func TestRebuild_UnfinishedHoldingTable(t *testing.T) {
	t.Run("an existing table: compare the rows before copying anything back", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.heldTables = []string{"ptah_rebuild_events"}
		f.heldComment = "ptah rebuild holding table for events"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `unsupported feature: table ptah_rebuild_events is the holding table of a rebuild of table events that stopped\. `+
			`The plan would drop it, and it may hold rows the table lacks\. `+
			`Table events exists\. The rebuild may have stopped before it dropped the table, or at its last statement, and then ptah_rebuild_events holds a copy of every row\. `+
			`Copying it back would double them\. `+
			"Compare SELECT count\\(\\) FROM `events` with SELECT count\\(\\) FROM `ptah_rebuild_events`\\. "+
			"If events holds every row, drop the holding table: DROP TABLE `ptah_rebuild_events`\\. "+
			"If it lacks rows, copy them back: INSERT INTO `events` \\(`id`\\) SELECT `id` FROM `ptah_rebuild_events`\\. Then drop the holding table\\. "+
			"If the count of events fails because the table does not exist, the rebuild stopped after it dropped the table: "+
			"create events from your declaration and copy the rows back, and do not drop the holding table first")
		c.Assert(statements, qt.IsNil)
	})

	t.Run("a missing table: create it by hand first", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.heldTables = []string{"ptah_rebuild_events"}
		f.heldComment = "ptah rebuild holding table for events"
		f.ownerMissing = true

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `unsupported feature: table ptah_rebuild_events is the holding table of a rebuild of table events that stopped\. `+
			`The plan would drop it, and it may hold rows the table lacks\. `+
			`Table events is missing, so the rebuild stopped after it dropped the table, and ptah_rebuild_events may hold the only copy of the rows\. `+
			`Create events by hand from your declaration\. `+
			"Copy the rows back: INSERT INTO `events` \\(`id`\\) SELECT `id` FROM `ptah_rebuild_events`\\. Check them\\. "+
			"Then drop the holding table: DROP TABLE `ptah_rebuild_events`")
		c.Assert(statements, qt.IsNil)
	})

	t.Run("the refusal holds without the request for a rebuild", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.heldTables = []string{"ptah_rebuild_events"}
		f.heldComment = "ptah rebuild holding table for events"

		statements, err := f.plan(c, planext.Options{Statements: planext.StatementStyleExact})

		c.Assert(err, qt.ErrorMatches, `unsupported feature: table ptah_rebuild_events is the holding table of a rebuild of table events that stopped\..*`)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("control: a table of that name without the comment is dropped like any other", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.heldTables = []string{"ptah_rebuild_events"}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.DeepEquals, []string{"DROP TABLE ptah_rebuild_events"})
	})
}
