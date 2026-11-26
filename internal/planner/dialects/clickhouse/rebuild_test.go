package clickhouse_test

import (
	"cmp"
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
	// table is the name of the table, events when empty.
	table string
	// projections, assumptions, checks and comment are what the table holds
	// beside its columns, and constraints are what the declaration states.
	projections []catalog.Projection
	assumptions []catalog.Assumption
	checks      []catalog.Constraint
	comment     string
	constraints []schemamodel.Constraint
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
		Tables:      []schemamodel.Table{{Name: f.name(), StructName: "Event", Facets: must.Must(schemaext.NewFacets(f.after))}},
		Fields:      f.fields,
		Indexes:     f.indexes,
		Constraints: f.constraints,
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: f.name(), Facets: must.Must(schemaext.NewFacets(f.before)), Columns: f.current,
		Projections: f.projections, Assumptions: f.assumptions, Comment: f.comment,
	}}, Constraints: f.checks}
	for _, name := range f.extraTables {
		source.Tables = append(source.Tables, schemamodel.Table{Name: name, StructName: "Extra"})
		source.Fields = append(source.Fields, schemamodel.Field{Name: "id", StructName: "Extra", Type: "UInt64", Primary: true})
	}
	for _, name := range f.heldTables {
		current.Tables = append(current.Tables, catalog.Table{Name: name, Facets: must.Must(schemaext.NewFacets(f.before)), Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"}}})
	}
	diff, err := schemadiff.CompareWithDialect(c.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, "clickhouse", opts)
}

func allowRebuild() planext.Options {
	return planext.Options{AllowTableRebuild: true, Statements: planext.StatementStyleExact}
}

// The guard asks the server two questions before anything is created: whether
// the table's database is Atomic, the one engine that swaps two tables, and
// whether the user reads every row the table stores, which a row policy would
// hide from the copy.
func TestRebuild_GuardAsksForAnAtomicDatabaseAndEveryRow(t *testing.T) {
	c := qt.New(t)
	f := newRebuildFixture()
	f.after.Engine.Value = "ReplacingMergeTree(ver)"

	statements, err := f.plan(c, allowRebuild())

	c.Assert(err, qt.IsNil)
	c.Assert(statements[0], qt.Contains, "SELECT throwIf((SELECT count() FROM system.databases WHERE name = currentDatabase() AND engine = 'Atomic') = 0, "+
		"'cannot create events again: EXCHANGE TABLES swaps two tables only in an Atomic database, and the database of events is not one'), ")
	c.Assert(statements[0], qt.Contains, "throwIf((SELECT count() FROM `events`) != "+
		"(SELECT ifNull(sum(total_rows), 0) FROM system.tables WHERE database = currentDatabase() AND name = 'events'), "+
		"'cannot create events again: the user running the plan does not read every row events stores, ")
	c.Assert(statements[0], qt.Matches, `(?s).* SETTINGS apply_deleted_mask = 0`)
}

func TestRebuild_HappyPath(t *testing.T) {
	t.Run("an engine change copies the rows into a new table and swaps the two", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.HasLen, 10)
		c.Assert(statements[0], qt.Matches, "(?s)^-- ClickHouse table rebuild of events.*")
		c.Assert(statements[1], qt.Equals, "SYSTEM START MERGES `ptah_rebuild_events`")
		c.Assert(statements[2], qt.Contains, "CREATE TABLE `ptah_rebuild_events` (")
		c.Assert(statements[2], qt.Contains, "ENGINE = ReplacingMergeTree(ver) ORDER BY (id)")
		c.Assert(statements[3:], qt.DeepEquals, []string{
			"SELECT throwIf((SELECT arraySort(groupArray((name, compression_codec))) FROM system.columns " +
				"WHERE database = currentDatabase() AND table = 'ptah_rebuild_events' AND name IN ('id', 'name', 'ver')) != " +
				"(SELECT arraySort(groupArray((name, compression_codec))) FROM system.columns " +
				"WHERE database = currentDatabase() AND table = 'events' AND name IN ('id', 'name', 'ver')), " +
				"'cannot create events again: the copy ptah_rebuild_events does not have the codec events has for every column the two share. " +
				"events is as it was. Drop ptah_rebuild_events and run the plan again'), " +
				"throwIf((SELECT arraySort(groupArray((name, query))) FROM system.projections WHERE database = currentDatabase() AND table = 'ptah_rebuild_events') != " +
				"(SELECT arraySort(groupArray((name, query))) FROM system.projections WHERE database = currentDatabase() AND table = 'events'), " +
				"'cannot create events again: the copy ptah_rebuild_events does not have the projections events has. " +
				"events is as it was. Drop ptah_rebuild_events and run the plan again')",
			"SYSTEM STOP MERGES `ptah_rebuild_events`",
			"INSERT INTO `ptah_rebuild_events` (`id`, `name`, `ver`) SELECT `id`, `name`, `ver` FROM `events` " +
				"SETTINGS max_partitions_per_insert_block = 0, optimize_on_insert = 0, insert_deduplicate = 0",
			"WITH (SELECT count() FROM `ptah_rebuild_events`) AS copied, (SELECT count() FROM `events`) AS source SELECT " +
				"throwIf(copied < source, 'cannot create events again: the copy ptah_rebuild_events holds fewer rows than events, " +
				"so a row was written to events while it was copied, or a limit in the user''s settings cut the copy short. " +
				"events is as it was. Drop ptah_rebuild_events, stop writes to the table, and run the plan again'), " +
				"throwIf(copied > source, 'cannot create events again: the copy ptah_rebuild_events holds more rows than events, " +
				"so rows were deleted from events while it was copied, and the new table would bring them back. " +
				"events is as it was. Drop ptah_rebuild_events, stop writes to the table, and run the plan again')",
			"SYSTEM START MERGES `ptah_rebuild_events`",
			"EXCHANGE TABLES `events` AND `ptah_rebuild_events`",
			"DROP TABLE `ptah_rebuild_events` SYNC",
		})
		c.Assert(statements[0], qt.Contains, "Merges of ptah_rebuild_events stop while it is filled and counted, because its engine ReplacingMergeTree merges rows when parts merge; "+
			"they start again before the swap. SYSTEM STOP MERGES needs the SYSTEM MERGES privilege on ptah_rebuild_events; "+
			"the SYSTEM START MERGES before CREATE TABLE checks it while ptah_rebuild_events does not exist, so a user without it is refused before anything is created.")
	})

	t.Run("the plan says what each stop leaves, in every statement style", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.PartitionBy.Value = "toYYYYMM(d)"
		f.fields = append(f.fields, schemamodel.Field{Name: "d", StructName: "Event", Type: "Date"})
		f.current = append(f.current, catalog.Column{Name: "d", DataType: "Date", ColumnType: "Date", IsNullable: "NO"})

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(strings.Join(statements, "\n"), qt.Contains, "WARNING: the steps are not atomic, and rows written to the table while they run are lost")
		c.Assert(statements[0], qt.Contains, "table's PARTITION BY")
		c.Assert(statements[0], qt.Contains, "The new table is created as ptah_rebuild_events and filled from events, "+
			"which stays as it is until EXCHANGE TABLES swaps the two names. Then the old table, named ptah_rebuild_events by then, is dropped.")
		c.Assert(statements[0], qt.Contains, "If a statement before EXCHANGE TABLES fails, events is as it was, and ptah_rebuild_events holds part of the copy or all of it. "+
			"If the last DROP TABLE fails, events is the new table and holds every row the copy read, and ptah_rebuild_events is the old table. "+
			"Either way drop ptah_rebuild_events; Ptah does not rebuild events while it exists.")
		c.Assert(statements[0], qt.Contains, "A row written to events while it is copied stops the plan before the swap.")
		c.Assert(statements[0], qt.Contains, "Materialized views, dictionaries, Buffer tables, row policies and grants find events by name")
	})

	t.Run("a declared TTL stops the merges of the copy, which is then counted", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.PartitionBy.Value = "intDiv(id, 1000)"
		f.after.TTL.Value = "toDateTime(id) + INTERVAL 1 DAY"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[1], qt.Equals, "SYSTEM START MERGES `ptah_rebuild_events`")
		c.Assert(statements[2], qt.Contains, "TTL toDateTime(id) + INTERVAL 1 DAY")
		c.Assert(statements, qt.Contains, "SYSTEM STOP MERGES `ptah_rebuild_events`")
		c.Assert(statements, qt.Contains, "SYSTEM START MERGES `ptah_rebuild_events`")
		c.Assert(statements[0], qt.Contains, "because its TTL removes expired rows when parts merge")
		c.Assert(strings.Join(statements, "\n"), qt.Contains, "throwIf(copied > source, ")
	})

	t.Run("a MergeTree copy without a TTL merges no row away, and its merges run", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.PartitionBy.Value = "intDiv(id, 1000)"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		joined := strings.Join(statements, "\n")
		c.Assert(joined, qt.Not(qt.Contains), "MERGES")
		c.Assert(joined, qt.Contains, "throwIf(copied < source, ")
		c.Assert(joined, qt.Contains, "throwIf(copied > source, ")
	})

	t.Run("a table whose own merges remove rows is not refused for a copy that holds more", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.before.Engine = "ReplacingMergeTree(ver)"
		f.after = f.before.Desired()
		f.after.PartitionBy.Value = "intDiv(id, 1000)"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		joined := strings.Join(statements, "\n")
		c.Assert(joined, qt.Contains, "throwIf(copied < source, ")
		c.Assert(joined, qt.Not(qt.Contains), "throwIf(copied > source, ")
	})

	t.Run("codecs and projections are carried onto the copy before its rows", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.PartitionBy.Value = "intDiv(id, 1000)"
		f.current[1].StorageClauses = []string{"CODEC(ZSTD(3))"}
		f.projections = []catalog.Projection{
			{Name: "p_by_name", Definition: "(SELECT * ORDER BY name)"},
			{Name: "p q", Definition: "INDEX name TYPE basic"},
		}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		at := func(prefix string) int {
			return slices.IndexFunc(statements, func(s string) bool { return strings.HasPrefix(s, prefix) })
		}
		create := at("CREATE TABLE `ptah_rebuild_events` (")
		codec := at("ALTER TABLE `ptah_rebuild_events` MODIFY COLUMN `name` CODEC(ZSTD(3))")
		projection := at("ALTER TABLE `ptah_rebuild_events` ADD PROJECTION `p_by_name` (SELECT * ORDER BY name)")
		index := at("ALTER TABLE `ptah_rebuild_events` ADD PROJECTION `p q` INDEX name TYPE basic")
		check := at("SELECT throwIf((SELECT arraySort(groupArray((name, compression_codec)))")
		copied := at("INSERT INTO `ptah_rebuild_events`")
		order := []int{create, codec, projection, index, check, copied}
		c.Assert(slices.IsSorted(order) && create >= 0, qt.IsTrue, qt.Commentf("%v %q", order, statements))
		c.Assert(statements[0], qt.Contains, "The new table keeps what no declaration states and events has: "+
			"the codecs of columns name CODEC(ZSTD(3)); the projections p_by_name, p q")
		c.Assert(statements[0], qt.Not(qt.Contains), "merge_tree_settings")
	})

	t.Run("the new table holds the declared CHECKs and the ASSUME constraints the table has", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.constraints = []schemamodel.Constraint{
			{StructName: "Event", Table: "events", Type: "CHECK", CheckExpression: "ver > 0"},
			{StructName: "Event", Table: "events", Type: "CHECK", Name: "named", CheckExpression: "length(name) < 10"},
		}
		f.checks = []catalog.Constraint{
			{Name: "events_check", TableName: "events", Type: "CHECK", CheckClause: new("ver > 1")},
			{Name: "old_one", TableName: "events", Type: "CHECK", CheckClause: new("id > 0")},
		}
		f.assumptions = []catalog.Assumption{{Name: "a_name", Expression: "length(name) < 100"}}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		at := func(prefix string) int {
			return slices.IndexFunc(statements, func(s string) bool { return strings.HasPrefix(s, prefix) })
		}
		create := at("CREATE TABLE `ptah_rebuild_events` (")
		assume := at("ALTER TABLE `ptah_rebuild_events` ADD CONSTRAINT `a_name` ASSUME length(name) < 100")
		copied := at("INSERT INTO `ptah_rebuild_events`")
		order := []int{create, assume, copied}
		c.Assert(slices.IsSorted(order) && create >= 0, qt.IsTrue, qt.Commentf("%v %q", order, statements))
		c.Assert(statements[create], qt.Contains, "  CONSTRAINT `events_check` CHECK (ver > 0),\n  CONSTRAINT `named` CHECK (length(name) < 10)\n)")
		c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), " DROP CONSTRAINT ")
		c.Assert(statements[0], qt.Contains, "Each row copied into ptah_rebuild_events is checked against its CHECK constraints, events_check, named, "+
			"so a row that breaks one stops the plan before the swap, with code 469.")
		c.Assert(statements[0], qt.Contains, "which changes: the CHECK constraints it adds are named; "+
			"the CHECK constraints whose condition changes are events_check; the CHECK constraints it does not have are old_one\n")
		c.Assert(statements[0], qt.Contains, "The new table keeps what no declaration states and events has: the ASSUME constraints a_name\n")
	})

	t.Run("projections on an engine that merges rows ask for deduplicate_merge_projection_mode", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.projections = []catalog.Projection{{Name: "p_by_name", Definition: "(SELECT * ORDER BY name)"}}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[0], qt.Contains, "throwIf((SELECT value FROM system.merge_tree_settings WHERE name = 'deduplicate_merge_projection_mode') NOT IN ('drop', 'rebuild'), "+
			"'cannot create events again: events has projections, and ClickHouse keeps a projection on a ReplacingMergeTree table only when "+
			"deduplicate_merge_projection_mode is drop or rebuild, which neither the declaration nor the server sets. "+
			"Set it in the table''s declared SETTINGS, or drop the projections first') SETTINGS apply_deleted_mask = 0")
	})

	t.Run("a declared deduplicate_merge_projection_mode leaves the decision to the server", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.after.Settings.Value = "index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild'"
		f.projections = []catalog.Projection{{Name: "p_by_name", Definition: "(SELECT * ORDER BY name)"}}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[0], qt.Not(qt.Contains), "merge_tree_settings")
		c.Assert(statements, qt.Contains, "ALTER TABLE `ptah_rebuild_events` ADD PROJECTION `p_by_name` (SELECT * ORDER BY name)")
	})

	t.Run("the plan names the comments the new table does not have", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.comment = "tbl comment"
		f.current[1].Comment = "it's the name"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[0], qt.Contains, "The new table takes what the declaration states, which changes: "+
			"the table comment 'tbl comment' -> none; column name: comment 'it''s the name' -> none")
	})

	t.Run("an unnamed check keeps the name it has on the table", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields[2].Check = "ver > 0"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[2], qt.Contains, "CONSTRAINT `events_ver_check` CHECK")
		c.Assert(statements[2], qt.Not(qt.Contains), "ptah_rebuild_events_ver_check")
	})

	for _, test := range []struct {
		table, guard, create string
	}{
		{table: `sl\x`, guard: "(SELECT count() FROM `sl\\\\x`) != ", create: "CREATE TABLE `ptah_rebuild_sl\\\\x` ("},
		{table: "we'ird", guard: "'cannot create we''ird again: EXCHANGE TABLES", create: "CREATE TABLE `ptah_rebuild_we'ird` ("},
	} {
		t.Run("a table named "+test.table+" is quoted in every statement", func(t *testing.T) {
			c := qt.New(t)
			f := newRebuildFixture()
			f.table = test.table
			f.after.Engine.Value = "ReplacingMergeTree(ver)"

			statements, err := f.plan(c, allowRebuild())

			c.Assert(err, qt.IsNil)
			c.Assert(statements[0], qt.Contains, test.guard)
			c.Assert(statements[2], qt.Contains, test.create)
		})
	}

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
		c.Assert(joined, qt.Contains, "INSERT INTO `ptah_rebuild_events` (`id`, `ver`) SELECT `id`, `ver` FROM `events` SETTINGS ")
		c.Assert(joined, qt.Contains, "`note` String DEFAULT 'n'")
	})

	t.Run("a kept column keeps its default in the new table", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields[1].Default = "x"
		f.current[1].ColumnDefault = new("'x'")

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[2], qt.Contains, "`name` String DEFAULT 'x'")
	})

	t.Run("a column whose type changes is cast where the value is checked", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields[2].Type = "UInt16"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.Contains,
			"INSERT INTO `ptah_rebuild_events` (`id`, `name`, `ver`) SELECT `id`, `name`, accurateCast(`ver`, 'UInt16') FROM `events` "+
				"SETTINGS max_partitions_per_insert_block = 0, optimize_on_insert = 0, insert_deduplicate = 0")
	})

	// A codec fits the type it was chosen for, so the copy does not take the
	// codec of a column whose type changes, and the check of the carried codecs
	// leaves that column out.
	t.Run("the codec of a column whose type changes is left behind and named", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.fields[1].Type = "UInt64"
		f.current[1].StorageClauses = []string{"CODEC(ZSTD(3))"}
		f.current[2].StorageClauses = []string{"CODEC(Delta(4), ZSTD(1))"}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		joined := strings.Join(statements, "\n")
		c.Assert(joined, qt.Not(qt.Contains), "MODIFY COLUMN `name`")
		c.Assert(statements, qt.Contains, "ALTER TABLE `ptah_rebuild_events` MODIFY COLUMN `ver` CODEC(Delta(4), ZSTD(1))")
		c.Assert(joined, qt.Contains, "WHERE database = currentDatabase() AND table = 'events' AND name IN ('id', 'ver'))")
		c.Assert(statements[0], qt.Contains, "column name: codec ZSTD(3) -> none, the type changes")
		c.Assert(statements[0], qt.Contains, "The new table keeps what no declaration states and events has: the codecs of columns ver CODEC(Delta(4), ZSTD(1))")
	})

	t.Run("a sorting key that does not start with the old one follows into the primary key", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.before = &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id, name", PrimaryKey: "id, name", Settings: "index_granularity = 8192"}
		f.after = f.before.Desired()
		f.after.OrderBy.Value = "name, id"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements[1], qt.Contains, "ORDER BY (name, id) PRIMARY KEY (name, id)")
	})

	t.Run("a skipping index is created with the table, before its rows", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.indexes = []schemamodel.Index{{Name: "name_idx", StructName: "Event", Fields: []string{"name"}}}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		at := func(match func(string) bool) int { return slices.IndexFunc(statements, match) }
		create := at(func(s string) bool { return strings.HasPrefix(s, "CREATE TABLE `ptah_rebuild_events`") })
		index := at(func(s string) bool {
			return strings.HasPrefix(s, "ALTER TABLE `ptah_rebuild_events` ADD INDEX `name_idx`")
		})
		copied := at(func(s string) bool { return strings.HasPrefix(s, "INSERT INTO `ptah_rebuild_events`") })
		c.Assert(create >= 0 && create < index && index < copied, qt.IsTrue, qt.Commentf("%q", statements))
	})

	t.Run("a table that changes in place is not rebuilt, with or without the request", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.TTL.Value = "toDateTime(id) + INTERVAL 1 DAY"

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.IsNil)
		c.Assert(statements, qt.DeepEquals, []string{"ALTER TABLE `events` MODIFY TTL toDateTime(id) + INTERVAL 1 DAY"})
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

	t.Run("a table of the declaration has the copy's name", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.extraTables = []string{"ptah_rebuild_events"}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `(?s).*cannot create table events again: the declaration has a table named ptah_rebuild_events, the name the new table takes until the swap.*`)
		c.Assert(statements, qt.IsNil)
	})

	t.Run("the database holds a table of the copy's name", func(t *testing.T) {
		c := qt.New(t)
		f := newRebuildFixture()
		f.after.Engine.Value = "ReplacingMergeTree(ver)"
		f.heldTables = []string{"ptah_rebuild_events"}

		statements, err := f.plan(c, allowRebuild())

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `unsupported feature: cannot create table events again: the database holds a table named ptah_rebuild_events, the name the new table takes until the swap\. `+
			`A rebuild of events that stopped leaves it there, and events then holds every row that rebuild read, whichever statement it stopped at\. `+
			`Drop ptah_rebuild_events and run the plan again; if no rebuild left it there, rename it first`)
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

// A rebuild that would lose something no declaration states, or cannot prove
// it copies every row, is refused when asked, before anything is created.
func TestRebuild_RefusesWhatTheNewTableWouldLose(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*rebuildFixture)
		wantErr string
	}{
		{
			name: "a column TTL",
			change: func(f *rebuildFixture) {
				f.current[1].StorageClauses = []string{"CODEC(ZSTD(1))", "TTL toDateTime(id) + toIntervalDay(1)"}
			},
			wantErr: `unsupported feature: cannot create table events again: column name has TTL, which no declaration states and Ptah does not carry into the new table, ` +
				`so the new table would lose it: TTL toDateTime\(id\) \+ toIntervalDay\(1\)\. Remove it from the column first, or create the table again by hand`,
		},
		{
			name:    "column statistics",
			change:  func(f *rebuildFixture) { f.current[2].StorageClauses = []string{"STATISTICS(tdigest)"} },
			wantErr: `unsupported feature: cannot create table events again: column ver has STATISTICS, .*: STATISTICS\(tdigest\)\. .*`,
		},
		{
			name: "column settings",
			change: func(f *rebuildFixture) {
				f.current[1].StorageClauses = []string{"SETTINGS (max_compress_block_size = 1048576)"}
			},
			wantErr: `unsupported feature: cannot create table events again: column name has SETTINGS, .*: SETTINGS \(max_compress_block_size = 1048576\)\. .*`,
		},
		{
			name: "a projection over a column the declaration drops",
			change: func(f *rebuildFixture) {
				f.fields = f.fields[:1]
				f.fields = append(f.fields, schemamodel.Field{Name: "ver", StructName: "Event", Type: "UInt32"})
				f.projections = []catalog.Projection{{Name: "p_by_name", Definition: "(SELECT name, count() GROUP BY name)"}}
			},
			wantErr: `unsupported feature: cannot create table events again: its projection p_by_name uses column name, which the declaration drops, ` +
				`so the new table cannot have the projection\. Drop the projection first, or keep the column`,
		},
		{
			name: "an ASSUME constraint over a column the declaration drops",
			change: func(f *rebuildFixture) {
				f.fields = slices.Delete(f.fields, 1, 2)
				f.assumptions = []catalog.Assumption{{Name: "a_name", Expression: "length(name) < 100"}}
			},
			wantErr: `unsupported feature: cannot create table events again: its ASSUME constraint a_name uses column name, which the declaration drops, ` +
				`so the new table cannot have the constraint\. Drop the constraint first, or keep the column`,
		},
		{
			name: "a TinyLog table, which reports no row count",
			change: func(f *rebuildFixture) {
				f.before = &chschema.ObservedTable{Engine: "TinyLog"}
				f.after = f.before.Desired()
				f.after.Engine.Value = "MergeTree"
				f.after.OrderBy.Value = "id"
			},
			wantErr: `unsupported feature: .*ClickHouse has no ALTER TABLE that changes a table's ENGINE; Ptah does not create table events again, ` +
				`because its engine TinyLog reports no row count in system\.tables\. The rebuild compares that count with the rows the user running the plan reads, ` +
				`so a row policy cannot hide rows from the copy unnoticed, and without it nothing proves the copy holds every row\. Create the table again by hand\.`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newRebuildFixture()
			f.after.Engine.Value = "ReplacingMergeTree(ver)"
			test.change(f)

			statements, err := f.plan(c, allowRebuild())

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s)`+test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
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
	c.Assert(reverse, qt.Contains, "EXCHANGE TABLES `events` AND `ptah_rebuild_events`;")
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
					`The rebuild is not safe there\. The new table is created on this server only\. The copy reads one replica, which may lag\. `+
					`A new table at a fixed Keeper path meets the metadata the other replicas keep at that path\. `+
					`Create the table again by hand, on every replica\..*`)
				c.Assert(statements, qt.IsNil)
			})
		}
	}
}

// An engine that does not keep the table's rows on this server is refused,
// with or without the request: a copy out of it reads rows from elsewhere, and
// one into it writes them elsewhere or drops them.
func TestRebuild_EngineThatKeepsTheRowsElsewhere(t *testing.T) {
	tests := []struct {
		name   string
		change func(*rebuildFixture)
		engine string
	}{
		{
			name:   "the declaration drops every row",
			change: func(f *rebuildFixture) { f.after.Engine.Value = "Null" },
			engine: "Null",
		},
		{
			name:   "the declaration writes the rows to another server",
			change: func(f *rebuildFixture) { f.after.Engine.Value = "MySQL('db:3306', 'app', 'events', 'u', 'p')" },
			engine: `MySQL\('db:3306', 'app', 'events', 'u', 'p'\)`,
		},
		{
			name: "the table reads its rows from another server",
			change: func(f *rebuildFixture) {
				f.before.Engine = "Distributed('cluster', 'app', 'events_local')"
				f.after = f.before.Desired()
				f.after.Engine.Value = "MergeTree"
			},
			engine: `Distributed\('cluster', 'app', 'events_local'\)`,
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
				c.Assert(err, qt.ErrorMatches, `(?s).*Ptah does not create table events again, because its engine `+test.engine+
					` does not keep the table's rows on this server\. The copy would read rows from somewhere else, write them somewhere else, or drop them\. `+
					`Ptah copies rows only between engines that store them here: a MergeTree engine that is not replicated or shared, Memory, Log, StripeLog, and TinyLog as the new engine\. `+
					`Create the table again by hand\.`)
				c.Assert(statements, qt.IsNil)
			})
		}
	}
}

// Every engine that stores its rows here is rebuilt when asked.
func TestRebuild_EngineThatStoresTheRows(t *testing.T) {
	engines := []string{"ReplacingMergeTree(ver)", "CollapsingMergeTree(ver)", "Memory", "Log", "TinyLog", "StripeLog"}
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			c := qt.New(t)
			f := newRebuildFixture()
			f.after.Engine.Value = engine

			statements, err := f.plan(c, allowRebuild())

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.Contains, "EXCHANGE TABLES `events` AND `ptah_rebuild_events`")
		})
	}
}

// A table of the copy's name is what a stopped rebuild left, and it holds no
// row the table lacks, so a plan that rebuilds nothing drops it like any other
// table the declaration does not hold.
func TestRebuild_ALeftCopyIsDroppedLikeAnyOtherTable(t *testing.T) {
	c := qt.New(t)
	f := newRebuildFixture()
	f.heldTables = []string{"ptah_rebuild_events"}

	statements, err := f.plan(c, allowRebuild())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"DROP TABLE `ptah_rebuild_events`"})
}
