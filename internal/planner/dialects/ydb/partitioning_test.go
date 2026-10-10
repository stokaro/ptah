package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// declareSettings gives a declaration the settings as the YDB owner's facet,
// or none for nil.
func declareSettings(declaration schemacapture.TableDeclaration, settings *ydbschema.TablePartitioning) schemacapture.TableDeclaration {
	if settings != nil {
		declaration.Table.Facets = must.Must(declaration.Table.Facets.With(&ydbschema.DesiredTablePartitioning{TablePartitioning: *settings}))
	}
	return declaration
}

// withSettingsChange adds to tableDiff the YDB owner's change of its table's
// settings, from current, nil for YDB's defaults, to desired, nil for a
// declaration stating none.
func withSettingsChange(tableDiff difftypes.TableDiff, desired, current *ydbschema.TablePartitioning) difftypes.TableDiff {
	change := &ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{}}
	if desired != nil {
		change.After.TablePartitioning = *desired
	}
	if current != nil {
		change.Before = &ydbschema.ObservedTablePartitioning{TablePartitioning: *current}
	}
	table := tableDiff.Desired.Table
	subject := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(table.Schema, table.Name)
	tableDiff.FeatureChanges = append(tableDiff.FeatureChanges, schemaext.ChangeRecord{Subject: subject, Value: change})
	return tableDiff
}

// holdSettings makes the read of the one table diff modifies hold the
// settings, as the reader's observed facet.
func holdSettings(diff *difftypes.SchemaDiff, current *ydbschema.TablePartitioning) *difftypes.SchemaDiff {
	if current != nil {
		observation := &diff.TablesModified[0].Current.Table
		observation.Facets = must.Must(observation.Facets.With(&ydbschema.ObservedTablePartitioning{TablePartitioning: *current}))
	}
	return diff
}

// partitioningChanged is a change of the settings of items, beside a column
// added and a column dropped, with the declaration holding desired.
func partitioningChanged(t *testing.T, desired, current *ydbschema.TablePartitioning) *difftypes.SchemaDiff {
	declaration := declareSettings(itemsDeclaration(field("note", "TEXT", true)), desired)
	return holdSettings(modified(t, withSettingsChange(difftypes.TableDiff{
		TableName:      "items",
		Desired:        declaration,
		ColumnsAdded:   difftypes.ColumnChanges{field("note", "TEXT", true)},
		ColumnsRemoved: difftypes.ColumnChanges{field("old", "TEXT", true)},
	}, desired, current)), current)
}

// TestGenerateMigrationAST_TablePartitioning_HappyPath pins where a change of a
// table's settings sits: after the column additions, before the column drops,
// in one statement naming every setting of each group it touches. A new
// table's settings go inside its CREATE TABLE. Plans of this shape applied on
// local-ydb 26.2.1.14 and 25.1.4.7 and read back as declared.
func TestGenerateMigrationAST_TablePartitioning_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "the minimum of a table splitting by load",
			diff: partitioningChanged(t, &ydbschema.TablePartitioning{ByLoad: new(true), MinPartitions: 4},
				&ydbschema.TablePartitioning{MinPartitions: 6}),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, " +
				"AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			// The owners' statements for a table's own facets sit together,
			// ordered by name; neither reads the other.
			name: "beside the TTL",
			diff: func() *difftypes.SchemaDiff {
				diff := partitioningChanged(t, &ydbschema.TablePartitioning{KeyBloomFilter: new(true)}, nil)
				diff.TablesModified[0].Desired.Fields = append(diff.TablesModified[0].Desired.Fields, field("ts", "TIMESTAMP", true))
				return addTTLChange(diff, &ydbschema.TTL{Column: "ts", Interval: "P1D"}, nil)
			}(),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` SET (KEY_BLOOM_FILTER = ENABLED);\n" +
				"ALTER TABLE `items` SET (TTL = Interval(\"P1D\") ON `ts`);\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "replicas and a filter alone",
			diff: partitioningChanged(t, &ydbschema.TablePartitioning{ReadReplicas: "ANY_AZ:2", KeyBloomFilter: new(true)}, nil),
			want: "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n" +
				"ALTER TABLE `items` SET (READ_REPLICAS_SETTINGS = \"ANY_AZ:2\", KEY_BLOOM_FILTER = ENABLED);\n" +
				"ALTER TABLE `items` DROP COLUMN `old`;\n",
		},
		{
			name: "a new table",
			diff: &difftypes.SchemaDiff{TablesAdded: []difftypes.TableCreation{{
				Name: "events",
				Table: schemamodel.Table{StructName: "E", Name: "events", Facets: must.Must(schemaext.NewFacets(
					&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{UniformPartitions: 4, KeyBloomFilter: new(true)}}))},
				Fields: []schemamodel.Field{{StructName: "E", Name: "id", Type: "BIGINT UNSIGNED", Primary: true}},
			}}},
			want: "CREATE TABLE `events` (\n" +
				"    `id` Uint64 NOT NULL,\n" +
				"    PRIMARY KEY (`id`)\n" +
				") WITH (KEY_BLOOM_FILTER = ENABLED, UNIFORM_PARTITIONS = 4);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.YDB251(), test.diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_TablePartitioning_FailurePath refuses, before any
// node, a change the target cannot make: the YDB owner by the key the target
// lacks and by YDB's reason for a declaration it refuses, and the host by the
// reason a change only a rebuild makes, naming how to ask for one.
func TestGenerateMigrationAST_TablePartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		caps        capability.Capabilities
		diff        *difftypes.SchemaDiff
		wantFeature string
		wantErr     string
	}{
		{
			name:        "partitioning without the key",
			caps:        capability.YDB262().With(capability.PartitioningOptions, false),
			diff:        partitioningChanged(t, &ydbschema.TablePartitioning{MinPartitions: 4}, nil),
			wantFeature: "YDB table partitioning planning",
			wantErr:     `changing the partitioning of table "items", which requires target capability partitioning_options, .*`,
		},
		{
			name:        "replicas going away without the key",
			caps:        capability.YDB262().With(capability.ReadReplicas, false),
			diff:        partitioningChanged(t, nil, &ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:1"}),
			wantFeature: "YDB table partitioning planning",
			wantErr:     `changing the read replicas of table "items", which requires target capability read_replicas, .*`,
		},
		{
			name:        "a declaration YDB refuses",
			caps:        capability.YDB262(),
			diff:        partitioningChanged(t, &ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64}, nil),
			wantFeature: "YDB table partitioning planning",
			wantErr:     `table "items": auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`,
		},
		{
			name:        "a starting layout on a table created without it",
			caps:        capability.YDB262(),
			diff:        partitioningChanged(t, &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}}}, nil),
			wantFeature: `table "items"`,
			wantErr: `table "items": it declares PARTITION_AT_KEYS with one split point, which YDB takes only when it ` +
				`creates a table .* where this table holds 1\. Declare auto_partitioning_min_partitions_count to change ` +
				`the minimum in place; YDB makes it by rebuilding the table, which Ptah plans when asked with --allow-table-rebuild`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var refusal *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Feature, qt.Equals, test.wantFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_TablePartitioning_Rebuild makes a change only a
// rebuild makes when one is asked for: the new table takes the declared
// settings in its CREATE TABLE, with the held value of every setting the
// declaration leaves out, and no SET is written for the old one.
func TestGenerateMigrationAST_TablePartitioning_Rebuild(t *testing.T) {
	tests := []struct {
		name       string
		desired    *ydbschema.TablePartitioning
		current    *ydbschema.TablePartitioning
		wantCreate string
	}{
		{
			name:    "a starting layout",
			desired: &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}, {"20"}}},
			current: nil,
			wantCreate: ") WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, " +
				"AUTO_PARTITIONING_BY_LOAD = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, KEY_BLOOM_FILTER = DISABLED, " +
				"PARTITION_AT_KEYS = ((10), (20)));\n",
		},
		{
			name:    "a starting layout over held settings",
			desired: &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}}},
			current: &ydbschema.TablePartitioning{BySize: new(false), ByLoad: new(true), MaxPartitions: 9,
				ReadReplicas: "PER_AZ:1", KeyBloomFilter: new(true)},
			wantCreate: ") WITH (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:1\", KEY_BLOOM_FILTER = ENABLED, PARTITION_AT_KEYS = ((10)));\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := declareSettings(appItems(field("label", "TEXT", true)), test.desired)
			diff := holdSettings(modified(t, withSettingsChange(difftypes.TableDiff{TableName: "app.items", Desired: declaration},
				test.desired, test.current)), test.current)
			got := renderRebuild(c, capability.YDB262(), diff)
			c.Assert(got, qt.Contains, rebuildNote+"CREATE TABLE `app/__ptah_rebuild_items` (")
			c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n"+test.wantCreate)
			c.Assert(got, qt.Not(qt.Contains), "ALTER TABLE `app/items` SET (")
			c.Assert(got, qt.Contains, "DROP TABLE `app/__ptah_replaced_items`;\n")
		})
	}
}

// TestGenerateMigrationAST_TablePartitioning_RebuildCarriesTheSettings writes a
// rebuilt table's declared settings into its new CREATE TABLE when the rebuild
// is for another change, so the settings survive the swap.
func TestGenerateMigrationAST_TablePartitioning_RebuildCarriesTheSettings(t *testing.T) {
	c := qt.New(t)
	declaration := declareSettings(appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		&ydbschema.TablePartitioning{MinPartitions: 4, ReadReplicas: "PER_AZ:1"})
	got := renderRebuild(c, capability.YDB262(), modified(t, difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	}))
	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n"+
		") WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, "+
		"AUTO_PARTITIONING_BY_LOAD = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, "+
		"READ_REPLICAS_SETTINGS = \"PER_AZ:1\", KEY_BLOOM_FILTER = DISABLED);\n")
}

// TestGenerateMigrationAST_TablePartitioning_RebuildKeepsTheHeldSettings writes
// what the old table and its indexes hold for every setting the declaration
// leaves out, so a rebuild changes no setting nobody declared: the new table
// would otherwise take the cluster's table profile. An index renamed in the
// same plan takes what it held under its old name.
func TestGenerateMigrationAST_TablePartitioning_RebuildKeepsTheHeldSettings(t *testing.T) {
	c := qt.New(t)
	declaration := declareSettings(appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		&ydbschema.TablePartitioning{MinPartitions: 4})
	diff := holdSettings(modified(t, difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	}), &ydbschema.TablePartitioning{BySize: new(false), ByLoad: new(true), MaxPartitions: 20, KeyBloomFilter: new(true)})
	diff.IndexesRenamed = []difftypes.IndexRename{{TableName: "app.items", From: "items_old_label", To: "items_label"}}
	diff.TablesModified[0].Current.Indexes = []catalog.Index{{Schema: "app", TableName: "items", Name: "items_old_label",
		Columns: []string{"label"}, Method: "GLOBAL SYNC",
		Facets: must.Must(schemaext.NewFacets(&ydbschema.ObservedIndexPartitioning{
			IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 3, ReadReplicas: "ANY_AZ:1"},
		}))}}

	got := renderRebuild(c, capability.YDB262(), diff)

	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n"+
		") WITH (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20, KEY_BLOOM_FILTER = ENABLED);\n"+
		"ALTER TABLE `app/__ptah_rebuild_items` ALTER INDEX `items_label` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, READ_REPLICAS_SETTINGS = \"ANY_AZ:1\");\n")
}

// TestGenerateMigrationAST_TablePartitioning_RebuildRefusesWhatItCannotWrite
// refuses a rebuild whose new table would carry a setting the target has no key
// for, by the key, before any node.
func TestGenerateMigrationAST_TablePartitioning_RebuildRefusesWhatItCannotWrite(t *testing.T) {
	c := qt.New(t)
	settings := &ydbschema.TablePartitioning{KeyBloomFilter: new(true)}
	declaration := declareSettings(appItems(field("label", "TEXT", true), field("n", "BIGINT", true)), settings)
	diff := modified(t, withSettingsChange(difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	}, settings, nil))

	nodes, err := ydb.NewWithCapabilities(capability.YDB262().With(capability.KeyBloomFilter, false)).
		WithTableRebuild(true).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)

	c.Assert(err, qt.ErrorMatches, `rebuilding table "app/items" with its key bloom filter, which requires target capability key_bloom_filter, .*`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(nodes, qt.IsNil)
}

// TestGenerateMigrationAST_TablePartitioning_RebuildIgnoresAFormatLimit
// rebuilds a table whose current state came from a document that cannot spell
// table settings: the format's limit says nothing about the table carrying any,
// where a read's record of storage settings stops the rebuild.
func TestGenerateMigrationAST_TablePartitioning_RebuildIgnoresAFormatLimit(t *testing.T) {
	c := qt.New(t)
	diff := notDescribing(modified(t, difftypes.TableDiff{
		TableName: "app.items", Desired: appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	}), coverage.Object{Kind: ydbschema.CoverageTableOption, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact})

	got := renderRebuild(c, capability.YDB262(), diff)

	c.Assert(got, qt.Contains, "CREATE TABLE `app/__ptah_rebuild_items` (")
}
