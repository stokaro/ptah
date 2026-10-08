package ydb_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// changefeedsChanged is a plan whose only change to items is its changefeeds.
func changefeedsChanged(t *testing.T, desired, current []ydbschema.ChangefeedSpec) *difftypes.SchemaDiff {
	t.Helper()
	declaration := declaredFeeds(t, itemsDeclaration(), desired...)
	observation := observedFeeds(t, "", "items", current...)
	return modified(t, difftypes.TableDiff{TableName: "items", Desired: declaration, Current: observation,
		FeatureChanges: feedChanges(t, declaration, observation)})
}

// TestGenerateMigrationAST_Changefeeds_HappyPath pins how a plan changes a
// table's changefeeds: after the table's column changes, the drops first, so
// a swap stays within YDB's limit of changefeeds per table, then the
// additions, a changed changefeed among both, then the changes of a topic in
// place; and a note above each statement that ends a stream or a consumer's
// position, saying what is lost. A plan of this shape applied on 26.2.1.14
// and 25.1.4.7 and read back as declared.
func TestGenerateMigrationAST_Changefeeds_HappyPath(t *testing.T) {
	c := qt.New(t)
	audit := ast.TopicConsumerSpec{Name: "audit", SupportedCodecs: []string{"raw"}}
	current := []ydbschema.ChangefeedSpec{
		{Name: "gone", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "reader"}}},
		{Name: "moved", Mode: "KEYS_ONLY", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "a"}, {Name: "b"}}},
		{Name: "kept", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT6H", Consumers: []ast.TopicConsumerSpec{audit}},
	}
	desired := []ydbschema.ChangefeedSpec{
		{Name: "moved", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "a"}, {Name: "b"}}},
		{Name: "kept", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "audit"}}},
		{Name: "fresh", Mode: "NEW_IMAGE", Format: "DEBEZIUM_JSON", InitialScan: true},
	}
	diff := changefeedsChanged(t, desired, current)
	diff.TablesModified[0].Desired.Fields = itemsDeclaration(field("note", "TEXT", true)).Fields
	diff.TablesModified[0].ColumnsAdded = difftypes.ColumnChanges{field("note", "TEXT", true)}

	got := render(c, capability.YDB251(), diff)

	c.Assert(got, qt.Equals, "ALTER TABLE `items` ADD COLUMN `note` Utf8;\n"+
		"-- Changefeed gone of table items is dropped with its topic: the records nobody read are lost, and so is "+
		"consumer reader.\n"+
		"ALTER TABLE `items` DROP CHANGEFEED `gone`;\n"+
		"-- Changefeed moved of table items is dropped and added again, because YDB changes no option of a changefeed "+
		"in place. Its stream restarts: the records nobody read are lost, and consumers a, b lose their position and "+
		"start again from the beginning of the new stream.\n"+
		"ALTER TABLE `items` DROP CHANGEFEED `moved`;\n"+
		"ALTER TABLE `items` ADD CHANGEFEED `fresh` WITH (MODE = 'NEW_IMAGE', FORMAT = 'DEBEZIUM_JSON', INITIAL_SCAN = TRUE);\n"+
		"ALTER TABLE `items` ADD CHANGEFEED `moved` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n"+
		"ALTER TOPIC `items/moved` ADD CONSUMER `a`;\n"+
		"ALTER TOPIC `items/moved` ADD CONSUMER `b`;\n"+
		"-- Consumer audit of changefeed kept of table items is dropped and added again, because YDB keeps a "+
		"consumer's codecs once it has any. It loses its position and starts again from the beginning of the stream.\n"+
		"ALTER TOPIC `items/kept` SET (retention_period = Interval('P1D'));\n"+
		"ALTER TOPIC `items/kept` DROP CONSUMER `audit`;\n"+
		"ALTER TOPIC `items/kept` ADD CONSUMER `audit`;\n")
}

// TestGenerateMigrationAST_Changefeeds_RebuildCarriesThem pins a rebuild of a
// table holding changefeeds: each is dropped from the old table before the
// swap, since YDB moves no table that carries one, and the declared ones are
// added to the new table once it holds the name, with a note above the steps.
// The scratch table takes none. A plan of this shape applied on 26.2.1.14 and
// 25.1.4.7, with rows written before and after, and read back as declared.
func TestGenerateMigrationAST_Changefeeds_RebuildCarriesThem(t *testing.T) {
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", Important: true}}}
	declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true))
	declaration = declaredFeeds(t, declaration, feed)
	typeChange := []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
		note string
	}{
		{
			name: "a changefeed the comparison found unchanged",
			note: "Changefeed updates of table app/items is dropped before the swap and added again after it",
			diff: modified(t, difftypes.TableDiff{TableName: "app.items", Desired: declaration, Current: observedFeeds(t, "app", "items", feed), ColumnsModified: typeChange}),
			want: "ALTER TABLE `app/items` DROP CHANGEFEED `updates`;\n" +
				"ALTER TABLE `app/items` RENAME TO `app/__ptah_replaced_items`;\n" +
				"ALTER TABLE `app/__ptah_rebuild_items` RENAME TO `app/items`;\n" +
				"ALTER TABLE `app/items` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n" +
				"ALTER TOPIC `app/items/updates` ADD CONSUMER `audit` WITH (important = TRUE);\n" +
				"DROP TABLE `app/__ptah_replaced_items`;\n",
		},
		{
			name: "a changefeed the declaration replaces",
			note: "Changefeed old of table app/items is dropped with its topic: the records nobody read are lost.",
			diff: modified(t, difftypes.TableDiff{TableName: "app.items", Desired: declaration, ColumnsModified: typeChange,
				Current: observedFeeds(t, "app", "items", ydbschema.ChangefeedSpec{Name: "old", Mode: "KEYS_ONLY", Format: "JSON"}),
			}),
			want: "ALTER TABLE `app/items` DROP CHANGEFEED `old`;\n" +
				"ALTER TABLE `app/items` RENAME TO `app/__ptah_replaced_items`;\n" +
				"ALTER TABLE `app/__ptah_rebuild_items` RENAME TO `app/items`;\n" +
				"ALTER TABLE `app/items` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n" +
				"ALTER TOPIC `app/items/updates` ADD CONSUMER `audit` WITH (important = TRUE);\n" +
				"DROP TABLE `app/__ptah_replaced_items`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := renderRebuild(c, capability.YDB262(), test.diff)

			c.Assert(got, qt.Contains, test.note)
			c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n"+heldDefaults+
				heldIndexDefaults("app/__ptah_rebuild_items", "items_label")+"INSERT INTO")
			c.Assert(got, qt.Contains, test.want)
		})
	}
}

// TestGenerateMigrationAST_Changefeeds_FailurePath refuses, before any node, a
// changefeed change the target cannot make: by its key, and by the server's
// reason where YDB refuses it on every line.
func TestGenerateMigrationAST_Changefeeds_FailurePath(t *testing.T) {
	plain := ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}
	withSIDs := plain
	withSIDs.UserSIDs = true
	partitioned := plain
	partitioned.TopicMinActivePartitions = 2
	indexed := changefeedsChanged(t, []ydbschema.ChangefeedSpec{{Name: "items_label", Mode: "UPDATES", Format: "JSON"}}, nil)
	indexed.TablesModified[0].Desired.Indexes = []schemamodel.Index{{Name: "items_label", Fields: []string{"label"}}}
	tests := []struct {
		name        string
		caps        capability.Capabilities
		diff        *difftypes.SchemaDiff
		wantFeature string
		wantErr     string
	}{
		{
			name: "any change without the key", caps: capability.YDB262().With(capability.Changefeeds, false),
			diff:        changefeedsChanged(t, nil, []ydbschema.ChangefeedSpec{plain}),
			wantFeature: string(capability.Changefeeds),
			wantErr:     `changing the changefeeds of table "items", which requires target capability changefeeds, .*`,
		},
		{
			name: "USER_SIDS on 25.4", caps: capability.YDB254(),
			diff:        changefeedsChanged(t, []ydbschema.ChangefeedSpec{withSIDs}, nil),
			wantFeature: string(capability.ChangefeedUserSIDs),
			wantErr:     `changefeed "feed" of table "items" takes USER_SIDS, which requires target capability changefeed_user_sids, .*`,
		},
		{
			name: "a changefeed named after a declared index", caps: capability.YDB262(),
			diff:        indexed,
			wantFeature: `table "items"`,
			wantErr:     `table "items": changefeed "items_label" has the name of one of its indexes, .*`,
		},
		{
			name: "several topic partitions on an Int64 key", caps: capability.YDB262(),
			diff:        changefeedsChanged(t, []ydbschema.ChangefeedSpec{partitioned}, nil),
			wantFeature: `changefeed "feed" of table "items"`,
			wantErr:     `changefeed "feed" of table "items": its topic starts with 2 partitions, .* not Int64`,
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
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, test.wantFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
