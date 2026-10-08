//go:build integration

package ydb_test

import (
	"context"
	"path"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// changefeedSchema is the directory the changefeed tests write into.
const changefeedSchema = "ptah_ydb_changefeeds"

var changefeedSchemas = []string{changefeedSchema}

// changefeedDeclaration is table events, keyed on a Uint64 so its changefeed's
// topic may start with several partitions, carrying changefeeds.
func changefeedDeclaration(changefeeds ...ydbschema.ChangefeedSpec) *schemamodel.Database {
	var objects []schemaext.Object
	for _, stream := range changefeeds {
		objects = append(objects, ydbschema.DesiredObject(changefeedSchema, "events", stream))
	}
	db := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil)),
		Tables:          []schemamodel.Table{{StructName: "Event", Name: "events", Schema: changefeedSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Event", Name: "id", Type: "BIGINT UNSIGNED", Primary: true},
			{StructName: "Event", Name: "payload", Type: "TEXT", Nullable: true},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// lineChangefeeds are the changefeeds a round trip declares on a line, in the
// order the reader reports them, by name: every option the line takes, and
// consumers, one of them important and reading two codecs from a point in
// time. Debezium JSON writes no timestamps and no schema change records, so
// those options are declared on the JSON changefeed. Where the line has them,
// the options that came later -- user SIDs, schema changes, an
// auto-partitioned topic and a consumer's availability period -- are declared
// too.
func lineChangefeeds(caps capability.Capabilities) []ydbschema.ChangefeedSpec {
	audit := ydbschema.ChangefeedSpec{
		Name: "audit", Mode: "NEW_AND_OLD_IMAGES", Format: "DEBEZIUM_JSON", RetentionPeriod: "PT12H",
		InitialScan: true, TopicMinActivePartitions: 2,
		Consumers: []ast.TopicConsumerSpec{
			{Name: "billing", Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}},
			{Name: "search"},
		},
	}
	keys := ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON", VirtualTimestamps: true,
		ResolvedTimestamps: "PT10S"}
	keys.UserSIDs = caps.Has(capability.ChangefeedUserSIDs)
	keys.SchemaChanges = caps.Has(capability.ChangefeedSchemaChanges)
	keys.TopicAutoPartitioning = caps.Has(capability.ChangefeedTopicAutoPartitioning)
	late := map[bool][]ast.TopicConsumerSpec{true: {{Name: "late", AvailabilityPeriod: "PT1H"}}}
	keys.Consumers = late[caps.Has(capability.TopicConsumerAvailabilityPeriod)]
	return []ydbschema.ChangefeedSpec{audit, keys}
}

// changefeedsOf reads table events' changefeeds back from the server.
func changefeedsOf(c *qt.C, conn *dbschema.DatabaseConnection) []ydbschema.ChangefeedSpec {
	c.Helper()
	return observedChangefeeds(c, readScoped(c, conn, changefeedSchemas), changefeedSchema, "events")
}

// TestYDBChangefeeds_RoundTrip applies a table whose changefeeds use every
// option the line takes, reads them back from DescribeTable and from
// DescribeTopic on each changefeed's path, and plans nothing after; applying
// the same declaration again plans nothing too.
func TestYDBChangefeeds_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, changefeedSchemas)
			c.Cleanup(func() { dropTables(c, conn, changefeedSchemas) })
			changefeeds := lineChangefeeds(line.preset())
			declared := changefeedDeclaration(changefeeds...)

			apply(c, conn, planAgainst(c, conn, declared, changefeedSchemas))
			c.Assert(planAgainst(c, conn, declared, changefeedSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, changefeedSchemas))
			c.Assert(planAgainst(c, conn, declared, changefeedSchemas), qt.HasLen, 0)

			c.Assert(changefeedsOf(c, conn), qt.DeepEquals, changefeeds)
		})
	}
}

// TestYDBChangefeeds_SwapAtLimit requires a dependency even when the added
// stream sorts before the removed one. Adding first would exceed YDB's limit.
func TestYDBChangefeeds_SwapAtLimit(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, changefeedSchemas)
			c.Cleanup(func() { dropTables(c, conn, changefeedSchemas) })
			var streams []ydbschema.ChangefeedSpec
			for _, name := range []string{"b0", "b1", "b2", "b3", "z_old"} {
				streams = append(streams, ydbschema.ChangefeedSpec{Name: name, Mode: "UPDATES", Format: "JSON"})
			}
			apply(c, conn, planAgainst(c, conn, changefeedDeclaration(streams...), changefeedSchemas))
			c.Assert(changefeedsOf(c, conn), qt.HasLen, 5)
			streams[4].Name = "a_new"
			declared := changefeedDeclaration(streams...)
			plan := planAgainst(c, conn, declared, changefeedSchemas)
			c.Assert(plan, qt.HasLen, 2)
			c.Assert(plan[0], qt.Contains, "DROP CHANGEFEED `z_old`")
			c.Assert(plan[1], qt.Contains, "ADD CHANGEFEED `a_new`")
			apply(c, conn, plan)
			actual := changefeedsOf(c, conn)
			c.Assert(actual, qt.HasLen, 5)
			c.Assert(actual[0].Name, qt.Equals, "a_new")
			c.Assert(planAgainst(c, conn, declared, changefeedSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBChangefeeds_ChangesInPlace changes what YDB changes in place -- the
// retention, and the consumers, added, changed and dropped -- by ALTER TOPIC
// alone, and what it does not -- an option of the changefeed, and a
// consumer's codecs taken away -- by dropping and adding it again. Each step
// plans the statements it should and ends with nothing left to plan.
func TestYDBChangefeeds_ChangesInPlace(t *testing.T) {
	topic := "`" + changefeedSchema + "/events/feed`"
	table := "`" + changefeedSchema + "/events`"
	feed := func(mode, retention string, consumers ...ast.TopicConsumerSpec) ydbschema.ChangefeedSpec {
		return ydbschema.ChangefeedSpec{Name: "feed", Mode: mode, Format: "JSON", RetentionPeriod: retention, Consumers: consumers}
	}
	steps := []struct {
		name string
		feed ydbschema.ChangefeedSpec
		want []string
	}{
		{name: "the changefeed added", feed: feed("UPDATES", "PT6H",
			ast.TopicConsumerSpec{Name: "a", SupportedCodecs: []string{"raw"}}, ast.TopicConsumerSpec{Name: "b"}),
			want: []string{
				"ALTER TABLE " + table + " ADD CHANGEFEED `feed` WITH (MODE = 'UPDATES', FORMAT = 'JSON', RETENTION_PERIOD = Interval('PT6H'))",
				"ALTER TOPIC " + topic + " ADD CONSUMER `a` WITH (supported_codecs = 'raw')",
				"ALTER TOPIC " + topic + " ADD CONSUMER `b`",
			}},
		{name: "the retention set back, a consumer changed, one dropped and one added", feed: feed("UPDATES", "",
			ast.TopicConsumerSpec{Name: "a", Important: true, SupportedCodecs: []string{"raw", "zstd"}},
			ast.TopicConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:00Z"}),
			want: []string{
				"ALTER TOPIC " + topic + " SET (retention_period = Interval('P1D'))",
				"ALTER TOPIC " + topic + " DROP CONSUMER `b`",
				"ALTER TOPIC " + topic + " ALTER CONSUMER `a` SET (important = TRUE, read_from = Timestamp('1970-01-01T00:00:00Z'), " +
					"supported_codecs = 'raw,zstd')",
				"ALTER TOPIC " + topic + " ADD CONSUMER `c` WITH (read_from = Timestamp('2026-01-01T00:00:00Z'))",
			}},
		{name: "a consumer's codecs taken away", feed: feed("UPDATES", "",
			ast.TopicConsumerSpec{Name: "a", Important: true}, ast.TopicConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:00Z"}),
			want: []string{
				"-- Consumer a of changefeed feed of table " + changefeedSchema + ".events is dropped and added again, " +
					"because YDB keeps a consumer's codecs once it has any. It loses its position and starts again from " +
					"the beginning of the stream.\n" +
					"ALTER TOPIC " + topic + " DROP CONSUMER `a`",
				"ALTER TOPIC " + topic + " ADD CONSUMER `a` WITH (important = TRUE)",
			}},
		{name: "the mode changed", feed: feed("KEYS_ONLY", "",
			ast.TopicConsumerSpec{Name: "a", Important: true}, ast.TopicConsumerSpec{Name: "c", ReadFrom: "2026-01-01T00:00:00Z"}),
			want: []string{
				"-- Changefeed feed of table " + changefeedSchema + ".events is dropped and added again, because YDB " +
					"changes no option of a changefeed in place. Its stream restarts: the records nobody read are lost, " +
					"and consumers a, c lose their position and start again from the beginning of the new stream.\n" +
					"ALTER TABLE " + table + " DROP CHANGEFEED `feed`",
				"ALTER TABLE " + table + " ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON')",
				"ALTER TOPIC " + topic + " ADD CONSUMER `a` WITH (important = TRUE)",
				"ALTER TOPIC " + topic + " ADD CONSUMER `c` WITH (read_from = Timestamp('2026-01-01T00:00:00Z'))",
			}},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, changefeedSchemas)
			c.Cleanup(func() { dropTables(c, conn, changefeedSchemas) })
			apply(c, conn, planAgainst(c, conn, changefeedDeclaration(), changefeedSchemas))

			for _, step := range steps {
				declared := changefeedDeclaration(step.feed)
				planned := planAgainst(c, conn, declared, changefeedSchemas)
				c.Assert(planned, qt.DeepEquals, step.want, qt.Commentf("step: %s", step.name))
				apply(c, conn, planned)
				c.Assert(planAgainst(c, conn, declared, changefeedSchemas), qt.HasLen, 0, qt.Commentf("step: %s", step.name))
				c.Assert(changefeedsOf(c, conn), qt.DeepEquals, []ydbschema.ChangefeedSpec{step.feed}, qt.Commentf("step: %s", step.name))
			}

			removed := planAgainst(c, conn, changefeedDeclaration(), changefeedSchemas)
			c.Assert(removed, qt.DeepEquals, []string{"-- Changefeed feed of table " + changefeedSchema + ".events is " +
				"dropped with its topic: the records nobody read are lost, and so are consumers a, c.\n" +
				"ALTER TABLE " + table + " DROP CHANGEFEED `feed`"})
			apply(c, conn, removed)
			c.Assert(changefeedsOf(c, conn), qt.IsNil)
		})
	}
}

// addChangefeedWithAttributes adds changefeed tagged to table events through
// the table service, which takes attributes on a changefeed where YQL has no
// spelling for them.
func addChangefeedWithAttributes(c *qt.C, line ydbLine) {
	c.Helper()
	ctx := context.Background()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(ctx) }()
	client := Ydb_Table_V1.NewTableServiceClient(ydbsdk.GRPCConn(driver))
	session, err := client.CreateSession(ctx, &Ydb_Table.CreateSessionRequest{})
	c.Assert(err, qt.IsNil)
	var created Ydb_Table.CreateSessionResult
	c.Assert(session.GetOperation().GetResult().UnmarshalTo(&created), qt.IsNil)
	defer func() {
		_, _ = client.DeleteSession(ctx, &Ydb_Table.DeleteSessionRequest{SessionId: created.GetSessionId()})
	}()
	altered, err := client.AlterTable(ctx, &Ydb_Table.AlterTableRequest{
		SessionId: created.GetSessionId(),
		Path:      path.Join(driver.Name(), changefeedSchema, "events"),
		AddChangefeeds: []*Ydb_Table.Changefeed{{Name: "tagged", Mode: Ydb_Table.ChangefeedMode_MODE_UPDATES,
			Format: Ydb_Table.ChangefeedFormat_FORMAT_JSON, Attributes: map[string]string{"owner": "team"}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(altered.GetOperation().GetStatus(), qt.Equals, Ydb.StatusIds_SUCCESS)
}

// TestYDBChangefeeds_LeavesWhatItDoesNotModel records a changefeed holding
// attributes, which Ptah does not model, as not described rather than reading
// it without them: a plan neither drops it nor changes it, and a declaration
// of the same name is withheld rather than planned as an ADD the server
// refuses.
func TestYDBChangefeeds_LeavesWhatItDoesNotModel(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, changefeedSchemas)
			c.Cleanup(func() { dropTables(c, conn, changefeedSchemas) })
			apply(c, conn, planAgainst(c, conn, changefeedDeclaration(), changefeedSchemas))
			addChangefeedWithAttributes(c, line)

			live := readScoped(c, conn, changefeedSchemas)

			c.Assert(observedChangefeeds(c, live, changefeedSchema, "events"), qt.IsNil)
			c.Assert(live.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, ydbschema.ChangefeedRef(changefeedSchema, "events", "tagged")).State, qt.Equals, schemaext.Unrepresentable)
			for _, desired := range []*schemamodel.Database{
				changefeedDeclaration(),
				changefeedDeclaration(ydbschema.ChangefeedSpec{Name: "tagged", Mode: "UPDATES", Format: "JSON"}),
			} {
				diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, desired, live, nil, must.Must(builtin.New()))
				c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
				c.Assert(diff, qt.IsNil)
			}
		})
	}
}

// TestYDBChangefeeds_RebuildCarriesThem rebuilds a table carrying a
// changefeed to change a column's type: the plan drops the changefeed before
// moving the old table, which YDB refuses to move with one, and adds it with
// its consumer to the new table. The rows survive, the changefeed reads back
// as declared, and nothing is left to plan.
func TestYDBChangefeeds_RebuildCarriesThem(t *testing.T) {
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT6H",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", Important: true}}}
	withFeed := func(db *schemamodel.Database) {
		table := db.Tables[0]
		db.FeatureObjects = must.Must(schemaext.NewObjects(ydbschema.DesiredObject(table.Schema, table.Name, feed)))
		db.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil))
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			cleanRebuild(c, conn)
			c.Cleanup(func() { cleanRebuild(c, conn) })
			seedRebuildItems(c, conn, rebuildItems(withFeed), twoRows)
			after := rebuildItems(withFeed, withField("n", func(f *schemamodel.Field) { f.Type = "BIGINT" }))

			file, err := rebuildPlan(c, conn, after, true)
			c.Assert(err, qt.IsNil)
			drop := strings.Index(file, "ALTER TABLE `"+rebuildSchema+"/items` DROP CHANGEFEED `updates`;")
			move := strings.Index(file, "ALTER TABLE `"+rebuildSchema+"/items` RENAME TO")
			add := strings.Index(file, "ALTER TABLE `"+rebuildSchema+"/items` ADD CHANGEFEED `updates`")
			c.Assert(drop >= 0 && drop < move && move < add, qt.IsTrue, qt.Commentf("plan:\n%s", file))
			c.Assert(file, qt.Contains, "Its stream restarts")

			c.Assert(rebuildMigrator(c, conn, file, nil).MigrateUp(c.Context()), qt.IsNil)

			c.Assert(planAgainst(c, conn, after, rebuildSchemas), qt.HasLen, 0)
			live := readScoped(c, conn, rebuildSchemas)
			c.Assert(tableNames(live), qt.DeepEquals, []string{rebuildSchema + "|items"})
			c.Assert(observedChangefeeds(c, live, rebuildSchema, "items"), qt.DeepEquals, []ydbschema.ChangefeedSpec{feed})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `"+rebuildSchema+"/items`"), qt.Equals, int64(2))
		})
	}
}

func observedChangefeeds(c *qt.C, database *catalog.Database, schema, table string) []ydbschema.ChangefeedSpec {
	c.Helper()
	tableNamed(c, database, schema, table)
	streams, err := ydbschema.ObservedChangefeeds(database.FeatureObjects, schema, table)
	c.Assert(err, qt.IsNil)
	return streams
}
