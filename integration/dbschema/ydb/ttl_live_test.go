//go:build integration

package ydb_test

import (
	"context"
	"path"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ttlSchema is the directory the TTL tests write into.
const ttlSchema = "ptah_ydb_ttl"

var ttlSchemas = []string{ttlSchema}

// ttlEvents is a table whose TTL reads a date column, and whose own columns
// are the ones fields names, each a nullable Timestamp. The narrow type is
// spelled as YDB spells it, so both lines build the same column.
func ttlEvents(policy *ydbschema.TTL, fields ...string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "Event", Name: "events", Schema: ttlSchema, Facets: ttlFacets(policy)}},
		Fields:          []schemamodel.Field{{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true}},
		FeatureCoverage: must.Must(ydbschema.TTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	for _, name := range fields {
		db.Fields = append(db.Fields, schemamodel.Field{StructName: "Event", Name: name, Type: "Timestamp", Nullable: true})
	}
	schemamodel.Finalize(db)
	return db
}

// ttlTokens is a table whose TTL reads an integer column counting
// milliseconds since the Unix epoch.
func ttlTokens(policy *ydbschema.TTL) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Token", Name: "tokens", Schema: ttlSchema, Facets: ttlFacets(policy)}},
		Fields: []schemamodel.Field{
			{StructName: "Token", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Token", Name: "expires", Type: "BIGINT UNSIGNED", Nullable: true},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// ttlFacets is the YDB owner's declaration of policy, bound to YDB, and no
// facet for none.
func ttlFacets(policy *ydbschema.TTL) schemaext.Facets {
	if policy == nil {
		return schemaext.Facets{}
	}
	facets := must.Must(schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: *policy}))
	return must.Must(facets.WithTargetScope(ydbschema.TTLKind, "ydb"))
}

// observedTTL is the TTL a read found on table, nil for none.
func observedTTL(c *qt.C, table catalog.Table) *ydbschema.ObservedTTL {
	c.Helper()
	observed, _, err := schemaext.FacetAs[*ydbschema.ObservedTTL](table.Facets, ydbschema.TTLKind)
	c.Assert(err, qt.IsNil)
	return observed
}

// policyOf reads the TTL of one table in the TTL directory, nil for none.
func policyOf(c *qt.C, conn *dbschema.DatabaseConnection, table string) *ydbschema.TTL {
	c.Helper()
	observed := observedTTL(c, tableNamed(c, readScoped(c, conn, ttlSchemas), ttlSchema, table))
	if observed == nil {
		return nil
	}
	return &observed.Policy
}

// declaredPolicy is the TTL the first table of db declares, nil for none.
func declaredPolicy(c *qt.C, db *schemamodel.Database) *ydbschema.TTL {
	c.Helper()
	declared, _, err := schemaext.FacetAs[*ydbschema.DesiredTTL](db.Tables[0].Facets, ydbschema.TTLKind)
	c.Assert(err, qt.IsNil)
	if declared == nil {
		return nil
	}
	return &declared.Policy
}

// TestYDBTTL_RoundTrip creates a table whose TTL reads a date column and one
// whose TTL reads an integer column with its unit, reads both back as
// declared, and plans nothing after, and the same apply run twice plans
// nothing either. The date interval is declared in hours and read back in
// days: YDB keeps the seconds.
func TestYDBTTL_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, ttlSchemas)
			c.Cleanup(func() { dropTables(c, conn, ttlSchemas) })

			declared := ttlEvents(&ydbschema.TTL{Column: "created_at", Interval: "PT720H"}, "created_at")
			tokens := ttlTokens(&ydbschema.TTL{Column: "expires", Interval: "PT1H", Unit: "MILLISECONDS"})
			declared.Tables = append(declared.Tables, tokens.Tables...)
			declared.Fields = append(declared.Fields, tokens.Fields...)

			created := planAgainst(c, conn, declared, ttlSchemas)
			c.Assert(created, qt.DeepEquals, []string{
				"CREATE TABLE `ptah_ydb_ttl/events` (\n    `id` Int64 NOT NULL,\n    `created_at` Timestamp,\n" +
					"    PRIMARY KEY (`id`)\n) WITH (TTL = Interval(\"PT720H\") ON `created_at`)",
				"CREATE TABLE `ptah_ydb_ttl/tokens` (\n    `id` Int64 NOT NULL,\n    `expires` Uint64,\n" +
					"    PRIMARY KEY (`id`)\n) WITH (TTL = Interval(\"PT1H\") ON `expires` AS MILLISECONDS)",
			})
			apply(c, conn, created)
			c.Assert(planAgainst(c, conn, declared, ttlSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, ttlSchemas))
			c.Assert(planAgainst(c, conn, declared, ttlSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, ttlSchemas)
			c.Assert(observedTTL(c, tableNamed(c, live, ttlSchema, "events")), qt.DeepEquals,
				&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}})
			c.Assert(observedTTL(c, tableNamed(c, live, ttlSchema, "tokens")), qt.DeepEquals,
				&ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "expires", Interval: "PT1H", Unit: "MILLISECONDS"}})
			c.Assert(live.NotDescribed.Describes(ydbschema.CoverageTTL, ttlSchema+".events"), qt.IsTrue)
		})
	}
}

// TestYDBTTL_ChangesInPlace changes a TTL on a table holding rows: its
// interval, then its column, which moves to a column the same plan adds
// while the old one is dropped, then its removal with the column it read.
// Each plan is the statements in the order YDB takes them -- the TTL after
// the column it reads exists, and before the column it read is dropped --
// and each ends with nothing left to plan and the rows in place.
func TestYDBTTL_ChangesInPlace(t *testing.T) {
	steps := []struct {
		name     string
		declared *schemamodel.Database
		want     []string
	}{
		{
			name:     "another interval",
			declared: ttlEvents(&ydbschema.TTL{Column: "created_at", Interval: "P2D"}, "created_at"),
			want:     []string{"ALTER TABLE `ptah_ydb_ttl/events` SET (TTL = Interval(\"P2D\") ON `created_at`)"},
		},
		{
			name:     "another column, the old one dropped",
			declared: ttlEvents(&ydbschema.TTL{Column: "expires_at", Interval: "P7D"}, "expires_at"),
			want: []string{
				"ALTER TABLE `ptah_ydb_ttl/events` ADD COLUMN `expires_at` Timestamp",
				"ALTER TABLE `ptah_ydb_ttl/events` SET (TTL = Interval(\"P7D\") ON `expires_at`)",
				"ALTER TABLE `ptah_ydb_ttl/events` DROP COLUMN `created_at`",
			},
		},
		{
			name:     "no TTL, its column dropped",
			declared: ttlEvents(nil),
			want: []string{
				"ALTER TABLE `ptah_ydb_ttl/events` RESET (TTL)",
				"ALTER TABLE `ptah_ydb_ttl/events` DROP COLUMN `expires_at`",
			},
		},
	}
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, ttlSchemas)
			c.Cleanup(func() { dropTables(c, conn, ttlSchemas) })
			apply(c, conn, planAgainst(c, conn,
				ttlEvents(&ydbschema.TTL{Column: "created_at", Interval: "P1D"}, "created_at"), ttlSchemas))
			apply(c, conn, []string{"UPSERT INTO `ptah_ydb_ttl/events` (id, created_at) " +
				"VALUES (1l, NULL), (2l, CurrentUtcTimestamp())"})

			for _, step := range steps {
				planned := planAgainst(c, conn, step.declared, ttlSchemas)
				c.Assert(planned, qt.DeepEquals, step.want, qt.Commentf("step %q", step.name))
				apply(c, conn, planned)
				c.Assert(planAgainst(c, conn, step.declared, ttlSchemas), qt.HasLen, 0, qt.Commentf("step %q", step.name))
				c.Assert(policyOf(c, conn, "events"), qt.DeepEquals, declaredPolicy(c, step.declared),
					qt.Commentf("step %q", step.name))
			}
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `ptah_ydb_ttl/events`"), qt.Equals, int64(2))
		})
	}
}

// setRunInterval sets the run interval of a table's TTL through the table
// service, which is the only way to set one: YQL has no spelling for it. The
// TTL is set whole, as the CLI's `ydb table ttl set` sets it.
func setRunInterval(c *qt.C, line ydbLine, table, column string, expireAfter, runInterval uint32) {
	c.Helper()
	ctx := c.Context()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(context.Background()) }()
	client := Ydb_Table_V1.NewTableServiceClient(ydbsdk.GRPCConn(driver))
	session, err := client.CreateSession(ctx, &Ydb_Table.CreateSessionRequest{})
	c.Assert(err, qt.IsNil)
	var created Ydb_Table.CreateSessionResult
	c.Assert(session.GetOperation().GetResult().UnmarshalTo(&created), qt.IsNil)
	defer func() {
		_, _ = client.DeleteSession(context.Background(), &Ydb_Table.DeleteSessionRequest{SessionId: created.GetSessionId()})
	}()
	altered, err := client.AlterTable(ctx, &Ydb_Table.AlterTableRequest{
		SessionId: created.GetSessionId(),
		Path:      path.Join(driver.Name(), table),
		TtlAction: &Ydb_Table.AlterTableRequest_SetTtlSettings{SetTtlSettings: &Ydb_Table.TtlSettings{
			Mode: &Ydb_Table.TtlSettings_DateTypeColumn{DateTypeColumn: &Ydb_Table.DateTypeColumnModeSettings{
				ColumnName: column, ExpireAfterSeconds: expireAfter,
			}},
			RunIntervalSeconds: runInterval,
		}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(altered.GetOperation().GetStatus(), qt.Equals, Ydb.StatusIds_SUCCESS,
		qt.Commentf("issues: %v", altered.GetOperation().GetIssues()))
}

// TestYDBTTL_KeepsARunIntervalYQLCannotWrite reads a TTL whose run interval
// was set through the table service, which YQL cannot write and which SET
// (TTL = ...) resets: measured, a table set to 1800 seconds reads back with no
// run interval after it. The policy reads back and plans nothing, the run
// interval is recorded as not described, and a change of the policy is
// refused before anything runs rather than planned as a SET that resets it.
// Removing the TTL takes the run interval with it, as the declaration asks.
func TestYDBTTL_KeepsARunIntervalYQLCannotWrite(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, ttlSchemas)
			c.Cleanup(func() { dropTables(c, conn, ttlSchemas) })
			declared := ttlEvents(&ydbschema.TTL{Column: "created_at", Interval: "PT1H"}, "created_at")
			apply(c, conn, planAgainst(c, conn, declared, ttlSchemas))
			setRunInterval(c, line, ttlSchema+"/events", "created_at", 3600, 1800)

			c.Assert(planAgainst(c, conn, declared, ttlSchemas), qt.HasLen, 0)
			live := readScoped(c, conn, ttlSchemas)
			c.Assert(live.NotDescribed.Describes(ydbschema.CoverageTTL, ttlSchema+".events"), qt.IsFalse)
			c.Assert(observedTTL(c, tableNamed(c, live, ttlSchema, "events")).RunIntervalSeconds, qt.Equals, uint64(1800))

			changed := ttlEvents(&ydbschema.TTL{Column: "created_at", Interval: "PT2H"}, "created_at")
			info := conn.Info()
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), changed, live, info, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
			)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*the TTL of table "ptah_ydb_ttl/events": the table's TTL runs every 1800 seconds, `+
				`which only the SDK and the CLI set, and SET \(TTL = \.\.\.\) resets it .*`)
			c.Assert(statements, qt.IsNil)

			removed := ttlEvents(nil, "created_at")
			c.Assert(planAgainst(c, conn, removed, ttlSchemas), qt.DeepEquals,
				[]string{"ALTER TABLE `ptah_ydb_ttl/events` RESET (TTL)"})
			apply(c, conn, planAgainst(c, conn, removed, ttlSchemas))
			c.Assert(planAgainst(c, conn, removed, ttlSchemas), qt.HasLen, 0)
			c.Assert(readScoped(c, conn, ttlSchemas).NotDescribed.Describes(ydbschema.CoverageTTL, ttlSchema+".events"), qt.IsTrue)
		})
	}
}
