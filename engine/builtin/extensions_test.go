package builtin_test

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerrender"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsrender"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
	"ptah.run/engine/builtin/internal/dialects/mssql"
	"ptah.run/engine/builtin/internal/dialects/mysql"
	"ptah.run/engine/builtin/internal/dialects/oracle"
	"ptah.run/engine/builtin/internal/dialects/postgres"
	"ptah.run/engine/builtin/internal/dialects/sqlite"
	"ptah.run/engine/builtin/internal/dialects/ydb"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyrender"
	"ptah.run/internal/astrouteguard"
	"ptah.run/internal/ydbextensions"
)

type extensionFixture struct {
	payload ast.ExtensionPayload
	wantSQL string
}

func extensionFixtures() []extensionFixture {
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	grown := feed.Clone()
	grown.RetentionPeriod = "PT2H"
	return []extensionFixture{
		{payload: &ydbast.AddChangefeed{Changefeed: feed}, wantSQL: "ALTER TABLE `items` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n"},
		{payload: &ydbast.DropChangefeed{Name: "updates"}, wantSQL: "ALTER TABLE `items` DROP CHANGEFEED `updates`;\n"},
		{payload: &ydbast.AlterChangefeedTopic{Changefeed: grown, Previous: feed}, wantSQL: "ALTER TOPIC `items/updates` SET (retention_period = Interval('PT2H'));\n"},
		{payload: &ydbast.AlterTTL{Change: ydbdiff.TTL{After: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}}}},
			wantSQL: "ALTER TABLE `items` SET (TTL = Interval(\"P30D\") ON `created_at`);\n"},
	}
}

func clickhouseTTLFixture() extensionFixture {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "created_at + INTERVAL 1 DAY"}
	after := before.Desired()
	after.TTL.Value = ""
	return extensionFixture{payload: &chast.AlterTTL{Change: chdiff.Table{Before: before, After: after}}, wantSQL: "ALTER TABLE items REMOVE TTL;\n"}
}

// cockroachDBRowTTLFixture drops one parameter and changes the expression, so
// the fixture lowers to both statements a row-level TTL change can need. The
// header and the closing blank line are what the PostgreSQL-family renderer
// writes around every ALTER TABLE.
func cockroachDBRowTTLFixture() extensionFixture {
	change := crdbdiff.RowTTL{
		Before: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"}},
		After:  &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 day'"}},
	}
	return extensionFixture{payload: &crdbast.AlterRowTTL{Change: change},
		wantSQL: "-- ALTER statements: --\nALTER TABLE \"items\" SET (ttl_expiration_expression = 'expires_at + INTERVAL ''1 day''');\n" +
			"ALTER TABLE \"items\" RESET (ttl_job_cron);\n\n"}
}

func spannerRowDeletionFixture() extensionFixture {
	change := spannerdiff.RowDeletion{
		Before: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}},
		After:  &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "60 days"}},
	}
	return extensionFixture{payload: &spannerast.AlterRowDeletion{Change: change},
		wantSQL: "-- ALTER statements: --\nALTER TABLE \"items\" ALTER TTL INTERVAL '60 days' ON \"created_at\";\n\n"}
}

func clickhouseIndexFixture() extensionFixture {
	return extensionFixture{payload: &chast.AddSkippingIndex{Name: "idx_c", Expression: "c"}, wantSQL: "ALTER TABLE `items` ADD INDEX `idx_c` c TYPE minmax GRANULARITY 1;\n"}
}

func clickhouseDropIndexFixture() extensionFixture {
	return extensionFixture{payload: &chast.DropSkippingIndex{Name: "idx_c"}, wantSQL: "ALTER TABLE `items` DROP INDEX `idx_c`;\n"}
}

func clickhouseRefreshFixture() extensionFixture {
	return extensionFixture{payload: &chast.ModifyRefresh{Schedule: chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR"}}, wantSQL: "ALTER TABLE `items` MODIFY REFRESH EVERY 1 HOUR;\n"}
}

func clickhouseRowPolicyFixture() extensionFixture {
	filter := "tenant = 1"
	return extensionFixture{payload: &chast.RowPolicy{Database: "app", Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(nil,
		&chschema.DesiredRowPolicy{Filter: &filter, Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}})},
		wantSQL: "CREATE ROW POLICY `tenant` ON `app`.`orders` USING (tenant = 1) AS RESTRICTIVE TO ALL EXCEPT `admin`;\n"}
}

func coordinationFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.CoordinationNode{Schema: "app", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}, wantSQL: "CREATE COORDINATION NODE `app/locks`;\n"}
}

func streamingFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Schema: "jobs.daily", Name: "copy.events",
		Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}},
		wantSQL: "CREATE STREAMING QUERY `jobs.daily/copy.events` WITH (RUN = FALSE, RESOURCE_POOL = `default`) AS DO BEGIN\nSELECT 1;\nEND DO;\n"}
}

func secretFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.Secret{Operation: ydbast.SecretCreate, Schema: "ext", Name: "pg.password", ValueEnv: "PTAH_SECRET_PG"},
		wantSQL: "CREATE SECRET `ext/pg.password` WITH (value = $PTAH_SECRET_PG);\n"}
}

func topicFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.Topic{Schema: "ext", Name: "events.v1", Change: ydbdiff.Topic{
		After: &ydbtopic.Desired{Spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "billing", Important: true}}}}}},
		wantSQL: "CREATE TOPIC `ext/events.v1` (CONSUMER `billing` WITH (important = TRUE));\n"}
}

func topicConsumerFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.TopicConsumer{Schema: "ext", Name: "events.v1", Consumer: ydbtopic.ConsumerSpec{Name: "audit"}},
		wantSQL: "ALTER TOPIC `ext/events.v1` ADD CONSUMER `audit`;\n"}
}

func hypertableFixture() extensionFixture {
	return extensionFixture{payload: &tsast.CreateHypertable{Table: "items", Hypertable: tsschema.DesiredHypertable{Column: "ts", ChunkInterval: "1 day"}},
		wantSQL: "SELECT create_hypertable('\"items\"', by_range('ts', INTERVAL '1 day'), create_default_indexes => FALSE);\n"}
}

func continuousAggregateFixture() extensionFixture {
	return extensionFixture{payload: &tsast.ContinuousAggregate{Schema: "app", Name: "hourly",
		Change: tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}}},
		wantSQL: "CREATE MATERIALIZED VIEW \"app\".\"hourly\" WITH (timescaledb.continuous) AS\nSELECT 1\nWITH NO DATA\n;\n"}
}

func allExtensionFixtures() []extensionFixture {
	return append(extensionFixtures(), clickhouseTTLFixture(), clickhouseIndexFixture(), clickhouseDropIndexFixture(), clickhouseRefreshFixture(), clickhouseRowPolicyFixture(), cockroachDBRowTTLFixture(), spannerRowDeletionFixture(), coordinationFixture(), streamingFixture(), poolFixture(), classifierFixture(), defaultPoolFixture(), secretFixture(), topicFixture(), topicConsumerFixture(),
		hypertableFixture(), continuousAggregateFixture(), policyFixture(), policyCommentFixture(), tableStateFixture())
}

func policyFixture() extensionFixture {
	using := "tenant_id = 1"
	return extensionFixture{payload: &pgpolicy.PolicyOperation{Schema: "app", Table: "orders", Name: "tenant", Change: pgpolicy.PolicyChange{
		After: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "reader"}, {Keyword: pgpolicy.CurrentUser}},
			Using: &using, Composition: pgpolicy.Restrictive},
		Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "a restrictive policy can hide rows from the roles it names"}}},
		wantSQL: "CREATE POLICY \"tenant\" ON \"app\".\"orders\" AS RESTRICTIVE FOR SELECT TO CURRENT_USER, \"reader\"\n    USING (tenant_id = 1)\n;\n"}
}

func policyCommentFixture() extensionFixture {
	return extensionFixture{payload: &pgpolicy.PolicyCommentOperation{Schema: "app", Table: "orders", Name: "tenant", Comment: "it's tenants"},
		wantSQL: "COMMENT ON POLICY \"tenant\" ON \"app\".\"orders\" IS 'it''s tenants';\n"}
}

func tableStateFixture() extensionFixture {
	return extensionFixture{payload: &pgpolicy.TableStateOperation{Schema: "app", Table: "orders", Change: pgpolicy.TableStateChange{
		Before: &pgpolicy.ObservedTableState{Forced: true}, After: &pgpolicy.DesiredTableState{Enabled: true},
		Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "enabling row security hides every row no policy admits"}}},
		wantSQL: "ALTER TABLE \"app\".\"orders\" ENABLE ROW LEVEL SECURITY;\nALTER TABLE \"app\".\"orders\" NO FORCE ROW LEVEL SECURITY;\n"}
}

func defaultPoolFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.DefaultPoolSettings{Spec: ydbworkload.PoolSpec{ResourceWeight: new(20.0)}},
		wantSQL: "ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 20);\n"}
}

// The source inventory is independent of both owner registration and fixtures.
// Moving a concrete node out of core cannot remove its routing evidence.
func TestExtensionPayloads_CoverSourceTypesAndHandlers(t *testing.T) {
	c := qt.New(t)
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	kinds, err := astrouteguard.ExtensionKinds(root)
	c.Assert(err, qt.IsNil)
	c.Assert(len(kinds) >= astrouteguard.ExtensionKindFloor, qt.IsTrue)
	var source []string
	for _, kind := range kinds {
		source = append(source, kind.Package+"."+kind.Name)
	}
	registry, err := ydbextensions.Registry()
	c.Assert(err, qt.IsNil)
	clickhouseRegistry, err := chrender.Registry()
	c.Assert(err, qt.IsNil)
	cockroachRegistry, err := crdbrender.Registry()
	c.Assert(err, qt.IsNil)
	timescaleRegistry, err := tsrender.Registry()
	c.Assert(err, qt.IsNil)
	spannerRegistry, err := spannerrender.Registry()
	c.Assert(err, qt.IsNil)
	policyRegistry, err := policyrender.Registry()
	c.Assert(err, qt.IsNil)
	var registered, fixtures []string
	for _, payloadType := range slices.Concat(registry.PayloadTypes(), clickhouseRegistry.PayloadTypes(), cockroachRegistry.PayloadTypes(), timescaleRegistry.PayloadTypes(),
		spannerRegistry.PayloadTypes(), policyRegistry.PayloadTypes()) {
		registered = append(registered, payloadType.Elem().PkgPath()+"."+payloadType.Elem().Name())
	}
	for _, fixture := range allExtensionFixtures() {
		payloadType := reflect.TypeOf(fixture.payload).Elem()
		fixtures = append(fixtures, payloadType.PkgPath()+"."+payloadType.Name())
	}
	slices.Sort(registered)
	slices.Sort(fixtures)
	c.Assert(registered, qt.DeepEquals, source)
	c.Assert(fixtures, qt.DeepEquals, source)
}

func TestExtensionPayloads_AllEntryPoints(t *testing.T) {
	for _, fixture := range allExtensionFixtures() {
		for _, dialect := range renderedDialects() {
			t.Run(dialect+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				fragment := &ast.ExtensionAlterOperation{Payload: fixture.payload}
				parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{fragment}}
				nodes := []ast.Node{fragment, parent, &ast.ExtensionStatement{Payload: fixture.payload}}
				for _, node := range nodes {
					visited := visitAnswer(c, dialect, node)
					c.Assert(renderAnswer(c, dialect, node), qt.DeepEquals, visited)
					c.Assert(renderSQLAnswer(dialect, node), qt.DeepEquals, visited)
				}
			})
		}
	}
}

func TestExtensionPayloads_OwnerRendersAndRequiresParent(t *testing.T) {
	for _, fixture := range extensionFixtures() {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			fragment := &ast.ExtensionAlterOperation{Payload: fixture.payload}
			parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{fragment}}
			for _, visitor := range []renderer.RenderVisitor{ydb.NewWithCapabilities(capability.YDB262()), mustYDBRenderer(c)} {
				sql, err := visitor.Render(parent)
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Equals, fixture.wantSQL)
				sql, err = visitor.Render(fragment)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(err, qt.ErrorMatches, `extension .* requires an ALTER TABLE parent`)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

func mustYDBRenderer(c *qt.C) renderer.RenderVisitor {
	c.Helper()
	r, err := builtin.NewRendererWithCapabilities("ydb", capability.YDB262())
	c.Assert(err, qt.IsNil)
	return r
}

func TestClickHouseExtensionOwnerRendersAndNonownersRefuse(t *testing.T) {
	for _, fixture := range []extensionFixture{clickhouseTTLFixture(), clickhouseIndexFixture()} {
		for _, dialect := range renderedDialects() {
			t.Run(dialect+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}}
				answer := renderSQLAnswer(dialect, parent)
				c.Assert(answer, qt.DeepEquals, visitAnswer(c, dialect, parent))
				c.Assert(answer, qt.DeepEquals, renderAnswer(c, dialect, parent))
			})
		}
		c := qt.New(t)
		sql, err := builtin.RenderSQL("clickhouse", &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}})
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, fixture.wantSQL)
		for _, dialect := range []string{"postgres", "mysql", "sqlite", "sqlserver", "oracle", "ydb"} {
			sql, err := builtin.RenderSQL(dialect, &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		}
	}
}

// TestCockroachDBExtensionOwnerRendersAndNonownersRefuse renders the row-level
// TTL operation on its owner and refuses it, without partial SQL, on every
// other target, the PostgreSQL-wire ones that share its renderer included.
func TestCockroachDBExtensionOwnerRendersAndNonownersRefuse(t *testing.T) {
	fixture := cockroachDBRowTTLFixture()
	parent := func() *ast.AlterTableNode {
		return &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}}
	}
	t.Run("owner", func(t *testing.T) {
		c := qt.New(t)
		sql, err := builtin.RenderSQL("cockroachdb", parent())
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, fixture.wantSQL)
	})
	for _, dialect := range []string{"postgres", "yugabytedb", "spanner", "mysql", "sqlite", "sqlserver", "oracle", "clickhouse", "ydb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQL(dialect, parent())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(fmt.Sprint(err), qt.Contains, string(crdbast.AlterRowTTLKind))
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestTimescaleExtensionOwnerRendersAndNonownersRefuse pins the composition of
// the TimescaleDB owner: every PostgreSQL-family target renders its payloads
// where the capability is present and writes the skip line where it is not,
// and every other target refuses them through the common boundary.
func TestTimescaleExtensionOwnerRendersAndNonownersRefuse(t *testing.T) {
	for _, fixture := range []extensionFixture{hypertableFixture(), continuousAggregateFixture()} {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			node := &ast.ExtensionStatement{Payload: fixture.payload}
			caps := capability.Postgres17().With(capability.Hypertables, true).With(capability.ContinuousAggregates, true)
			for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb", "spanner"} {
				sql, err := builtin.RenderSQLWithCapabilities(dialect, caps, node)
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Equals, fixture.wantSQL)
			}
			sql, err := builtin.RenderSQL("postgres", node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Matches, `-- POSTGRES: (hypertable items|continuous aggregate hourly) is not supported by this target; skipped.\n`)
			for _, dialect := range []string{"mysql", "mariadb", "sqlite", "sqlserver", "oracle", "clickhouse", "ydb"} {
				sql, err := builtin.RenderSQL(dialect, node)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// TestRowSecurityExtensionOwnerRendersAndNonownersRefuse pins the composition
// of the row-security owner: every PostgreSQL-family target renders its
// payloads where the capability is present and writes the skip line where it
// is not, and every other target refuses them through the common boundary.
// CockroachDB holds policies but not their comments, so the comment payload
// writes its own skip line there.
func TestRowSecurityExtensionOwnerRendersAndNonownersRefuse(t *testing.T) {
	for _, fixture := range []extensionFixture{policyFixture(), policyCommentFixture(), tableStateFixture()} {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			node := &ast.ExtensionStatement{Payload: fixture.payload}
			for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb", "spanner"} {
				sql, err := builtin.RenderSQLWithCapabilities(dialect, capability.Postgres17(), node)
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Equals, fixture.wantSQL)
			}
			sql, err := builtin.RenderSQL("spanner", node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Matches, `-- SPANNER: (policy tenant on app.orders|policy comment tenant on app.orders|row-level security on app.orders) is not supported by this target; skipped.\n`)
			for _, dialect := range []string{"mysql", "mariadb", "sqlite", "sqlserver", "oracle", "clickhouse", "ydb"} {
				sql, err := builtin.RenderSQL(dialect, node)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
	c := qt.New(t)
	sql, err := builtin.RenderSQL("cockroachdb", &ast.ExtensionStatement{Payload: policyCommentFixture().payload})
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "-- COCKROACHDB: policy comment tenant on app.orders is not supported by this target; skipped.\n")
}

// TestRowSecurityTableFacetsFollowTheirTable pins the lowering of a new table's
// declared switches: ENABLE and FORCE follow the CREATE TABLE, after a
// hypertable call the same table carries, and a table declaring both off writes
// neither.
func TestRowSecurityTableFacetsFollowTheirTable(t *testing.T) {
	hypertable := &tsschema.DesiredHypertable{Column: "id"}
	header := "-- POSTGRES TABLE: orders --\nCREATE TABLE \"orders\" (\n  \"id\" INTEGER\n);\n\n"
	tests := []struct {
		name   string
		facets []schemaext.Value
		want   string
	}{
		{name: "enabled and forced", facets: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true, Forced: true}},
			want: header + "ALTER TABLE \"orders\" ENABLE ROW LEVEL SECURITY;\nALTER TABLE \"orders\" FORCE ROW LEVEL SECURITY;\n"},
		{name: "after a hypertable", facets: []schemaext.Value{hypertable, &pgpolicy.DesiredTableState{Enabled: true}},
			want: header + "SELECT create_hypertable('\"orders\"', by_range('id'), create_default_indexes => FALSE);\nALTER TABLE \"orders\" ENABLE ROW LEVEL SECURITY;\n"},
		{name: "both off", facets: []schemaext.Value{&pgpolicy.DesiredTableState{}},
			want: header},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := ast.NewCreateTable("orders").AddColumn(ast.NewColumn("id", "INTEGER"))
			table.Facets = must.Must(schemaext.NewFacets(test.facets...))
			caps := capability.Postgres17().With(capability.Hypertables, true)

			sql, err := builtin.RenderSQLWithCapabilities("postgres", caps, table)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestSpannerExtensionOwnerRendersAndNonownersRefuse renders the row deletion
// policy operation on its owner and refuses it, without partial SQL, on every
// other target, the PostgreSQL-wire ones that share its renderer included.
func TestSpannerExtensionOwnerRendersAndNonownersRefuse(t *testing.T) {
	fixture := spannerRowDeletionFixture()
	parent := func() *ast.AlterTableNode {
		return &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}}
	}
	t.Run("owner", func(t *testing.T) {
		c := qt.New(t)
		sql, err := builtin.RenderSQL("spanner", parent())
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, fixture.wantSQL)
	})
	for _, dialect := range []string{"postgres", "yugabytedb", "cockroachdb", "mysql", "sqlite", "sqlserver", "oracle", "clickhouse", "ydb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQL(dialect, parent())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(fmt.Sprint(err), qt.Contains, string(spannerast.AlterRowDeletionKind))
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestExtensionPayloads_RefuseMalformedDrop(t *testing.T) {
	for _, name := range []string{"", " ", "table/feed"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			op := &ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: name}}
			for _, visitor := range []renderer.RenderVisitor{ydb.NewWithCapabilities(capability.YDB262()), mustYDBRenderer(c)} {
				for _, node := range []ast.Node{op, &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{op}}} {
					sql, err := visitor.Render(node)
					c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
					c.Assert(err, qt.ErrorMatches, "a changefeed needs a name without a slash")
					c.Assert(sql, qt.Equals, "")
				}
			}
		})
	}
}

func TestExtensionPayloads_NonownersRefuseClaimedCapabilities(t *testing.T) {
	caps := capability.YDB262()
	visitors := []renderer.RenderVisitor{
		clickhouse.NewWithCapabilities(caps), mssql.NewWithCapabilities(caps), mysql.NewWithCapabilities(caps),
		oracle.NewWithCapabilities(caps), postgres.NewWithCapabilities(caps, "postgres"), sqlite.NewWithCapabilities(caps),
	}
	for _, visitor := range visitors {
		for _, fixture := range extensionFixtures() {
			t.Run(visitor.Dialect()+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				for _, node := range []ast.Node{
					&ast.ExtensionStatement{Payload: fixture.payload},
					&ast.ExtensionAlterOperation{Payload: fixture.payload},
					&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("extra", "INTEGER")}, &ast.ExtensionAlterOperation{Payload: fixture.payload}}},
				} {
					sql, err := visitor.Render(node)
					c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
					c.Assert(fmt.Sprint(err), qt.Contains, string(fixture.payload.Kind()))
					c.Assert(sql, qt.Equals, "")
				}
			})
		}
	}
}

func TestClickHouseIndexExtensionRefusesMissingOperandsWithoutPartialSQL(t *testing.T) {
	for _, payload := range []ast.ExtensionPayload{(*chast.AddSkippingIndex)(nil), &chast.AddSkippingIndex{Name: "idx"}} {
		c := qt.New(t)
		parent := &ast.AlterTableNode{Name: "events", Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn("valid", "UInt64")},
			&ast.ExtensionAlterOperation{Payload: payload},
		}}
		for _, visitor := range []renderer.RenderVisitor{clickhouse.New(), mustClickHouseRenderer(c)} {
			sql, err := visitor.Render(parent)
			c.Assert(err, qt.IsNotNil)
			c.Assert(sql, qt.Equals, "")
			c.Assert(visitor.Output(), qt.Equals, "")
		}
	}
	c := qt.New(t)
	fragment := &ast.ExtensionAlterOperation{Payload: clickhouseIndexFixture().payload}
	for _, visitor := range []renderer.RenderVisitor{clickhouse.New(), mustClickHouseRenderer(c)} {
		sql, err := visitor.Render(fragment)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(sql, qt.Equals, "")
	}
}

func mustClickHouseRenderer(c *qt.C) renderer.RenderVisitor {
	c.Helper()
	r, err := builtin.NewRenderer("clickhouse")
	c.Assert(err, qt.IsNil)
	return r
}

func TestStreamingExtensionRendersAfterSelectedCodecRoundTrip(t *testing.T) {
	c := qt.New(t)
	fixture := streamingFixture()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{fixture.payload})
	c.Assert(err, qt.IsNil)
	values, err := runtime.Codecs().Unmarshal(c.Context(), data)
	c.Assert(err, qt.IsNil)
	node := &ast.ExtensionStatement{Payload: values[0].(ast.ExtensionPayload)}
	caps := capability.YDB262().With(capability.StreamingQueries, true)
	visitor, err := builtin.NewRendererWithCapabilities("ydb", caps)
	c.Assert(err, qt.IsNil)
	visited, err := visitor.Render(node)
	c.Assert(err, qt.IsNil)
	c.Assert(visited, qt.Equals, fixture.wantSQL)
	rendered, err := builtin.RenderSQLWithCapabilities("ydb", caps, node)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Equals, fixture.wantSQL)
	result, err := runtime.Render(c.Context(), renderer.Request{Target: "ydb", Capabilities: caps, Nodes: []ast.Node{node}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Fragments, qt.DeepEquals, []string{fixture.wantSQL})
}

func poolFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: "batch", Spec: &ydbworkload.PoolSpec{}}, wantSQL: "CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = \"-1\");\n"}
}

func classifierFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: "route", Spec: &ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 0}}, wantSQL: "CREATE RESOURCE POOL CLASSIFIER `route` WITH (RESOURCE_POOL = 'default', RANK = 0);\n"}
}

// TestRowSecurityPayloadsRenderAfterTheRuntimeRoundTrip drives each
// row-security operation through the builtin runtime's codecs and renderer, the
// path a saved plan takes, and each change through the runtime's change codec.
func TestRowSecurityPayloadsRenderAfterTheRuntimeRoundTrip(t *testing.T) {
	runtime := must.Must(builtin.New())
	for _, fixture := range []extensionFixture{policyFixture(), policyCommentFixture(), tableStateFixture()} {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{fixture.payload})
			c.Assert(err, qt.IsNil)
			values, err := runtime.Codecs().Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			node := &ast.ExtensionStatement{Payload: values[0].(ast.ExtensionPayload)}

			result, err := runtime.Render(c.Context(), renderer.Request{Target: "postgres", Capabilities: capability.Postgres17(), Nodes: []ast.Node{node}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.SQL(), qt.Equals, fixture.wantSQL)
		})
	}
	t.Run("changes", func(t *testing.T) {
		c := qt.New(t)
		changes := []schemaext.Payload{
			&policyFixture().payload.(*pgpolicy.PolicyOperation).Change,
			&tableStateFixture().payload.(*pgpolicy.TableStateOperation).Change,
		}
		data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Change, changes)
		c.Assert(err, qt.IsNil)
		values, err := runtime.Codecs().Unmarshal(c.Context(), data)
		c.Assert(err, qt.IsNil)
		c.Assert(values, qt.DeepEquals, changes)
	})
}

// TestRowSecurityPolicyTransitionsRender pins the statements of a removal and
// of a change: a change drops the policy and creates it again, because ALTER
// POLICY changes neither the command nor the composition.
func TestRowSecurityPolicyTransitionsRender(t *testing.T) {
	observed := &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Composition: pgpolicy.Permissive}
	declared := &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandInsert, WithCheck: new("owner_id = 7")}
	access := schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "r"}
	tests := []struct {
		name   string
		change pgpolicy.PolicyChange
		want   string
	}{
		{name: "a removal", change: pgpolicy.PolicyChange{Before: observed, Access: access},
			want: "DROP POLICY \"tenant\" ON \"orders\";\n"},
		{name: "a change", change: pgpolicy.PolicyChange{Before: observed, After: declared, Access: access},
			want: "DROP POLICY \"tenant\" ON \"orders\";\nCREATE POLICY \"tenant\" ON \"orders\" FOR INSERT\n    WITH CHECK (owner_id = 7)\n;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.ExtensionStatement{Payload: &pgpolicy.PolicyOperation{Table: "orders", Name: "tenant", Change: test.change}}

			sql, err := builtin.RenderSQL("postgres", node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
