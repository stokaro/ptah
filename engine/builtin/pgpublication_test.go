package builtin_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// These tests read the desired publications from SQL, build the observed ones
// as the PostgreSQL reader reports them, and drive both through the bundled
// runtime the commands use.

// declaredPublications reads a desired SQL document.
func declaredPublications(c *qt.C, sql string) *schemamodel.Database {
	c.Helper()
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), platform.Postgres, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	return &db
}

// observedPublications is a database holding publications and nothing else,
// read completely.
func observedPublications(publications map[string]pgpublication.ObservedPublication) *catalog.Database {
	var objects []schemaext.Object
	for name, publication := range publications {
		if publication.Publish == nil {
			publication.Publish = pgpublication.PublishOperations()
		}
		objects = append(objects, must.Must(pgpublication.ObservedObject(pgpublication.Ref(name), publication)))
	}
	return &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgpublication.CompleteCoverage(schemaext.Observed))}
}

// planPublications plans desired against current on PostgreSQL with caps.
func planPublications(c *qt.C, desired *schemamodel.Database, current *catalog.Database, caps capability.Capabilities) (string, error) {
	c.Helper()
	runtime := must.Must(builtin.New())
	info := catalog.ServerInfo{Dialect: platform.Postgres, Capabilities: caps}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, info, nil, runtime)
	if err != nil {
		return "", err
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Postgres, planext.Options{Capabilities: caps})
	return strings.Join(statements, "\n"), err
}

func publicationStatements(script string) []string {
	var statements []string
	for line := range strings.SplitSeq(script, "\n") {
		if strings.Contains(line, "PUBLICATION") && !strings.HasPrefix(strings.TrimSpace(line), "--") {
			statements = append(statements, line)
		}
	}
	return statements
}

var orders = pgpublication.Member{Schema: "public", Table: "orders"}

// TestPublicationPlan_HappyPath pins the statements each change plans: a
// member added or dropped by itself, options set in place, a change of
// membership form rebuilt, a managed owner set, an unmanaged one left alone,
// and a publication the desired schema leaves out dropped only when it
// declares others.
func TestPublicationPlan_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current map[string]pgpublication.ObservedPublication
		caps    capability.Capabilities
		want    []string
	}{
		{name: "create on two tables", desired: `CREATE PUBLICATION "Orders Feed" FOR TABLE orders, audit.events`,
			caps: capability.Postgres16(),
			want: []string{`CREATE PUBLICATION "Orders Feed" FOR TABLE ONLY "audit"."events", ONLY "public"."orders"`}},
		{name: "a zero diff", desired: `CREATE PUBLICATION feed FOR TABLE orders`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{orders}, Owner: "postgres"}},
			caps:    capability.Postgres16()},
		{name: "a member added and one dropped", desired: `CREATE PUBLICATION feed FOR TABLE orders, items`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{orders, {Schema: "public", Table: "refunds"}}}},
			caps:    capability.Postgres16(),
			want:    []string{`ALTER PUBLICATION "feed" ADD TABLE ONLY "public"."items"`, `ALTER PUBLICATION "feed" DROP TABLE ONLY "public"."refunds"`}},
		{name: "a schema added", desired: `CREATE PUBLICATION feed FOR TABLE orders, TABLES IN SCHEMA audit`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{orders}}},
			caps:    capability.Postgres16(),
			want:    []string{`ALTER PUBLICATION "feed" ADD TABLES IN SCHEMA "audit"`}},
		{name: "options set in place", desired: `CREATE PUBLICATION feed FOR TABLE orders WITH (publish = 'insert', publish_via_partition_root = true)`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{orders}}},
			caps:    capability.Postgres16(),
			want:    []string{`ALTER PUBLICATION "feed" SET (publish = 'insert', publish_via_partition_root = true)`}},
		{name: "generated columns on PostgreSQL 18", desired: `CREATE PUBLICATION feed FOR TABLE orders WITH (publish_generated_columns = stored)`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{orders}, GeneratedColumns: "none"}},
			caps:    capability.Postgres18(),
			want: []string{`ALTER PUBLICATION "feed" SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = false, ` +
				`publish_generated_columns = stored)`}},
		{name: "a form change rebuilt", desired: `CREATE PUBLICATION feed FOR ALL TABLES`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{orders}}},
			caps:    capability.Postgres16(),
			want:    []string{`DROP PUBLICATION "feed"`, `CREATE PUBLICATION "feed" FOR ALL TABLES`}},
		{name: "a managed owner set", desired: "CREATE PUBLICATION feed;\nALTER PUBLICATION feed OWNER TO replicator;",
			current: map[string]pgpublication.ObservedPublication{"feed": {Owner: "postgres"}},
			caps:    capability.Postgres16(),
			want:    []string{`ALTER PUBLICATION "feed" OWNER TO "replicator"`}},
		{name: "an unmanaged owner left alone", desired: "CREATE PUBLICATION feed;",
			current: map[string]pgpublication.ObservedPublication{"feed": {Owner: "someone"}},
			caps:    capability.Postgres16()},
		{name: "one left out dropped", desired: "CREATE PUBLICATION feed;",
			current: map[string]pgpublication.ObservedPublication{"feed": {}, "old": {}},
			caps:    capability.Postgres16(),
			want:    []string{`DROP PUBLICATION IF EXISTS "old"`}},
		{name: "none declared drops none", desired: "CREATE TABLE orders (id int);",
			current: map[string]pgpublication.ObservedPublication{"dbz_publication": {AllTables: true}},
			caps:    capability.Postgres16()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			script, err := planPublications(c, declaredPublications(c, test.desired), observedPublications(test.current), test.caps)
			c.Assert(err, qt.IsNil)
			c.Assert(publicationStatements(script), qt.DeepEquals, test.want, qt.Commentf("%s", script))
		})
	}
}

// TestPublicationPlan_FailurePath pins the changes refused rather than
// widened: a setting the target lacks, a row filter or column list Ptah
// would have to write, and an owner no declaration can name.
func TestPublicationPlan_FailurePath(t *testing.T) {
	filtered := pgpublication.Member{Schema: "public", Table: "orders", RowFilter: "(id > 0)"}
	tests := []struct {
		name    string
		desired string
		current map[string]pgpublication.ObservedPublication
		caps    capability.Capabilities
		wantErr string
	}{
		{name: "generated columns before PostgreSQL 18", desired: `CREATE PUBLICATION feed WITH (publish_generated_columns = stored);`,
			caps: capability.Postgres17(), wantErr: `(?s).*publish_generated_columns = stored, which requires target capability publication.generated_columns.*`},
		{name: "a schema before PostgreSQL 15", desired: `CREATE PUBLICATION feed FOR TABLES IN SCHEMA audit;`,
			caps: capability.Postgres14(), wantErr: `(?s).*publishes every table of schema "audit", which requires target capability publication.schemas.*`},
		{name: "a filtered member kept", desired: `CREATE PUBLICATION feed FOR TABLE orders;`,
			current: map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{filtered}}},
			caps:    capability.Postgres16(), wantErr: `(?s).*publishes table public.orders with another row filter or column list than the desired schema declares.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			script, err := planPublications(c, declaredPublications(c, test.desired), observedPublications(test.current), test.caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(script, qt.Equals, "")
		})
	}
}

// TestPublicationPlan_FailurePathRefusesADeclaredFilterInPlace adds a member
// with a row filter to a publication the server holds. Neither source reads a
// filter into a declaration, so the declaration is built directly, as an
// embedder may build one. Adding the table in place would publish every row of
// it, so the plan is refused. The comparison validates each declaration as a
// creation first, which refuses it before the in-place planner sees it; the
// planner refuses the in-place change too, for a caller that plans one directly.
func TestPublicationPlan_FailurePathRefusesADeclaredFilterInPlace(t *testing.T) {
	c := qt.New(t)
	items := pgpublication.Member{Schema: "public", Table: "items"}
	declared := pgpublication.DesiredPublication{Tables: []pgpublication.Member{items, {Schema: "public", Table: "orders", RowFilter: "(id > 0)"}}}
	desired := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgpublication.DesiredObject(pgpublication.Ref("feed"), declared)))),
		FeatureCoverage: must.Must(pgpublication.CompleteCoverage(schemaext.Desired)),
	}
	current := observedPublications(map[string]pgpublication.ObservedPublication{"feed": {Tables: []pgpublication.Member{items}}})

	script, err := planPublications(c, desired, current, capability.Postgres16())

	c.Assert(err, qt.ErrorMatches,
		`(?s).*publication "feed" publishes table public.orders with a row filter; Ptah does not write a publication's row filters.*`)
	c.Assert(script, qt.Equals, "")
}

// orderedStatements keeps the table and publication statements of script, in
// the order they run.
func orderedStatements(script string) []string {
	var statements []string
	for line := range strings.SplitSeq(script, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "--") {
			continue
		}
		if strings.HasPrefix(line, "CREATE TABLE") || strings.HasPrefix(line, "DROP TABLE") || strings.Contains(line, "PUBLICATION") {
			statements = append(statements, line)
		}
	}
	return statements
}

// publishedTable is a table of the current database with a single integer
// column, as the PostgreSQL reader reports one.
func publishedTable(name string) catalog.Table {
	return catalog.Table{Schema: "public", Name: name, Columns: []catalog.Column{{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO",
		IsPrimaryKey: true}}}
}

// TestPublicationPlan_OrdersAroundTables pins the order of a publication's
// statements against its tables: a table is created before the publication
// that names it, and a member leaves its publication before its table is
// dropped. The server refuses a publication of a table it does not hold, and
// the other order would drop a table a publication still names.
func TestPublicationPlan_OrdersAroundTables(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current *catalog.Database
		want    []string
	}{
		{name: "a table created before its publication",
			desired: "CREATE TABLE orders (id integer PRIMARY KEY);\nCREATE PUBLICATION feed FOR TABLE orders;",
			current: observedPublications(nil),
			want:    []string{`CREATE TABLE "orders" (`, `CREATE PUBLICATION "feed" FOR TABLE ONLY "public"."orders"`}},
		{name: "a member dropped before its table",
			desired: "CREATE TABLE orders (id integer PRIMARY KEY);\nCREATE PUBLICATION feed FOR TABLE orders;",
			current: func() *catalog.Database {
				db := observedPublications(map[string]pgpublication.ObservedPublication{
					"feed": {Tables: []pgpublication.Member{orders, {Schema: "public", Table: "items"}}}})
				db.Tables = []catalog.Table{publishedTable("orders"), publishedTable("items")}
				complete := schemaext.Knowledge{State: schemaext.Complete}
				db.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Observed, slices.Concat(db.FeatureCoverage.KindRecords(),
					must.Must(pgtable.PartitionCoverage(schemaext.Observed, complete, nil)).KindRecords(),
					must.Must(pgtable.PersistenceCoverage(schemaext.Observed, complete, nil)).KindRecords()), nil))
				return db
			}(),
			want: []string{`ALTER PUBLICATION "feed" DROP TABLE ONLY "public"."items"`, `DROP TABLE IF EXISTS "public"."items" CASCADE`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			script, err := planPublications(c, declaredPublications(c, test.desired), test.current, capability.Postgres16())
			c.Assert(err, qt.IsNil)
			c.Assert(orderedStatements(script), qt.DeepEquals, test.want, qt.Commentf("%s", script))
		})
	}
}

// TestPublicationRender_HappyPath pins the statement each action renders on
// PostgreSQL, every name quoted and every table written with ONLY.
func TestPublicationRender_HappyPath(t *testing.T) {
	members := []pgpublication.Member{{Schema: "public", Table: "orders"}, {Schema: "Sales", Table: "Line Items"}}
	tests := []struct {
		name      string
		operation pgpublication.Operation
		want      string
	}{
		{name: "empty", operation: pgpublication.Operation{Action: pgpublication.Create},
			want: `CREATE PUBLICATION "Feed";`},
		{name: "all tables", operation: pgpublication.Operation{Action: pgpublication.Create, AllTables: true, ViaPartitionRoot: true},
			want: `CREATE PUBLICATION "Feed" FOR ALL TABLES WITH (publish_via_partition_root = true);`},
		{name: "tables and schemas", operation: pgpublication.Operation{Action: pgpublication.Create, Tables: members, Schemas: []string{"audit"},
			Publish: make([]string, 0), GeneratedColumns: pgpublication.GeneratedColumnsStored},
			want: `CREATE PUBLICATION "Feed" FOR TABLE ONLY "public"."orders", ONLY "Sales"."Line Items", TABLES IN SCHEMA "audit" ` +
				`WITH (publish = '', publish_generated_columns = stored);`},
		{name: "add", operation: pgpublication.Operation{Action: pgpublication.AddMembers, Tables: members[1:]},
			want: `ALTER PUBLICATION "Feed" ADD TABLE ONLY "Sales"."Line Items";`},
		{name: "drop members", operation: pgpublication.Operation{Action: pgpublication.DropMembers, Schemas: []string{"audit"}},
			want: `ALTER PUBLICATION "Feed" DROP TABLES IN SCHEMA "audit";`},
		{name: "set members", operation: pgpublication.Operation{Action: pgpublication.SetMembers, Tables: members[:1]},
			want: `ALTER PUBLICATION "Feed" SET TABLE ONLY "public"."orders";`},
		{name: "options", operation: pgpublication.Operation{Action: pgpublication.SetOptions, Publish: []string{"insert", "delete"}},
			want: `ALTER PUBLICATION "Feed" SET (publish = 'insert, delete', publish_via_partition_root = false);`},
		{name: "owner", operation: pgpublication.Operation{Action: pgpublication.SetOwner, Owner: "Replicator"},
			want: `ALTER PUBLICATION "Feed" OWNER TO "Replicator";`},
		{name: "drop", operation: pgpublication.Operation{Action: pgpublication.Drop, IfExists: true},
			want: `DROP PUBLICATION IF EXISTS "Feed";`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operation := test.operation
			operation.Name = "Feed"
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), &ast.ExtensionStatement{Payload: &operation})
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want+"\n")
		})
	}
}

// TestPublicationRender_FailurePath pins the refusals: a clause the release
// line lacks, and every target but PostgreSQL, YugabyteDB included, since
// Ptah models no publication there.
func TestPublicationRender_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		caps      capability.Capabilities
		operation pgpublication.Operation
		wantErr   string
	}{
		{name: "generated columns on PostgreSQL 17", dialect: platform.Postgres, caps: capability.Postgres17(),
			operation: pgpublication.Operation{Action: pgpublication.SetOptions, Publish: pgpublication.PublishOperations(),
				GeneratedColumns: pgpublication.GeneratedColumnsNone},
			wantErr: `.*publish_generated_columns = none, which requires target capability publication.generated_columns.*`},
		{name: "a schema on PostgreSQL 14", dialect: platform.Postgres, caps: capability.Postgres14(),
			operation: pgpublication.Operation{Action: pgpublication.AddMembers, Schemas: []string{"audit"}},
			wantErr:   `.*publishes every table of schema "audit", which requires target capability publication.schemas.*`},
		{name: "YugabyteDB", dialect: platform.YugabyteDB, caps: capability.YugabyteDB25(),
			operation: pgpublication.Operation{Action: pgpublication.Create}, wantErr: `.*`},
		{name: "MySQL", dialect: platform.MySQL, caps: capability.ForDialect(platform.MySQL),
			operation: pgpublication.Operation{Action: pgpublication.Create}, wantErr: `.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operation := test.operation
			operation.Name = "Feed"
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, &ast.ExtensionStatement{Payload: &operation})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestPublicationDeclarations_HappyPath pins where a whole-schema render
// creates a publication: after the schema a publication of every table in a
// schema names, and after the tables a publication lists, so the script runs
// on an empty database. The render is deterministic, so the whole order is
// pinned.
func TestPublicationDeclarations_HappyPath(t *testing.T) {
	c := qt.New(t)
	declared := declaredPublications(c, `CREATE PUBLICATION by_schema FOR TABLES IN SCHEMA audit;
CREATE PUBLICATION by_table FOR TABLE audit.events;
CREATE SCHEMA audit;
CREATE TABLE audit.events (id int);
`)

	statements, err := builtin.GetOrderedCreateStatements(declared, platform.Postgres)

	c.Assert(err, qt.IsNil)
	var heads []string
	for _, statement := range statements {
		heads = append(heads, strings.TrimSpace(strings.SplitN(statement, " (", 2)[0]))
	}
	c.Assert(heads, qt.DeepEquals, []string{
		`CREATE SCHEMA IF NOT EXISTS "audit";`,
		`CREATE PUBLICATION "by_schema" FOR TABLES IN SCHEMA "audit";`,
		`CREATE TABLE "audit"."events"`,
		`CREATE PUBLICATION "by_table" FOR TABLE ONLY "audit"."events";`,
	}, qt.Commentf("%s", strings.Join(statements, "\n")))
}
