//go:build integration

package ydb_test

import (
	"context"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The directories the round trip writes into. A Ptah schema is a directory on
// YDB, so every table the tests create sits under one of these and a read
// scoped to them sees nothing another test or a previous run left elsewhere in
// the database.
const (
	roundTripSchema        = "ptah_ydb_roundtrip"
	roundTripArchiveSchema = "ptah_ydb_roundtrip/archive"
)

var roundTripSchemas = []string{roundTripSchema, roundTripArchiveSchema}

// TestYDBRoundTrip_NothingLeftToPlan is the round trip: a representative
// schema rendered for YDB, applied statement by statement, read back through
// the reader and compared with the declaration plans nothing, and the same
// declaration applied again plans nothing too.
//
// The schema carries every type the YDB type map writes on the line CI runs, a
// literal default of each kind the renderer writes, a Serial key, a composite
// key, unique, asynchronous and covering indexes, a table in a nested
// directory, and a table and a column whose names need escaping.
func TestYDBRoundTrip_NothingLeftToPlan(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	dropTables(c, conn, roundTripSchemas)
	c.Cleanup(func() { dropTables(c, conn, roundTripSchemas) })

	declared := roundTripDeclaration()
	first := planAgainst(c, conn, declared, roundTripSchemas)
	c.Assert(first, qt.Not(qt.HasLen), 0)
	apply(c, conn, first)

	c.Assert(planAgainst(c, conn, declared, roundTripSchemas), qt.HasLen, 0)
	// The same declaration applied a second time changes nothing and plans
	// nothing after it.
	apply(c, conn, planAgainst(c, conn, declared, roundTripSchemas))
	c.Assert(planAgainst(c, conn, declared, roundTripSchemas), qt.HasLen, 0)
}

// TestYDBRoundTrip_ReadsWhatTheServerBuilt pins what the reader reports for
// the round-trip schema, read from the server rather than restated from the
// declaration: the Serial key, the composite key, the index kinds, the
// defaults in the spelling the renderer writes, and the escaped names.
func TestYDBRoundTrip_ReadsWhatTheServerBuilt(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	dropTables(c, conn, roundTripSchemas)
	c.Cleanup(func() { dropTables(c, conn, roundTripSchemas) })
	apply(c, conn, planAgainst(c, conn, roundTripDeclaration(), roundTripSchemas))

	live := readScoped(c, conn, roundTripSchemas)

	c.Assert(tableNames(live), qt.DeepEquals, []string{
		"ptah_ydb_roundtrip/archive|events",
		"ptah_ydb_roundtrip|accounts",
		"ptah_ydb_roundtrip|orders",
		"ptah_ydb_roundtrip|tick`name.with.dot",
	})

	accounts := tableNamed(c, live, roundTripSchema, "accounts")
	id := columnNamed(c, accounts, "id")
	c.Assert(id.DataType, qt.Equals, "Int64")
	c.Assert(id.IsAutoIncrement, qt.IsTrue)
	c.Assert(id.IsNullable, qt.Equals, "NO")
	c.Assert(defaultOf(columnNamed(c, accounts, "status")), qt.Equals, "'active'u")
	c.Assert(defaultOf(columnNamed(c, accounts, "created_at")), qt.Equals, "Timestamp64('2026-01-02T03:04:05Z')")
	c.Assert(defaultOf(columnNamed(c, accounts, "lifetime")), qt.Equals, "Interval64('P1DT2H')")
	c.Assert(defaultOf(columnNamed(c, accounts, "balance")), qt.Equals, "Decimal('12.5', 10, 2)")
	c.Assert(defaultOf(columnNamed(c, accounts, "token")), qt.Equals,
		"Uuid('550e8400-e29b-41d4-a716-446655440000')")
	c.Assert(columnNamed(c, accounts, "email").DataType, qt.Equals, "Utf8")
	c.Assert(columnNamed(c, accounts, "birthday").DataType, qt.Equals, "Date32")

	c.Assert(primaryKeyOf(live, roundTripSchema, "orders"), qt.DeepEquals, []string{"tenant", "id"})

	email := indexNamed(c, live, "uq_accounts_email")
	c.Assert(email.IsUnique, qt.IsTrue)
	c.Assert(email.Method, qt.Equals, "GLOBAL SYNC")
	status := indexNamed(c, live, "idx_accounts_status")
	c.Assert(status.Method, qt.Equals, "GLOBAL ASYNC")
	created := indexNamed(c, live, "idx_accounts_created")
	c.Assert(created.Columns, qt.DeepEquals, []string{"created_at"})
	c.Assert(created.IncludeColumns, qt.DeepEquals, []string{"status", "visits"})

	tick := tableNamed(c, live, roundTripSchema, "tick`name.with.dot")
	c.Assert(columnNamed(c, tick, "weird-col").DataType, qt.Equals, "Utf8")
}

// roundTripDeclaration is the representative schema.
func roundTripDeclaration() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Account", Name: "accounts", Schema: roundTripSchema},
			{StructName: "Order", Name: "orders", Schema: roundTripSchema},
			{StructName: "Event", Name: "events", Schema: roundTripArchiveSchema},
			{StructName: "Tick", Name: "tick`name.with.dot", Schema: roundTripSchema},
		},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "BIGINT", Primary: true, AutoInc: true},
			{StructName: "Account", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "Account", Name: "status", Type: "VARCHAR(20)", Nullable: true, Default: "active"},
			{StructName: "Account", Name: "visits", Type: "INTEGER", Nullable: true, Default: "0"},
			{StructName: "Account", Name: "verified", Type: "BOOLEAN", Nullable: true, Default: "false"},
			{StructName: "Account", Name: "created_at", Type: "TIMESTAMP", Nullable: true, Default: "2026-01-02 03:04:05"},
			{StructName: "Account", Name: "birthday", Type: "DATE", Nullable: true, Default: "2000-02-29"},
			{StructName: "Account", Name: "lifetime", Type: "INTERVAL", Nullable: true, Default: "PT26H"},
			{StructName: "Account", Name: "balance", Type: "DECIMAL(10,2)", Nullable: true, Default: "12.50"},
			{StructName: "Account", Name: "ratio", Type: "REAL", Nullable: true, Default: "0.10"},
			{StructName: "Account", Name: "score", Type: "DOUBLE PRECISION", Nullable: true, Default: "1e21"},
			{StructName: "Account", Name: "profile", Type: "JSONB", Nullable: true, Default: `{"a":1}`},
			{StructName: "Account", Name: "raw_json", Type: "JSON", Nullable: true},
			{StructName: "Account", Name: "avatar", Type: "BYTEA", Nullable: true, Default: "raw"},
			{StructName: "Account", Name: "token", Type: "UUID", Nullable: true,
				Default: "550E8400-E29B-41D4-A716-446655440000"},
			{StructName: "Account", Name: "tiny", Type: "TINYINT", Nullable: true, Default: "-5"},
			{StructName: "Account", Name: "small", Type: "SMALLINT", Nullable: true, Default: "5"},
			{StructName: "Account", Name: "u8", Type: "TINYINT UNSIGNED", Nullable: true, Default: "200"},
			{StructName: "Account", Name: "u16", Type: "SMALLINT UNSIGNED", Nullable: true},
			{StructName: "Account", Name: "u32", Type: "INT UNSIGNED", Nullable: true, Default: "7"},
			{StructName: "Account", Name: "u64", Type: "BIGINT UNSIGNED", Nullable: true,
				Default: "18446744073709551615"},
			{StructName: "Account", Name: "dy", Type: "DyNumber", Nullable: true, Default: "3.14"},
			{StructName: "Account", Name: "doc", Type: "Yson", Nullable: true},
			{StructName: "Account", Name: "n_date", Type: "Date", Nullable: true, Default: "2026-01-02"},
			{StructName: "Account", Name: "n_datetime", Type: "Datetime", Nullable: true},
			{StructName: "Account", Name: "n_ts", Type: "Timestamp", Nullable: true,
				Default: "2026-01-02T03:04:05.123456Z"},
			{StructName: "Account", Name: "n_iv", Type: "Interval", Nullable: true, Default: "P1D"},
			{StructName: "Account", Name: "w_dt64", Type: "Datetime64", Nullable: true,
				Default: "1900-01-02T03:04:05Z"},

			{StructName: "Order", Name: "tenant", Type: "TEXT", Primary: true},
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "amount", Type: "DECIMAL(22,9)", Nullable: true, Default: "1.5"},
			{StructName: "Order", Name: "note", Type: "TEXT", Nullable: true},

			{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Event", Name: "payload", Type: "TEXT", Nullable: true},

			{StructName: "Tick", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Tick", Name: "weird-col", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Account", Name: "uq_accounts_email", Fields: []string{"email"}, Unique: true},
			{StructName: "Account", Name: "idx_accounts_status", Fields: []string{"status"}, Type: "async"},
			{StructName: "Account", Name: "idx_accounts_created", Fields: []string{"created_at"},
				IncludeColumns: []string{"status", "visits"}},
			{StructName: "Order", Name: "idx_orders_amount", Fields: []string{"amount"}},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// openYDB connects to the YDB database the run names.
func openYDB(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	url := dbtarget.URL(c, dbtarget.YDB)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// readScoped reads the directories a test owns.
func readScoped(c *qt.C, conn *dbschema.DatabaseConnection, schemas []string) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, schemas)
	c.Assert(err, qt.IsNil)
	return live
}

// planAgainst plans the statements that take the directories a test owns to
// the declaration.
func planAgainst(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
	schemas []string,
) []string {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readScoped(c, conn, schemas), info, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// apply runs each planned statement through the writer.
func apply(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// dropTables drops every table in the directories a test owns. The
// directories are left: YDB keeps a directory after its last table goes, and
// an empty one is invisible to a scoped read.
func dropTables(c *qt.C, conn *dbschema.DatabaseConnection, schemas []string) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(context.Background(), conn, schemas)
	c.Assert(err, qt.IsNil)
	for _, table := range live.Tables {
		path := table.Name
		if table.Schema != "" {
			path = table.Schema + "/" + table.Name
		}
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE "+sqlident.Quote("ydb", path)), qt.IsNil)
	}
}

func tableNames(live *catalog.Database) []string {
	names := make([]string, 0, len(live.Tables))
	for _, table := range live.Tables {
		names = append(names, table.Schema+"|"+table.Name)
	}
	slices.Sort(names)
	return names
}

func tableNamed(c *qt.C, live *catalog.Database, schema, name string) catalog.Table {
	c.Helper()
	index := slices.IndexFunc(live.Tables, func(table catalog.Table) bool {
		return table.Schema == schema && table.Name == name
	})
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("table %s/%s is not in the read", schema, name))
	return live.Tables[index]
}

func columnNamed(c *qt.C, table catalog.Table, name string) catalog.Column {
	c.Helper()
	index := slices.IndexFunc(table.Columns, func(column catalog.Column) bool { return column.Name == name })
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("column %s is not in table %s", name, table.Name))
	return table.Columns[index]
}

func defaultOf(column catalog.Column) string {
	if column.ColumnDefault == nil {
		return ""
	}
	return *column.ColumnDefault
}

func indexNamed(c *qt.C, live *catalog.Database, name string) catalog.Index {
	c.Helper()
	index := slices.IndexFunc(live.Indexes, func(index catalog.Index) bool { return index.Name == name })
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("index %s is not in the read", name))
	return live.Indexes[index]
}

func primaryKeyOf(live *catalog.Database, schema, table string) []string {
	for _, constraint := range live.Constraints {
		if constraint.Type == "PRIMARY KEY" && constraint.Schema == schema && constraint.TableName == table {
			return constraint.ColumnNames
		}
	}
	return nil
}

func columnNamesOf(table catalog.Table) []string {
	names := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		names = append(names, column.Name)
	}
	return names
}

func indexNamesOf(live *catalog.Database) []string {
	names := make([]string, 0, len(live.Indexes))
	for _, index := range live.Indexes {
		names = append(names, index.Name)
	}
	slices.Sort(names)
	return names
}
