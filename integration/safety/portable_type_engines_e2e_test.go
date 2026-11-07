//go:build integration

package safety_test

import (
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// sqliteScratch is a database in a file of its own.
func sqliteScratch(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	address := "sqlite://" + filepath.Join(c.TempDir(), "scratch.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn, address
}

// portableEngine is a server to compare a portable name on: how to reach a scratch database
// on it, and how to build table t.
type portableEngine struct {
	name    string
	scratch func(c *qt.C) (*dbschema.DatabaseConnection, string)
	table   string
}

func temporalPortable(name string, engine temporalEngine) portableEngine {
	return portableEngine{
		name:    name,
		scratch: func(c *qt.C) (*dbschema.DatabaseConnection, string) { return temporalScratch(c, engine) },
		table:   engine.table,
	}
}

var (
	sqlitePortable    = portableEngine{name: "sqlite", scratch: sqliteScratch, table: temporalTable}
	oraclePortable    = portableEngine{name: "oracle", scratch: oracleScratch, table: oracleTypeTable}
	cockroachPortable = temporalPortable("cockroachdb", cockroachTemporal)
	yugabytePortable  = temporalPortable("yugabytedb", yugabyteTemporal)
	mariadbPortable   = temporalPortable("mariadb", mariadbTemporal)
	mysqlPortable     = temporalPortable("mysql", mysqlTemporal)
	postgresPortable  = temporalPortable("postgres", postgresTemporal)
)

// replaceTable builds table t with column c of the type.
func replaceTable(c *qt.C, conn *dbschema.DatabaseConnection, engine portableEngine, columnType string) {
	c.Helper()
	_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), fmt.Sprintf(engine.table, columnType))
	c.Assert(err, qt.IsNil)
}

// TestSchemaPlanE2E_ANameAndTheColumnTheServerBuildsAreOneType builds each column on the
// server and declares the name. The catalog reads back another name than the one declared, so
// compared as words each row plans a statement that changes nothing, on every run.
func TestSchemaPlanE2E_ANameAndTheColumnTheServerBuildsAreOneType(t *testing.T) {
	engines := []struct {
		engine portableEngine
		pairs  [][2]string
	}{
		{cockroachPortable, [][2]string{
			{"bytes", "blob"}, {"blob", "blob"}, {"blob", "bytes"}, {"bytes[]", "blob[]"}, {"jsonb", "json"}, {"json", "json"}, {"json", "jsonb"},
		}},
		{yugabytePortable, [][2]string{{"serial", "serial"}, {"bigserial", "bigserial"}, {"smallserial", "smallserial"}}},
		{oraclePortable, [][2]string{{"BOOLEAN", "bool"}, {"BOOL", "BOOL"}, {"NUMBER(1)", "bool"}}},
		{mariadbPortable, [][2]string{{"json", "json"}, {"JSON", "json"}, {"longtext CHECK (json_valid(c))", "json"}}},
		{sqlitePortable, [][2]string{
			{"boolean", "boolean"}, {"BOOLEAN", "boolean"}, {"bool", "bool"}, {"serial", "serial"}, {"bigserial", "bigserial"},
			{"smallserial", "smallserial"}, {"bytea", "bytea"},
		}},
	}
	for _, engine := range engines {
		t.Run(engine.engine.name, func(t *testing.T) {
			conn, address := engine.engine.scratch(qt.New(t))
			for _, pair := range engine.pairs {
				t.Run(pair[0]+" as "+pair[1], func(t *testing.T) {
					c := qt.New(t)
					replaceTable(c, conn, engine.engine, pair[0])

					plan := planOf(c, address, schemaFile(c, fmt.Sprintf(engine.engine.table, pair[1])))

					c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
				})
			}
		})
	}
}

// TestSchemaPlanE2E_ANameMatchesTheColumnPtahBuildsFromIt applies each name to an empty
// database, reads what the server holds and plans the same declaration. wantHeld is the
// server's answer, so a row also fails if the server stops storing the name that way.
func TestSchemaPlanE2E_ANameMatchesTheColumnPtahBuildsFromIt(t *testing.T) {
	engines := []struct {
		engine portableEngine
		read   string
		rows   [][2]string
	}{
		{
			cockroachPortable,
			"SELECT crdb_sql_type FROM information_schema.columns WHERE table_name = 't' AND column_name = 'c'",
			[][2]string{{"blob", "BYTES"}, {"BLOB", "BYTES"}, {"json", "JSONB"}, {"JSON", "JSONB"}},
		},
		{
			yugabytePortable,
			"SELECT data_type || ' ' || is_nullable FROM information_schema.columns WHERE table_name = 't' AND column_name = 'c'",
			[][2]string{{"serial", "integer NO"}, {"bigserial", "bigint NO"}, {"smallserial", "smallint NO"}},
		},
		{
			mariadbPortable,
			`SELECT CONCAT(c.DATA_TYPE, ' ', k.CHECK_CLAUSE) FROM information_schema.COLUMNS c
				JOIN information_schema.CHECK_CONSTRAINTS k ON k.CONSTRAINT_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME AND k.CONSTRAINT_NAME = c.COLUMN_NAME
				WHERE c.TABLE_SCHEMA = DATABASE() AND c.TABLE_NAME = 't' AND c.COLUMN_NAME = 'c'`,
			[][2]string{{"json", "longtext json_valid(`c`)"}, {"JSON", "longtext json_valid(`c`)"}},
		},
	}
	for _, engine := range engines {
		t.Run(engine.engine.name, func(t *testing.T) {
			conn, address := engine.engine.scratch(qt.New(t))
			for _, row := range engine.rows {
				t.Run(row[0], func(t *testing.T) {
					c := qt.New(t)
					_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
					c.Assert(err, qt.IsNil)
					schema := schemaFile(c, fmt.Sprintf(engine.engine.table, row[0]))

					applySchema(c, address, schema)

					var held string
					c.Assert(conn.QueryRowContext(c.Context(), engine.read).Scan(&held), qt.IsNil)
					c.Assert(held, qt.Equals, row[1])
					plan := planOf(c, address, schema)
					c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
				})
			}
		})
	}
}

// portableChange is a column that is neither what the renderer writes for the declared name
// nor what the server builds from it, so a plan has work to do, and its rating.
type portableChange struct {
	built, declared string
	wantDestructive bool
}

// TestSchemaPlanE2E_AnotherTypeUnderANameStillPlans holds the guard the other way, with the
// engines where the two names of a folded pair are two types. The rating is the one any type
// change gets, read off the plan.
func TestSchemaPlanE2E_AnotherTypeUnderANameStillPlans(t *testing.T) {
	engines := []struct {
		engine portableEngine
		rows   []portableChange
	}{
		{postgresPortable, []portableChange{{"bigint", "integer", true}, {"json", "jsonb", false}, {"jsonb", "json", false}}},
		{cockroachPortable, []portableChange{{"text", "blob", false}, {"text", "json", false}, {"jsonb", "text", false}, {"bytes", "text", false}}},
		{yugabytePortable, []portableChange{{"int", "serial", false}, {"serial", "int", true}}},
		{oraclePortable, []portableChange{{"NUMBER(5)", "bool", true}}},
		{mariadbPortable, []portableChange{
			{"longtext", "json", false}, {"text", "json", false}, {"varchar(10)", "json", false}, {"json", "longtext", true}, {"json", "text", true},
		}},
		{mysqlPortable, []portableChange{{"longtext", "json", false}, {"json", "longtext", false}}},
		{sqlitePortable, []portableChange{
			{"boolean", "integer", true}, {"text", "boolean", true}, {"boolean", "text", true}, {"bytea", "blob", true}, {"serial", "integer", true},
		}},
	}
	for _, engine := range engines {
		t.Run(engine.engine.name, func(t *testing.T) {
			conn, address := engine.engine.scratch(qt.New(t))
			for _, row := range engine.rows {
				t.Run(row.built+" as "+row.declared, func(t *testing.T) {
					c := qt.New(t)
					replaceTable(c, conn, engine.engine, row.built)

					plan := planOf(c, address, schemaFile(c, fmt.Sprintf(engine.engine.table, row.declared)))

					c.Assert(plan.Outcome, qt.Equals, "changes")
					c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
				})
			}
		})
	}
}

// TestSchemaPlanE2E_APlanUnderANameConverges applies the plan of each row, where the server
// runs it, and plans again. MariaDB's rows are the ones a comparison of the CHECK alone gets
// wrong: a statement that rewrites the column writes the CHECK with it, and a second statement
// adding the same CHECK is refused.
func TestSchemaPlanE2E_APlanUnderANameConverges(t *testing.T) {
	engines := []struct {
		engine portableEngine
		pairs  [][2]string
	}{
		{mariadbPortable, [][2]string{{"longtext", "json"}, {"text", "json"}, {"varchar(10)", "json"}, {"json", "json NOT NULL"}}},
		{oraclePortable, [][2]string{{"NUMBER(5)", "bool"}}},
		{sqlitePortable, [][2]string{{"text", "boolean"}, {"boolean", "text"}, {"bytea", "blob"}}},
	}
	for _, engine := range engines {
		t.Run(engine.engine.name, func(t *testing.T) {
			conn, address := engine.engine.scratch(qt.New(t))
			for _, pair := range engine.pairs {
				t.Run(pair[0]+" as "+pair[1], func(t *testing.T) {
					c := qt.New(t)
					replaceTable(c, conn, engine.engine, pair[0])
					schema := schemaFile(c, fmt.Sprintf(engine.engine.table, pair[1]))
					c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "changes")

					applySchema(c, address, schema)

					plan := planOf(c, address, schema)
					c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
				})
			}
		})
	}
}
