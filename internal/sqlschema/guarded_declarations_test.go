package sqlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// readGuarded reads sql as PostgreSQL with no owners.
func readGuarded(c *qt.C, sql string) schemamodel.Database {
	c.Helper()
	database, _, err := sqlschema.ReadWith(context.Background(), []byte(sql), platform.Postgres, nil, sqlschema.Options{Owners: sqlext.None()})
	c.Assert(err, qt.IsNil)
	return database
}

// columnsOfTable is the names of the columns of the table structName joins.
func columnsOfTable(database schemamodel.Database, structName string) []string {
	var columns []string
	for _, field := range database.Fields {
		if field.StructName == structName {
			columns = append(columns, field.Name)
		}
	}
	return columns
}

// TestReadGuarded_IfNotExistsSkipsTheStatement holds that a CREATE ... IF NOT
// EXISTS of a table or sequence already declared adds nothing of its own, as
// PostgreSQL 15.19 skips it whole: the first table keeps its one column and
// no key, and the first sequence its start.
func TestReadGuarded_IfNotExistsSkipsTheStatement(t *testing.T) {
	c := qt.New(t)

	database := readGuarded(c, "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE t (a int);\n"+
		"CREATE TABLE IF NOT EXISTS t (a int UNIQUE, b int REFERENCES p (id), CHECK (a > 0));\n"+
		"CREATE SEQUENCE s START 5;\nCREATE SEQUENCE IF NOT EXISTS s START 10;")

	c.Assert(columnsOfTable(database, "T"), qt.DeepEquals, []string{"a"})
	c.Assert(database.Constraints, qt.HasLen, 0)
	c.Assert(database.Sequences, qt.HasLen, 1)
	c.Assert(*database.Sequences[0].Start, qt.Equals, int64(5))
}

// TestReadGuarded_OrReplaceKeepsTheLastDefinition holds that a CREATE OR
// REPLACE view, function or trigger replaces the earlier definition, as
// PostgreSQL 15.19 does, and leaves an overload with other arguments alone.
func TestReadGuarded_OrReplaceKeepsTheLastDefinition(t *testing.T) {
	c := qt.New(t)

	database := readGuarded(c, "CREATE TABLE t (a int, b int);\n"+
		"CREATE VIEW v AS SELECT a FROM t;\nCREATE OR REPLACE VIEW v AS SELECT b AS a FROM t;\n"+
		"CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql AS $$ SELECT x $$;\n"+
		"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n"+
		"CREATE OR REPLACE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;\n"+
		"CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n"+
		"CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();\n"+
		"CREATE OR REPLACE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();")

	c.Assert(database.Views, qt.HasLen, 1)
	c.Assert(database.Views[0].Body, qt.Equals, "SELECT b AS a FROM t")
	bodies := make(map[string]string)
	for _, function := range database.Functions {
		bodies[function.Name+"("+function.Parameters+")"] = function.Body
	}
	c.Assert(bodies, qt.DeepEquals, map[string]string{
		"f(x int)": "SELECT x", "f()": "SELECT 2", "trg()": "BEGIN RETURN NEW; END",
	})
	c.Assert(database.Triggers, qt.HasLen, 1)
	c.Assert(database.Triggers[0].Event, qt.Equals, "UPDATE")
}
