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

// typesOfTable is the declared types of the columns of the table structName
// joins, with whether each may hold NULL.
func typesOfTable(database schemamodel.Database, structName string) []string {
	var types []string
	for _, field := range database.Fields {
		if field.StructName == structName {
			types = append(types, field.Type+map[bool]string{true: "", false: " NOT NULL"}[field.Nullable])
		}
	}
	return types
}

// TestReadGuarded_IfNotExistsSkipsTheStatement holds that a CREATE ... IF NOT
// EXISTS of a table or sequence already declared, and an ADD COLUMN IF NOT
// EXISTS of a column the table has, add nothing of their own, as PostgreSQL
// 15.19 skips them whole: the first table keeps its one column, its type and
// no key, and the first sequence its start.
func TestReadGuarded_IfNotExistsSkipsTheStatement(t *testing.T) {
	c := qt.New(t)

	database := readGuarded(c, "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE t (a int);\n"+
		"CREATE TABLE IF NOT EXISTS t (a int UNIQUE, b int REFERENCES p (id), CHECK (a > 0));\n"+
		"ALTER TABLE t ADD COLUMN IF NOT EXISTS a varchar(10) NOT NULL;\n"+
		"CREATE SEQUENCE s START 5;\nCREATE SEQUENCE IF NOT EXISTS s START 10;")

	c.Assert(columnsOfTable(database, "T"), qt.DeepEquals, []string{"a"})
	c.Assert(typesOfTable(database, "T"), qt.DeepEquals, []string{"int"})
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

// TestReadGuarded_OrReplaceFindsTheObjectTheServerFinds holds that OR
// REPLACE finds the earlier object as the server does: written with the
// default schema or without it, and for a trigger only on its own table.
func TestReadGuarded_OrReplaceFindsTheObjectTheServerFinds(t *testing.T) {
	c := qt.New(t)

	database := readGuarded(c, "CREATE TABLE t (a int, b int);\nCREATE TABLE u (a int);\n"+
		"CREATE VIEW public.v AS SELECT a FROM t;\nCREATE OR REPLACE VIEW v AS SELECT b AS a FROM t;\n"+
		"CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n"+
		"CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();\n"+
		"CREATE OR REPLACE TRIGGER tr BEFORE UPDATE ON u FOR EACH ROW EXECUTE FUNCTION trg();")

	c.Assert(database.Views, qt.HasLen, 1)
	c.Assert(database.Views[0].Body, qt.Equals, "SELECT b AS a FROM t")
	events := make(map[string]string)
	for _, trigger := range database.Triggers {
		events[trigger.Table+"."+trigger.Name] = trigger.Event
	}
	c.Assert(events, qt.DeepEquals, map[string]string{"t.tr": "INSERT", "u.tr": "UPDATE"})
}

// TestReadGuarded_OrReplaceKeepsTheComment holds that a replaced view,
// function or trigger keeps the comment COMMENT ON gave it, as PostgreSQL
// 15.19 keeps it, when the new statement sets none.
func TestReadGuarded_OrReplaceKeepsTheComment(t *testing.T) {
	c := qt.New(t)

	database := readGuarded(c, "CREATE TABLE t (a int, b int);\n"+
		"CREATE VIEW v AS SELECT a FROM t;\nCOMMENT ON VIEW v IS 'view kept';\nCREATE OR REPLACE VIEW v AS SELECT b AS a FROM t;\n"+
		"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\nCOMMENT ON FUNCTION f() IS 'function kept';\n"+
		"CREATE OR REPLACE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;\n"+
		"CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;\nCOMMENT ON PROCEDURE p() IS 'procedure kept';\n"+
		"CREATE OR REPLACE PROCEDURE p() LANGUAGE sql AS $$ SELECT 2 $$;\n"+
		"CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n"+
		"CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trg();\nCOMMENT ON TRIGGER tr ON t IS 'trigger kept';\n"+
		"CREATE OR REPLACE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();")

	comments := map[string]string{"view": database.Views[0].Comment, "trigger": database.Triggers[0].Comment}
	for _, function := range database.Functions {
		comments[function.Name] = function.Comment
	}
	c.Assert(comments, qt.DeepEquals, map[string]string{
		"view": "view kept", "trigger": "trigger kept", "f": "function kept", "p": "procedure kept", "trg": "",
	})
}

// TestReadGuarded_ReplacesAProcedure holds that CREATE OR REPLACE PROCEDURE
// keeps the last body, as PostgreSQL 15.19 does.
func TestReadGuarded_ReplacesAProcedure(t *testing.T) {
	c := qt.New(t)

	database := readGuarded(c, "CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;\n"+
		"CREATE OR REPLACE PROCEDURE p() LANGUAGE sql AS $$ SELECT 2 $$;")

	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.Functions[0].Body, qt.Equals, "SELECT 2")
}

// TestReadOnto_IfNotExistsSkipsAnEarlierFilesTable holds that a later file
// of a schema directory skips a CREATE TABLE IF NOT EXISTS of a table an
// earlier file created, on the PostgreSQL family, whose namespaces the
// stored-name pass follows across files, and on SQLite, where the reader
// looks for the table in the earlier files' model.
func TestReadOnto_IfNotExistsSkipsAnEarlierFilesTable(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "PostgreSQL", dialect: platform.Postgres},
		{name: "SQLite", dialect: platform.SQLite},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := sqlschema.NewDocument(&schemamodel.Database{})
			earlier, _, err := sqlschema.ReadOnto([]byte("CREATE TABLE s (a int);"), test.dialect, document)
			c.Assert(err, qt.IsNil)
			document = sqlschema.NewDocument(&earlier)

			later, _, err := sqlschema.ReadOnto([]byte("CREATE TABLE IF NOT EXISTS s (a int, c int);"), test.dialect, document)

			c.Assert(err, qt.IsNil)
			c.Assert(later.Tables, qt.HasLen, 0)
			c.Assert(later.Fields, qt.HasLen, 0)
		})
	}
}

// TestReadOnto_IfNotExistsSkipsAnEarlierFilesMaterializedView holds that a
// later file's CREATE MATERIALIZED VIEW IF NOT EXISTS of a view an earlier
// file created adds nothing, as PostgreSQL 15.19 skips it: the first query
// stands. Within one file the model keeps the first of two views of a name
// anyway, so only a later file shows whether the statement is skipped.
func TestReadOnto_IfNotExistsSkipsAnEarlierFilesMaterializedView(t *testing.T) {
	c := qt.New(t)
	document := sqlschema.NewDocument(&schemamodel.Database{})
	earlier, _, err := sqlschema.ReadOnto([]byte("CREATE TABLE t (a int);\nCREATE MATERIALIZED VIEW mv AS SELECT a FROM t;"), platform.Postgres, document)
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT a + 1 AS a FROM t;"),
		platform.Postgres, sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(later.MaterializedViews, qt.HasLen, 0)
	c.Assert(earlier.MaterializedViews, qt.HasLen, 1)
	c.Assert(earlier.MaterializedViews[0].Body, qt.Equals, "SELECT a FROM t")
}

// TestReadOnto_OrReplaceReplacesAnEarlierFilesView holds that a later file's
// CREATE OR REPLACE VIEW replaces the view an earlier file declared, in
// place, as the server runs the files in order.
func TestReadOnto_OrReplaceReplacesAnEarlierFilesView(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int, b int);\nCREATE VIEW v AS SELECT a FROM t;"), platform.Postgres)
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("CREATE OR REPLACE VIEW v AS SELECT b AS a FROM t;"), platform.Postgres, sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(later.Views, qt.HasLen, 0)
	c.Assert(earlier.Views, qt.HasLen, 1)
	c.Assert(earlier.Views[0].Body, qt.Equals, "SELECT b AS a FROM t")
}
