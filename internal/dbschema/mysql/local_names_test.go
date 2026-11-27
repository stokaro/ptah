package mysql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/readscope"
)

// storedViewBody is a view body of database fx as MySQL 26.10 and MariaDB
// 12.3 store it: every relation of fx qualified by fx, and a relation of crm
// by crm.
const storedViewBody = "select `fx`.`child`.`id` AS `id` from (`fx`.`child` join `crm`.`accounts` on(`fx`.`child`.`id` = `crm`.`accounts`.`id`))"

// functionRow is a function the account may list, with its body.
func functionRow(name, body string) []driver.Value {
	return []driver.Value{name, "int", "YES", "NO SQL", "DEFINER", "owner@%", "app@%", body, "", "FUNCTION"}
}

// localNamesCatalog is a database fx holding a view, a function and a
// procedure, read through a connection that selected selected.
func localNamesCatalog(selected any) visibleCatalog {
	return visibleCatalog{
		selected:  selected,
		databases: []string{"fx"},
		tables:    []string{"child"},
		views:     [][]driver.Value{{"v_child", storedViewBody, "NONE", "DEFINER", "app@%", "app@%"}},
		routines:  [][]driver.Value{functionRow("f_double", "RETURN 2"), procedureRow("p_noop", "SELECT 1")},
	}
}

// viewsAndRoutines lists the views and routines of a description as
// schema/name, with the body of each view.
func viewsAndRoutines(database *catalog.Database) []string {
	var got []string
	for _, view := range database.Views {
		got = append(got, "view "+view.Schema+"/"+view.Name+": "+view.Body)
	}
	for _, function := range database.Functions {
		got = append(got, strings.ToLower(function.Kind)+" "+function.Schema+"/"+function.Name)
	}
	return got
}

// TestReader_NamesNoDatabaseInAOneDatabaseRead reads database fx through a
// connection that selected it. Its view, its function and its procedure carry
// no schema, as its tables carry none, and the view body reads fx's own
// relations unqualified while it keeps crm's. A plan recreating these objects
// from a read of a shadow or a dev database named that database, and a
// migration written from a read of one database ran there wherever it was
// applied (stokaro/ptah#6710).
func TestReader_NamesNoDatabaseInAOneDatabaseRead(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "MySQL", version: dbtest.MySQLVersion},
		{name: "MariaDB", version: dbtest.MariaDBVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
				localNamesCatalog("fx").answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Authored(t.Context()))

			c.Assert(err, qt.IsNil)
			c.Assert(viewsAndRoutines(got), qt.DeepEquals, []string{
				"view /v_child: select `child`.`id` AS `id` from (`child` join `crm`.`accounts` on(`child`.`id` = `crm`.`accounts`.`id`))",
				"function /f_double",
				"procedure /p_noop",
			})
		})
	}
}

// TestReader_NamesEachObjectsDatabaseInAServerRead reads the same database
// through a connection that selected none. The read describes the server, so
// every view and routine carries the database it is in, and a view body keeps
// the names the server stored: there the database is what tells the objects
// of one database from another's.
func TestReader_NamesEachObjectsDatabaseInAServerRead(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "MySQL", version: dbtest.MySQLVersion},
		{name: "MariaDB", version: dbtest.MariaDBVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
				localNamesCatalog(nil).answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Authored(t.Context()))

			c.Assert(err, qt.IsNil)
			c.Assert(viewsAndRoutines(got), qt.DeepEquals, []string{
				"view fx/v_child: " + storedViewBody,
				"function fx/f_double",
				"procedure fx/p_noop",
			})
		})
	}
}

// sequenceCatalog is localNamesCatalog with a MariaDB sequence s in fx.
type sequenceCatalog struct {
	visibleCatalog
}

func (s sequenceCatalog) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "TABLE_TYPE = 'SEQUENCE'"):
		return dbtest.QueryResult{Columns: []string{"TABLE_NAME"}, Rows: [][]driver.Value{{"s"}}}, nil
	case strings.Contains(query, "FROM `fx`.`s`"):
		return dbtest.QueryResult{
			Columns: []string{"start_value", "increment", "minimum_value", "maximum_value", "cache_size", "cycle_option"},
			Rows:    [][]driver.Value{{int64(1), int64(1), int64(1), int64(9223372036854775806), int64(1000), int64(0)}},
		}, nil
	}
	return s.visibleCatalog.answer(query, args)
}

// TestReader_NamesNoDatabaseOnASequenceInAOneDatabaseRead reads a MariaDB
// sequence through a connection that selected its database: it carries no
// schema, as a table does.
func TestReader_NamesNoDatabaseOnASequenceInAOneDatabaseRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
		sequenceCatalog{localNamesCatalog("fx")}.answer))

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.Of(map[capability.Capability]bool{capability.Sequences: true})).
		ReadSchemaContext(readscope.Authored(t.Context()))

	c.Assert(err, qt.IsNil)
	c.Assert(got.Sequences, qt.HasLen, 1)
	c.Assert(got.Sequences[0].QualifiedName(), qt.Equals, "s")
}

// TestReader_NamesTheDatabaseOnASequenceInAServerRead is the control: read
// through a connection that selected no database, the sequence carries fx.
func TestReader_NamesTheDatabaseOnASequenceInAServerRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
		sequenceCatalog{localNamesCatalog(nil)}.answer))

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.Of(map[capability.Capability]bool{capability.Sequences: true})).
		ReadSchemaContext(readscope.Authored(t.Context()))

	c.Assert(err, qt.IsNil)
	c.Assert(got.Sequences, qt.HasLen, 1)
	c.Assert(got.Sequences[0].QualifiedName(), qt.Equals, "fx.s")
}

// objectsAsked is a selector that keeps every object and records each one a
// read asks it about, as "kind schema/name".
func objectsAsked(asked *[]string) readscope.Selector {
	return func(read readscope.Read) []bool {
		kept := make([]bool, len(read.Objects))
		for index, object := range read.Objects {
			*asked = append(*asked, string(object.Kind)+" "+object.Schema+"/"+object.Name)
			kept[index] = true
		}
		return kept
	}
}

// TestReader_AsksSelectorsAboutRoutinesAndViewsInTheirDatabase reads a whole
// server under an include or exclude selection. The read finds a view and a
// routine of fx without a schema before it names fx on them, and asks the
// selectors which routine bodies to read while it does. A selection that
// names `fx.f_double` keeps or leaves out the routine only when the read
// names fx there too; asked about a bare f_double, it reads no database in
// front of it, since a read of a server qualifies every object.
func TestReader_AsksSelectorsAboutRoutinesAndViewsInTheirDatabase(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
		localNamesCatalog(nil).answer))
	var asked []string

	_, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Selecting(t.Context(), objectsAsked(&asked)))

	c.Assert(err, qt.IsNil)
	c.Assert(asked, qt.Contains, "routines fx/f_double")
	c.Assert(asked, qt.Contains, "routines fx/p_noop")
	c.Assert(asked, qt.Contains, "views fx/v_child")
}

// sequenceDefaultCatalog is sequenceCatalog with columns of child defaulted
// from fx's sequence s, from crm's and from a string, as MariaDB 12.3 stores
// each: the sequence qualified by its database, however the statement spelled
// it.
type sequenceDefaultCatalog struct {
	sequenceCatalog
}

func (s sequenceDefaultCatalog) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "FROM information_schema.COLUMNS") || !strings.Contains(query, "COLUMN_DEFAULT") {
		return s.sequenceCatalog.answer(query, args)
	}
	row := func(name, value string, position int64) []driver.Value {
		return []driver.Value{"child", name, "int", "int(11)", "YES", value, nil, int64(10), int64(0), position, nil, nil, "", nil, ""}
	}
	return dbtest.QueryResult{
		Columns: []string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT",
			"CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "ORDINAL_POSITION",
			"CHARACTER_SET_NAME", "COLLATION_NAME", "EXTRA", "GENERATION_EXPRESSION", "COLUMN_COMMENT",
		},
		Rows: [][]driver.Value{
			row("id", "nextval(`fx`.`s`)", 1),
			row("last", "lastval(`fx`.`s`)", 2),
			row("reset", "setval(`fx`.`s`,5,1,0)", 3),
			row("other", "nextval(`crm`.`s`)", 4),
			row("plus", "(nextval(`fx`.`s`) + 1)", 5),
		},
	}, nil
}

// childDefaults lists the column defaults of the table child.
func childDefaults(database *catalog.Database) []string {
	var got []string
	for _, table := range database.Tables {
		for _, column := range table.Columns {
			if column.ColumnDefault != nil {
				got = append(got, table.QualifiedName()+"."+column.Name+" "+*column.ColumnDefault)
			}
		}
	}
	return got
}

// TestReader_NamesNoDatabaseInASequenceDefaultInAOneDatabaseRead reads, through
// a connection that selected fx, defaults that read fx's sequence: they read it
// without the database, so a plan written from a read of a shadow or a dev
// database draws from the sequence of the database it runs in. A default that
// reads crm's sequence keeps crm (stokaro/ptah#6710).
func TestReader_NamesNoDatabaseInASequenceDefaultInAOneDatabaseRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
		sequenceDefaultCatalog{sequenceCatalog{localNamesCatalog("fx")}}.answer))

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.Of(map[capability.Capability]bool{capability.Sequences: true})).
		ReadSchemaContext(readscope.Authored(t.Context()))

	c.Assert(err, qt.IsNil)
	c.Assert(childDefaults(got), qt.DeepEquals, []string{
		"child.id nextval(`s`)",
		"child.last lastval(`s`)",
		"child.reset setval(`s`,5,1,0)",
		"child.other nextval(`crm`.`s`)",
		"child.plus (nextval(`s`) + 1)",
	})
}

// TestReader_NamesTheDatabaseInASequenceDefaultInAServerRead is the control:
// read through a connection that selected no database, a default keeps the
// database the server stored, which is what tells fx's sequence from
// another database's.
func TestReader_NamesTheDatabaseInASequenceDefaultInAServerRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MariaDBVersion, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
		sequenceDefaultCatalog{sequenceCatalog{localNamesCatalog(nil)}}.answer))

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.Of(map[capability.Capability]bool{capability.Sequences: true})).
		ReadSchemaContext(readscope.Authored(t.Context()))

	c.Assert(err, qt.IsNil)
	c.Assert(childDefaults(got), qt.DeepEquals, []string{
		"fx.child.id nextval(`fx`.`s`)",
		"fx.child.last lastval(`fx`.`s`)",
		"fx.child.reset setval(`fx`.`s`,5,1,0)",
		"fx.child.other nextval(`crm`.`s`)",
		"fx.child.plus (nextval(`fx`.`s`) + 1)",
	})
}
