package mysql_test

import (
	"database/sql/driver"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/readscope"
)

func TestReader_RefusesStoredProgramModes_FailurePath(t *testing.T) {
	for _, kind := range []string{"function", "procedure", "trigger"} {
		for _, mode := range []string{
			"PIPES_AS_CONCAT", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE", "REAL_AS_FLOAT",
			"EMPTY_STRING_IS_NULL", "SIMULTANEOUS_ASSIGNMENT", "NO_UNSIGNED_SUBTRACTION",
			"ANSI", "ORACLE", "DB2", "MAXDB", "MSSQL", "POSTGRESQL", "MYSQL323", "MYSQL40",
		} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				c := qt.New(t)
				fixture := storedModeCatalog(kind, "STRICT_TRANS_TABLES,"+mode, "STRICT_TRANS_TABLES")
				db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fixture.answer))

				got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

				c.Assert(err, qt.ErrorMatches, `.*cannot read `+kind+` "fx.program": stored sql_mode includes `+mode+`.*`)
				c.Assert(got, qt.IsNil)
			})
		}
	}
}

func TestReader_RefusesSessionProgramModes_FailurePath(t *testing.T) {
	for _, kind := range []string{"function", "procedure", "trigger"} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			fixture := storedModeCatalog(kind, "", "PIPES_AS_CONCAT")
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fixture.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, `.*cannot read `+kind+` "fx.program": session sql_mode includes PIPES_AS_CONCAT.*`)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestReader_RefusesAMatchingNonportableMode_FailurePath(t *testing.T) {
	c := qt.New(t)
	fixture := storedModeCatalog("function", " pipes_as_concat ,STRICT_TRANS_TABLES", "PIPES_AS_CONCAT")
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fixture.answer))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `.*stored sql_mode includes PIPES_AS_CONCAT.*`)
	c.Assert(got, qt.IsNil)
}

func TestReader_ReadsPortableStoredPrograms_HappyPath(t *testing.T) {
	for _, kind := range []string{"function", "procedure", "trigger"} {
		for _, mode := range []string{
			"", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION", "ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO",
			"NO_BACKSLASH_ESCAPES,ANSI_QUOTES", "TRADITIONAL",
		} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				c := qt.New(t)
				fixture := storedModeCatalog(kind, mode, mode)
				db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fixture.answer))

				got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

				c.Assert(err, qt.IsNil)
				c.Assert(len(got.Functions)+len(got.Triggers), qt.Equals, 1)
			})
		}
	}
}

func TestReader_LeavesUnusedStoredProgramKindsOut_HappyPath(t *testing.T) {
	for _, row := range []struct {
		kind string
		left readscope.Kind
	}{
		{kind: "function", left: readscope.Routines},
		{kind: "procedure", left: readscope.Routines},
		{kind: "trigger", left: readscope.Triggers},
	} {
		t.Run(row.kind, func(t *testing.T) {
			c := qt.New(t)
			fixture := storedModeCatalog(row.kind, "ANSI", "")
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fixture.answer))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Without(t.Context(), row.left))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.IsNotNil)
		})
	}
}

func TestReader_ProgramModeRefusalNamesTheObject_FailurePath(t *testing.T) {
	c := qt.New(t)
	fixture := storedModeCatalog("procedure", "HIGH_NOT_PRECEDENCE", "")
	fixture.routines[0][0] = "a.b`c"
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, fixture.answer))

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `.*`+regexp.QuoteMeta(`procedure "fx.a.b`+"`"+`c": stored sql_mode includes HIGH_NOT_PRECEDENCE`)+`.*`)
	c.Assert(got, qt.IsNil)
}

func storedModeCatalog(kind, stored, session string) visibleCatalog {
	fixture := visibleCatalog{selected: "fx", storedMode: stored, sessionMode: session}
	switch kind {
	case "trigger":
		fixture.tables = []string{"child"}
		fixture.triggers = [][]driver.Value{triggerRow("program", "SET NEW.n = 1")}
	case "function":
		fixture.routines = [][]driver.Value{{"program", "int", "YES", "NO SQL", "DEFINER", "app@%", "app@%", "RETURN 1", "", "FUNCTION"}}
	case "procedure":
		fixture.routines = [][]driver.Value{procedureRow("program", "SELECT 1")}
	}
	return fixture
}
