package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlschema"
)

// Every verb that reads a declared schema goes through Read, so the refusal of
// two CHECKs of one name in one table is asserted here, once, for each dialect
// family the descriptors distinguish. The verbs that never render or compare
// (validate, export, lineage, push) have no later place to refuse it.
func TestRead_TwoChecksOfOneNameInOneTable_FailurePath(t *testing.T) {
	const pair = "CREATE TABLE t (a int, CONSTRAINT dd CHECK (a > 0), CONSTRAINT dd CHECK (a < 9));\n"
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(pair), dialect)

			c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// The read does not know which server a mysql URL reaches, and MariaDB keeps a
// CHECK name per table where MySQL keeps it per schema, so two tables are left
// to the callers that ask the server.
func TestRead_ChecksOfOneNameInTwoTables_HappyPath(t *testing.T) {
	c := qt.New(t)
	const pair = "CREATE TABLE t1 (a int, CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a int, CONSTRAINT ck CHECK (a > 0));\n"

	database, _, err := sqlschema.Read([]byte(pair), platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 2)
}
