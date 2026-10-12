package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// TestMySQLColumnSettings_ReadFromSQL reads a column's character set and ON
// UPDATE clause through the bundled runtime's SQL owners, in a MySQL document
// and in a MariaDB one, whose target the MySQL owner reads only through the
// facets it compares there.
func TestMySQLColumnSettings_ReadFromSQL(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())

			database, _, err := sqlschema.ReadWith(c.Context(),
				[]byte("CREATE TABLE t (id int PRIMARY KEY, ts timestamp CHARACTER SET utf8mb4 ON UPDATE CURRENT_TIMESTAMP);"),
				dialect, nil, sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary()})

			c.Assert(err, qt.IsNil)
			c.Assert(database.Fields, qt.HasLen, 2)
			c.Assert(database.Fields[1].Name, qt.Equals, "ts")
			settings, found, err := mysqlschema.Settings(database.Fields[1].Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(settings, qt.Equals, mysqlschema.ColumnSettings{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP"})
		})
	}
}
