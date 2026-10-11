package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/sqlschema"
)

// extensionDocument declares an extension beside a table every target reads.
const extensionDocument = "CREATE EXTENSION IF NOT EXISTS pg_trgm;\nCREATE TABLE docs (id INTEGER PRIMARY KEY);\n"

// TestRead_ACreateExtensionOutsideTheFamily_FailurePath pins that a document
// read for a named target outside the PostgreSQL family refuses CREATE
// EXTENSION. That target's server refuses the statement, and an
// Atlas-compatible apply runs the document on the dev database, where it
// fails, so a read that skipped it would be looser than both.
func TestRead_ACreateExtensionOutsideTheFamily_FailurePath(t *testing.T) {
	for _, target := range []string{platform.MySQL, platform.MariaDB, platform.SQLite, platform.ClickHouse, platform.SQLServer, platform.Oracle} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			db, statements, err := sqlschema.Read([]byte(extensionDocument), target)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `CREATE EXTENSION pg_trgm: an extension is a PostgreSQL object, and `+target+` refuses the statement`)
			c.Assert(db.Tables, qt.HasLen, 0)
			c.Assert(db.Extensions, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRead_ACreateExtensionOutsideTheFamily_HappyPath is the control: a
// PostgreSQL-family document, and one read for no named target, still read
// the statement.
func TestRead_ACreateExtensionOutsideTheFamily_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, target string }{
		{name: "postgres", target: platform.Postgres}, {name: "cockroachdb", target: platform.CockroachDB},
		{name: "yugabytedb", target: platform.YugabyteDB}, {name: "spanner", target: platform.Spanner}, {name: "no target"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := sqlschema.Read([]byte(extensionDocument), test.target)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Extensions, qt.HasLen, 1)
			c.Assert(db.Extensions[0].Name, qt.Equals, "pg_trgm")
		})
	}
}
