package sqlschema_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/builtintest"
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

			db, statements, err := sqlschema.ReadWith(c.Context(), []byte(extensionDocument), target, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `CREATE EXTENSION pg_trgm: an extension is a PostgreSQL object, and `+target+` refuses the statement`)
			c.Assert(db.Tables, qt.HasLen, 0)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRead_ACreateExtensionWithoutItsOwner_FailurePath pins that a read which
// selects no extension owner refuses CREATE EXTENSION on a target that could
// host it: an extension is only its owner's object.
func TestRead_ACreateExtensionWithoutItsOwner_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, target string }{
		{name: "postgres", target: platform.Postgres}, {name: "cockroachdb", target: platform.CockroachDB}, {name: "no target"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, statements, err := sqlschema.Read([]byte(extensionDocument), test.target)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `CREATE EXTENSION pg_trgm needs the PostgreSQL extension owner, which this read did not select`)
			c.Assert(db.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRead_ACreateExtensionOutsideTheFamily_HappyPath is the control: a
// PostgreSQL-family document, and one read for no named target, hand the
// statement to the extension owner.
func TestRead_ACreateExtensionOutsideTheFamily_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, target string }{
		{name: "postgres", target: platform.Postgres}, {name: "cockroachdb", target: platform.CockroachDB},
		{name: "yugabytedb", target: platform.YugabyteDB}, {name: "spanner", target: platform.Spanner}, {name: "no target"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := sqlschema.ReadWith(c.Context(), []byte(extensionDocument), test.target, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})

			c.Assert(err, qt.IsNil)
			c.Assert(pgextension.Names(db.FeatureObjects), qt.DeepEquals, []string{"pg_trgm"})
		})
	}
}

// TestRead_AChangeToAnExtension_FailurePath pins that a schema document
// refuses DROP EXTENSION and ALTER EXTENSION by name. The extension owner
// reads each as the operation it performs, which declares nothing, and a
// schema document describes objects.
func TestRead_AChangeToAnExtension_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a drop", sql: "DROP EXTENSION IF EXISTS pg_trgm CASCADE;", want: "DROP EXTENSION IF EXISTS pg_trgm CASCADE"},
		{name: "an update", sql: "ALTER EXTENSION pg_trgm UPDATE TO '1.6';", want: "ALTER EXTENSION pg_trgm UPDATE TO '1.6'"},
		{name: "a move", sql: "ALTER EXTENSION pg_trgm SET SCHEMA ext;", want: "ALTER EXTENSION pg_trgm SET SCHEMA ext"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, statements, err := sqlschema.ReadWith(c.Context(), []byte("CREATE EXTENSION pg_trgm;\n"+test.sql), platform.Postgres, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, `the schema model has no place for this statement: `+regexp.QuoteMeta(test.want))
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}
