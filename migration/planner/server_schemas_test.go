package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// serverSchemaChanges is a whole-server diff that creates r9, changes r1 and
// drops r3.
func serverSchemaChanges() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		SchemasAdded: []schemamodel.Schema{{Name: "r9", Charset: "utf8mb4", Collate: "utf8mb4_bin"}},
		SchemasModified: []difftypes.SchemaChange{{
			Name: "r1", Charset: "latin1", Collate: "latin1_swedish_ci",
			CurrentCharset: "utf8mb4", CurrentCollate: "utf8mb4_0900_ai_ci",
		}},
		SchemasRemoved: []string{"r3"},
	}
}

// TestGenerateSchemaDiffSQLStatements_PlansTheDatabasesOfAWholeServer_HappyPath
// creates and changes the databases before anything in them, and drops the
// removed ones after everything else, on MySQL and MariaDB (stokaro/ptah#3789).
// A character set or collation the change leaves blank is not written.
func TestGenerateSchemaDiffSQLStatements_PlansTheDatabasesOfAWholeServer_HappyPath(t *testing.T) {
	collationOnly := serverSchemaChanges()
	collationOnly.SchemasModified[0].Charset = ""
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			diff:    serverSchemaChanges(),
			want: []string{
				"CREATE SCHEMA IF NOT EXISTS `r9` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
				"ALTER DATABASE `r1` CHARACTER SET latin1 COLLATE latin1_swedish_ci",
				"DROP DATABASE `r3`",
			},
		},
		{
			name:    "MariaDB, a collation alone",
			dialect: platform.MariaDB,
			diff:    collationOnly,
			want: []string{
				"CREATE SCHEMA IF NOT EXISTS `r9` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
				"ALTER DATABASE `r1` COLLATE latin1_swedish_ci",
				"DROP DATABASE `r3`",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_PlansTheDatabasesOfAWholeServer_FailurePath
// refuses a database change on a dialect whose planner plans no database. Only
// a comparison of a whole MySQL-family server records one, so another planner
// meets it only in a diff built by hand, and planning nothing would report the
// two sides equal.
func TestGenerateSchemaDiffSQLStatements_PlansTheDatabasesOfAWholeServer_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.SQLite, platform.ClickHouse, platform.SQLServer, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(serverSchemaChanges(), dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*the diff creates, drops or changes a database, which only a MySQL or MariaDB plan of a whole server does.*`)
			c.Assert(got, qt.IsNil)
		})
	}
}
