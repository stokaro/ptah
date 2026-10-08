package generator

// White-box testing required: the down migration of a whole MySQL-family
// server is reached through the exported API only by a generator run over a
// live connection to a server. The rule under test is the reversal of the
// database changes, which generateDownMigrationSQL renders from a diff alone.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateDownMigrationSQL_RestoresTheDatabasesOfAWholeServer rolls back a
// whole-server change on MySQL and MariaDB: the database the change created is
// dropped, the one it dropped is created again with the character set and
// collation the server held, and the one it changed is set back
// (stokaro/ptah#3789).
func TestGenerateDownMigrationSQL_RestoresTheDatabasesOfAWholeServer(t *testing.T) {
	up := &difftypes.SchemaDiff{
		SchemasAdded: []schemamodel.Schema{{Name: "r9", Charset: "utf8mb4", Collate: "utf8mb4_bin"}},
		SchemasModified: []difftypes.SchemaChange{{
			Name: "r1", Charset: "latin1", Collate: "latin1_swedish_ci",
			CurrentCharset: "utf8mb4", CurrentCollate: "utf8mb4_0900_ai_ci",
		}},
		SchemasRemoved: []string{"r3"},
	}
	desired := &schemamodel.Database{Schemas: []schemamodel.Schema{
		{Name: "r1", Charset: "latin1", Collate: "latin1_swedish_ci"},
		{Name: "r9", Charset: "utf8mb4", Collate: "utf8mb4_bin"},
	}}
	server := &catalog.Database{Schemas: []catalog.Schema{
		{Name: "r1", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
		{Name: "r3", Charset: "latin1", Collate: "latin1_bin"},
	}}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			down, err := generateDownMigrationSQL(t.Context(), must.Must(builtin.New()),
				up, desired, server, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(down, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `r3` DEFAULT CHARACTER SET latin1 COLLATE latin1_bin")
			c.Assert(down, qt.Contains, "ALTER DATABASE `r1` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
			c.Assert(down, qt.Contains, "DROP DATABASE `r9`")
			c.Assert(down, qt.Not(qt.Contains), "DROP DATABASE `r3`")
		})
	}
}
