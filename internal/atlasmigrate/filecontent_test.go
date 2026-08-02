package atlasmigrate_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/migration/migrator"
)

func defaultMigrateDiffFormat() string {
	return atlasreport.NormalizeMigrateDiffFormat("")
}

func concurrentIndexNode(name, table, column string) *ast.IndexNode {
	return &ast.IndexNode{
		Name:         name,
		Table:        table,
		Columns:      []string{column},
		Concurrently: true,
	}
}

func TestBuildMigrationFileContents_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Run("transactional plan stays one plain file", func(c *qt.C) {
		nodes := []ast.Node{
			ast.NewCreateTable("users").AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()),
		}

		contents, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), nodes)

		c.Assert(err, qt.IsNil)
		c.Assert(contents, qt.HasLen, 1)
		c.Assert(contents[0].NameSuffix, qt.Equals, "")
		c.Assert(contents[0].NoTransaction, qt.IsFalse)
		c.Assert(contents[0].SQL, qt.Contains, `CREATE TABLE "users"`)
		c.Assert(contents[0].SQL, qt.Not(qt.Contains), "atlas:txmode")
	})

	c.Run("concurrent-only plan carries txmode none header", func(c *qt.C) {
		nodes := []ast.Node{concurrentIndexNode("idx_users_email", "users", "email")}

		contents, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), nodes)

		c.Assert(err, qt.IsNil)
		c.Assert(contents, qt.HasLen, 1)
		c.Assert(contents[0].NameSuffix, qt.Equals, "")
		c.Assert(contents[0].NoTransaction, qt.IsTrue)
		c.Assert(strings.HasPrefix(contents[0].SQL, "-- atlas:txmode none\n\n"), qt.IsTrue)
		c.Assert(contents[0].SQL, qt.Contains, "CREATE INDEX CONCURRENTLY")
	})

	c.Run("mixed plan splits into transactional then concurrent files", func(c *qt.C) {
		nodes := []ast.Node{
			ast.NewCreateTable("orders").AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()),
			concurrentIndexNode("idx_users_email", "users", "email"),
		}

		contents, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), nodes)

		c.Assert(err, qt.IsNil)
		c.Assert(contents, qt.HasLen, 2)
		c.Assert(contents[0].NameSuffix, qt.Equals, "_transactional")
		c.Assert(contents[0].NoTransaction, qt.IsFalse)
		c.Assert(contents[0].SQL, qt.Contains, `CREATE TABLE "orders"`)
		c.Assert(contents[0].SQL, qt.Not(qt.Contains), "atlas:txmode")
		c.Assert(contents[0].SQL, qt.Not(qt.Contains), "CONCURRENTLY")
		c.Assert(contents[1].NameSuffix, qt.Equals, "_concurrent_indexes")
		c.Assert(contents[1].NoTransaction, qt.IsTrue)
		c.Assert(strings.HasPrefix(contents[1].SQL, "-- atlas:txmode none\n\n"), qt.IsTrue)
		c.Assert(contents[1].SQL, qt.Contains, "CREATE INDEX CONCURRENTLY")
		c.Assert(contents[1].SQL, qt.Not(qt.Contains), "CREATE TABLE")
	})

	c.Run("comments accompany no-transaction statements in one file", func(c *qt.C) {
		nodes := []ast.Node{
			ast.NewComment("concurrent index build"),
			concurrentIndexNode("idx_users_email", "users", "email"),
		}

		contents, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), nodes)

		c.Assert(err, qt.IsNil)
		c.Assert(contents, qt.HasLen, 1)
		c.Assert(contents[0].NoTransaction, qt.IsTrue)
		c.Assert(strings.HasPrefix(contents[0].SQL, "-- atlas:txmode none\n\n"), qt.IsTrue)
		c.Assert(contents[0].SQL, qt.Contains, "concurrent index build")
		c.Assert(contents[0].SQL, qt.Contains, "CREATE INDEX CONCURRENTLY")
	})

	c.Run("non-postgres dialect never splits", func(c *qt.C) {
		nodes := []ast.Node{
			ast.NewCreateTable("users").AddColumn(ast.NewColumn("id", "INTEGER").SetPrimary()),
			&ast.IndexNode{Name: "idx_users_email", Table: "users", Columns: []string{"email"}},
		}

		contents, err := atlasmigrate.BuildMigrationFileContents(
			platform.MySQL, capability.ForDialect(platform.MySQL), defaultMigrateDiffFormat(), nodes)

		c.Assert(err, qt.IsNil)
		c.Assert(contents, qt.HasLen, 1)
		c.Assert(contents[0].NoTransaction, qt.IsFalse)
	})

	c.Run("deterministic output for identical plans", func(c *qt.C) {
		buildNodes := func() []ast.Node {
			return []ast.Node{
				ast.NewCreateTable("orders").AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()),
				concurrentIndexNode("idx_users_email", "users", "email"),
			}
		}

		first, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), buildNodes())
		c.Assert(err, qt.IsNil)
		second, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), buildNodes())
		c.Assert(err, qt.IsNil)
		c.Assert(first, qt.DeepEquals, second)
	})
}

func TestBuildMigrationFileContents_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("mixed plan with non-concurrent no-transaction statements is refused", func(c *qt.C) {
		nodes := []ast.Node{
			ast.NewCreateTable("orders").AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()),
			ast.NewAlterType("enum_status").AddOperation(ast.NewAddEnumValueOperation("archived")),
		}

		contents, err := atlasmigrate.BuildMigrationFileContents(
			platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), nodes)

		c.Assert(err, qt.ErrorMatches,
			`generated migration mixes transactional statements with non-transactional statements that cannot be split automatically`)
		c.Assert(contents, qt.HasLen, 0)
	})
}

// TestBuildMigrationFileContents_TxModeRoundTrip proves the generated
// execution metadata survives parse -> render -> reload: files written with
// the Atlas txmode directive are reloaded by the migration provider with the
// matching no-transaction execution mode.
func TestBuildMigrationFileContents_TxModeRoundTrip(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		ast.NewCreateTable("orders").AddColumn(ast.NewColumn("id", "SERIAL").SetPrimary()),
		concurrentIndexNode("idx_users_email", "users", "email"),
	}
	contents, err := atlasmigrate.BuildMigrationFileContents(
		platform.Postgres, capability.ForDialect(platform.Postgres), defaultMigrateDiffFormat(), nodes)
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.HasLen, 2)

	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_add_transactional.sql"), []byte(contents[0].SQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_add_concurrent_indexes.sql"), []byte(contents[1].SQL), 0o600), qt.IsNil)

	provider, err := migrator.NewFSMigrationProvider(
		os.DirFS(dir),
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	c.Assert(migrations[0].UpNoTransaction, qt.IsFalse)
	c.Assert(migrations[1].Version, qt.Equals, int64(2))
	c.Assert(migrations[1].UpNoTransaction, qt.IsTrue)
}
