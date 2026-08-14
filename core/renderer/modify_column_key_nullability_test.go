package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// keyColumnModifyAlter is the shape that regressed: a single-column primary key
// whose ast.ColumnNode still says Nullable, handed to ALTER TABLE rather than
// CREATE TABLE.
//
// SetPrimary() stopped clearing Nullable when SQLite was given its own answer
// to "is a key column NOT NULL" (stokaro/ptah#1235), because SQLite's answer is
// no. Every CREATE TABLE renderer writes NOT NULL beside PRIMARY KEY itself and
// so was unaffected, but the PostgreSQL and SQL Server ALTER paths read the
// flag, and a plan that asks PostgreSQL to drop NOT NULL from a key column is
// refused outright with `column "id" is in a primary key` (SQLSTATE 42P16).
// Measured live on PostgreSQL 17: `schema apply` over a key column widened from
// integer to bigint planned DROP NOT NULL and exited 1 where it had applied.
func keyColumnModifyAlter() *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{Column: ast.NewColumn("id", "BIGINT").SetPrimary()},
		},
	}
}

// ordinaryColumnModifyAlter is the same operation on a column that is genuinely
// nullable and is not part of the primary key. It exists so that the guard
// above cannot be satisfied by never making any column nullable: over-correct
// the renderers to always write NOT NULL and these rows redden.
func ordinaryColumnModifyAlter() *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: "users",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{Column: ast.NewColumn("nickname", "TEXT")},
		},
	}
}

type modifyColumnNullabilityCase struct {
	name    string
	dialect string
	// key asserts the rendering of a nullable single-column primary key.
	key func(c *qt.C, out string, err error)
	// ordinary asserts the rendering of a nullable non-key column, pinning the
	// opposite direction so the key guard cannot pass by blanket NOT NULL.
	ordinary func(c *qt.C, out string, err error)
}

// modifyColumnNullabilityCases carries one row per dialect in
// renderer.SupportedDialects(). TestModifyColumn_KeyNullabilityCoversEveryDialect
// asserts the correspondence, so a dialect added without a row here reddens
// rather than silently inheriting the hole this table was written to close.
var modifyColumnNullabilityCases = []modifyColumnNullabilityCase{
	{
		name:    "postgresql",
		dialect: "postgresql",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "id" SET NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "DROP NOT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "nickname" DROP NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "SET NOT NULL")
		},
	},
	{
		name:    "postgres alias",
		dialect: "postgres",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "id" SET NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "DROP NOT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "nickname" DROP NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "SET NOT NULL")
		},
	},
	{
		name:    "cockroachdb",
		dialect: "cockroachdb",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "id" SET NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "DROP NOT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "nickname" DROP NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "SET NOT NULL")
		},
	},
	{
		name:    "yugabytedb",
		dialect: "yugabytedb",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "id" SET NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "DROP NOT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "nickname" DROP NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "SET NOT NULL")
		},
	},
	{
		name:    "spanner",
		dialect: "spanner",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "id" SET NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "DROP NOT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "nickname" DROP NOT NULL;`)
			c.Assert(out, qt.Not(qt.Contains), "SET NOT NULL")
		},
	},
	{
		name:    "sqlserver",
		dialect: "sqlserver",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE [users] ALTER COLUMN [id] BIGINT NOT NULL;")
			c.Assert(out, qt.Not(qt.Contains), "[id] BIGINT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE [users] ALTER COLUMN [nickname] NVARCHAR(MAX) NULL;")
			c.Assert(out, qt.Not(qt.Contains), "NVARCHAR(MAX) NOT NULL")
		},
	},
	{
		name:    "mssql alias",
		dialect: "mssql",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE [users] ALTER COLUMN [id] BIGINT NOT NULL;")
			c.Assert(out, qt.Not(qt.Contains), "[id] BIGINT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE [users] ALTER COLUMN [nickname] NVARCHAR(MAX) NULL;")
			c.Assert(out, qt.Not(qt.Contains), "NVARCHAR(MAX) NOT NULL")
		},
	},
	{
		// MySQL reaches the key column through the branch that writes
		// PRIMARY KEY and never looks at Nullable, so it renders exactly as it
		// did before SetPrimary() stopped clearing the flag.
		name:    "mysql",
		dialect: "mysql",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE `users` MODIFY COLUMN `id` BIGINT PRIMARY KEY;")
			c.Assert(out, qt.Not(qt.Contains), "`id` BIGINT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE `users` MODIFY COLUMN `nickname` TEXT;")
			c.Assert(out, qt.Not(qt.Contains), "NOT NULL")
		},
	},
	{
		name:    "mariadb",
		dialect: "mariadb",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE `users` MODIFY COLUMN `id` BIGINT PRIMARY KEY;")
			c.Assert(out, qt.Not(qt.Contains), "`id` BIGINT NULL")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE `users` MODIFY COLUMN `nickname` TEXT;")
			c.Assert(out, qt.Not(qt.Contains), "NOT NULL")
		},
	},
	{
		// ClickHouse rejects Nullable() on a sorting/primary key column, so its
		// type renderer has always excluded a key column from the wrapping.
		name:    "clickhouse",
		dialect: "clickhouse",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE users MODIFY COLUMN id Int64;")
			c.Assert(out, qt.Not(qt.Contains), "Nullable(")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "ALTER TABLE users MODIFY COLUMN nickname Nullable(String);")
		},
	},
	{
		// SQLite cannot ALTER a column at all: the operation is refused and the
		// caller is expected to plan a table rebuild, so no nullability is
		// rendered for either column and the flag is never read here.
		name:    "sqlite",
		dialect: "sqlite",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.ErrorMatches, `.*requires a table rebuild plan.*`)
			c.Assert(out, qt.Equals, "")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.ErrorMatches, `.*requires a table rebuild plan.*`)
			c.Assert(out, qt.Equals, "")
		},
	},
	{
		name:    "sqlite3 alias",
		dialect: "sqlite3",
		key: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.ErrorMatches, `.*requires a table rebuild plan.*`)
			c.Assert(out, qt.Equals, "")
		},
		ordinary: func(c *qt.C, out string, err error) {
			c.Assert(err, qt.ErrorMatches, `.*requires a table rebuild plan.*`)
			c.Assert(out, qt.Equals, "")
		},
	},
}

func TestModifyColumn_KeyColumnNeverRendersNullable(t *testing.T) {
	for _, test := range modifyColumnNullabilityCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQL(test.dialect, keyColumnModifyAlter())
			test.key(c, out, err)
		})
	}
}

func TestModifyColumn_OrdinaryColumnStillRendersNullable(t *testing.T) {
	for _, test := range modifyColumnNullabilityCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQL(test.dialect, ordinaryColumnModifyAlter())
			test.ordinary(c, out, err)
		})
	}
}

func TestModifyColumn_KeyNullabilityCoversEveryDialect(t *testing.T) {
	c := qt.New(t)

	covered := make([]string, 0, len(modifyColumnNullabilityCases))
	for _, test := range modifyColumnNullabilityCases {
		covered = append(covered, test.dialect)
	}

	c.Assert(covered, qt.ContentEquals, renderer.SupportedDialects())
}
