package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/parser"
)

// TestParse_AlterGuard_FailurePath refuses an existence guard inside ALTER
// TABLE on a MySQL-family target that does not take it. Measured on MySQL
// 8.4.11 and 26.7.0, each clause answers ERROR 1064 (42000). Without the
// refusal a guard is read, and it drops or adds the object it names in a file
// the server refuses to run (stokaro/ptah#3877). A capability set that answers false for the index guard
// refuses it on MariaDB too, as the renderer declines to write it there.
func TestParse_AlterGuard_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		clause  string
		wantErr string
	}{
		{
			name: "DROP INDEX", dialect: platform.MySQL, clause: "DROP INDEX IF EXISTS ix",
			wantErr: `IF EXISTS at position 25 in ALTER TABLE \.\.\. DROP INDEX: mysql takes no IF EXISTS on DROP INDEX, ` +
				`and answers ERROR 1064 \(42000\) to one; write the clause without it`,
		},
		{
			name: "DROP KEY", dialect: platform.MySQL, clause: "DROP KEY IF EXISTS ix",
			wantErr: `IF EXISTS at position 23 in ALTER TABLE \.\.\. DROP KEY: mysql takes no IF EXISTS on DROP KEY, .*`,
		},
		{
			name: "DROP FOREIGN KEY", dialect: platform.MySQL, clause: "DROP FOREIGN KEY IF EXISTS fk",
			wantErr: `IF EXISTS at position 31 in ALTER TABLE \.\.\. DROP FOREIGN KEY: mysql takes no IF EXISTS on DROP FOREIGN KEY, .*`,
		},
		{
			name: "DROP CHECK", dialect: platform.MySQL, clause: "DROP CHECK IF EXISTS ck",
			wantErr: `IF EXISTS at position 25 in ALTER TABLE \.\.\. DROP CHECK: mysql takes no IF EXISTS on DROP CHECK, .*`,
		},
		{
			name: "DROP CONSTRAINT", dialect: platform.MySQL, clause: "DROP CONSTRAINT IF EXISTS ck",
			wantErr: `IF EXISTS at position 30 in ALTER TABLE \.\.\. DROP CONSTRAINT: mysql takes no IF EXISTS on DROP CONSTRAINT, .*`,
		},
		{
			name: "DROP COLUMN", dialect: platform.MySQL, clause: "DROP COLUMN IF EXISTS w",
			wantErr: `IF EXISTS at position 26 in ALTER TABLE \.\.\. DROP COLUMN: mysql takes no IF EXISTS on DROP COLUMN, .*`,
		},
		{
			name: "DROP without COLUMN", dialect: platform.MySQL, clause: "DROP IF EXISTS w",
			wantErr: `IF EXISTS at position 19 in ALTER TABLE \.\.\. DROP: mysql takes no IF EXISTS on DROP, .*`,
		},
		{
			name: "ADD COLUMN", dialect: platform.MySQL, clause: "ADD COLUMN IF NOT EXISTS y int",
			wantErr: `IF NOT EXISTS at position 25 in ALTER TABLE \.\.\. ADD COLUMN: mysql takes no IF NOT EXISTS on ADD COLUMN, .*`,
		},
		{
			name: "a MariaDB line without the index guard", dialect: platform.MariaDB,
			caps: capability.MariaDBLegacy(), clause: "DROP INDEX IF EXISTS ix",
			wantErr: `IF EXISTS at position 25 in ALTER TABLE \.\.\. DROP INDEX: mariadb takes no IF EXISTS on DROP INDEX, .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("ALTER TABLE c "+row.clause+";",
				parser.WithDialect(row.dialect), parser.WithCapabilities(row.caps)).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParse_AlterGuard_HappyPath reads an existence guard inside ALTER TABLE
// where the target takes it. Measured on MariaDB 11.8.9 and 12.3.3, each
// MariaDB clause is accepted. A dialect-neutral document is read as written,
// and a MySQL capability set that answers true for the index guard takes it,
// because the key decides for the parser as it does for the renderer. A set
// that does not answer for the key leaves the dialect's default to decide.
func TestParse_AlterGuard_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		clause  string
		want    []ast.AlterOperation
	}{
		{
			name: "MariaDB DROP INDEX", dialect: platform.MariaDB, clause: "DROP INDEX IF EXISTS ix",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "ix", IfExists: true, Unique: true}},
		},
		{
			name: "MariaDB DROP KEY", dialect: platform.MariaDB, clause: "DROP KEY IF EXISTS ix",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "ix", IfExists: true, Unique: true}},
		},
		{
			name: "MariaDB DROP FOREIGN KEY", dialect: platform.MariaDB, clause: "DROP FOREIGN KEY IF EXISTS fk",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "fk", IfExists: true, ForeignKey: true}},
		},
		{
			name: "MariaDB DROP CONSTRAINT", dialect: platform.MariaDB, clause: "DROP CONSTRAINT IF EXISTS ck",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "ck", IfExists: true}},
		},
		{
			name: "MariaDB DROP COLUMN", dialect: platform.MariaDB, clause: "DROP COLUMN IF EXISTS w",
			want: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "w", IfExists: true}},
		},
		{
			name: "MariaDB DROP without COLUMN", dialect: platform.MariaDB, clause: "DROP IF EXISTS w",
			want: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "w", IfExists: true}},
		},
		{
			name: "a dialect-neutral document", dialect: "", clause: "DROP INDEX IF EXISTS ix",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "ix", IfExists: true, Unique: true}},
		},
		{
			name: "a MySQL set answering true for the index guard", dialect: platform.MySQL,
			caps: capability.MySQL84().With(capability.DropIndexIfExists, true), clause: "DROP INDEX IF EXISTS ix",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "ix", IfExists: true, Unique: true}},
		},
		{
			name: "a MariaDB set that does not answer for the guard", dialect: platform.MariaDB,
			caps: capability.Capabilities{capability.Views: true}, clause: "DROP INDEX IF EXISTS ix",
			want: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "ix", IfExists: true, Unique: true}},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("ALTER TABLE c "+row.clause+";",
				parser.WithDialect(row.dialect), parser.WithCapabilities(row.caps)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			alter, ok := statements.Statements[0].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(alter.Operations, qt.DeepEquals, row.want)
		})
	}
}

// TestParse_MariaDBAddColumnGuard_HappyPath reads ADD COLUMN IF NOT EXISTS on
// MariaDB, which takes it, with the guard on the operation.
func TestParse_MariaDBAddColumnGuard_HappyPath(t *testing.T) {
	c := qt.New(t)

	operations := alterOperationsOf(c, platform.MariaDB, "ALTER TABLE c ADD COLUMN IF NOT EXISTS y int;")

	c.Assert(operations, qt.HasLen, 1)
	add, ok := operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(add.IfNotExists, qt.IsTrue)
	c.Assert(add.Column.Name, qt.Equals, "y")
}
