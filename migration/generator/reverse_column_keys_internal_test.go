package generator

// White-box testing required: upMigrationSQL and downMigrationSQL are
// package-local, and the property is which statements the reversal plans for a
// column's own key, which only the rendered pair shows.

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// keyTable is a table whose first column, id, is its primary key. unique are
// the columns the database read reports as unique, or the columns the
// declaration writes UNIQUE.
type keyTable struct {
	name    string
	columns []string
	unique  []string
}

// keyConstraint is a key the database holds beside each table's primary key:
// a UNIQUE, or a foreign key to the column code of references.
type keyConstraint struct {
	table      string
	name       string
	kind       string
	columns    []string
	references string
}

// keyDatabase is the database read of tables and keys as the PostgreSQL and
// MySQL readers report it: a UNIQUE as a constraint and as its index.
func keyDatabase(tables []keyTable, keys []keyConstraint) *catalog.Database {
	database := &catalog.Database{}
	for _, table := range tables {
		read := catalog.Table{Name: table.name, Type: "BASE TABLE"}
		for position, column := range table.columns {
			nullable := "YES"
			if position == 0 {
				nullable = "NO"
			}
			read.Columns = append(read.Columns, catalog.Column{
				Name: column, DataType: "integer", FormattedType: "integer", IsNullable: nullable,
				IsPrimaryKey: position == 0, IsUnique: slices.Contains(table.unique, column), OrdinalPosition: position + 1,
			})
		}
		database.Tables = append(database.Tables, read)
		database.Indexes = append(database.Indexes, catalog.Index{
			Name: table.name + "_pkey", TableName: table.name, Columns: []string{"id"}, IsUnique: true, IsPrimary: true,
		})
		database.Constraints = append(database.Constraints, catalog.Constraint{
			Name: table.name + "_pkey", TableName: table.name, Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"},
		})
	}
	for _, key := range keys {
		constraint := catalog.Constraint{
			Name: key.name, TableName: key.table, Type: key.kind, ColumnName: key.columns[0], ColumnNames: key.columns,
		}
		if key.kind == "UNIQUE" {
			database.Indexes = append(database.Indexes, catalog.Index{
				Name: key.name, TableName: key.table, Columns: key.columns, IsUnique: true,
			})
		}
		if key.kind == "FOREIGN KEY" {
			constraint.ForeignTable = new(key.references)
			constraint.ForeignColumn = new("code")
			constraint.ForeignColumns = []string{"code"}
			constraint.DeleteRule = new("NO ACTION")
			constraint.UpdateRule = new("NO ACTION")
		}
		database.Constraints = append(database.Constraints, constraint)
	}
	return database
}

// keyDeclaration declares tables, each column an INTEGER.
func keyDeclaration(tables []keyTable) *schemamodel.Database {
	declaration := &schemamodel.Database{}
	for _, table := range tables {
		declaration.Tables = append(declaration.Tables, schemamodel.Table{StructName: table.name, Name: table.name})
		for position, column := range table.columns {
			declaration.Fields = append(declaration.Fields, schemamodel.Field{
				StructName: table.name, Name: column, Type: "INTEGER",
				Primary: position == 0, Nullable: position != 0, Unique: slices.Contains(table.unique, column),
			})
		}
	}
	return declaration
}

// statementsOf is the statements of a rendered migration, without its comments.
func statementsOf(sql string) []string {
	var lines []string
	for line := range strings.SplitSeq(sql, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	var statements []string
	for statement := range strings.SplitSeq(strings.Join(lines, "\n"), ";") {
		if statement = strings.TrimSpace(statement); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

// TestReverseColumnKeys_DownRestoresTheKeysTheDatabaseHeld plans the up and
// the down file of a change to a column's own UNIQUE, and of a change beside
// it that takes or frees the name of the column's key.
//
// The forward direction adds a column's key through the column, so exchanging
// the constraint lists gives the rollback no removal for it: a column the up
// file made UNIQUE kept its key on the way down, and a key the down file adds
// back under the same name was refused. And the rollback restored a key the
// up file dropped twice, once as the removed constraint and once through the
// column's UNIQUE. Each down file here, applied after its up file, gives the
// database the keys it held before; the live half of this test applies them
// (integration/column_key_rollback_e2e_test.go).
func TestReverseColumnKeys_DownRestoresTheKeysTheDatabaseHeld(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *catalog.Database
		desired  []keyTable
		up       []string
		down     []string
	}{
		{
			name:    "PostgreSQL drops a UNIQUE column a foreign key references, and its table",
			dialect: platform.Postgres,
			caps:    capability.Postgres17(),
			database: keyDatabase(
				[]keyTable{{name: "t", columns: []string{"id", "code"}, unique: []string{"code"}}, {name: "u", columns: []string{"id", "t_code"}}},
				[]keyConstraint{
					{table: "t", name: "t_code_key", kind: "UNIQUE", columns: []string{"code"}},
					{table: "u", name: "u_t_code_fkey", kind: "FOREIGN KEY", columns: []string{"t_code"}, references: "t"},
				},
			),
			desired: []keyTable{{name: "t", columns: []string{"id"}}},
			up: []string{
				`ALTER TABLE "u" DROP CONSTRAINT IF EXISTS "u_t_code_fkey"`,
				`ALTER TABLE "t" DROP COLUMN "code" CASCADE`,
				`ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_code_key"`,
				`DROP TABLE IF EXISTS "u" CASCADE`,
			},
			down: []string{
				"CREATE TABLE \"u\" (\n  \"id\" integer PRIMARY KEY NOT NULL,\n  \"t_code\" integer\n)",
				`ALTER TABLE "t" ADD COLUMN "code" integer`,
				`ALTER TABLE "t" ADD CONSTRAINT "t_code_key" UNIQUE ("code")`,
				`ALTER TABLE "u" ADD CONSTRAINT "u_t_code_fkey" FOREIGN KEY ("t_code") REFERENCES "t"("code") ON DELETE NO ACTION ON UPDATE NO ACTION`,
			},
		},
		{
			name:     "PostgreSQL drops a UNIQUE column",
			dialect:  platform.Postgres,
			caps:     capability.Postgres17(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}}, []keyConstraint{{table: "c", name: "c_x_key", kind: "UNIQUE", columns: []string{"x"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id"}}},
			up:       []string{`ALTER TABLE "c" DROP COLUMN "x" CASCADE`, `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`},
			down:     []string{`ALTER TABLE "c" ADD COLUMN "x" integer`, `ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`},
		},
		{
			name:     "PostgreSQL takes UNIQUE off a column",
			dialect:  platform.Postgres,
			caps:     capability.Postgres17(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}}, []keyConstraint{{table: "c", name: "c_x_key", kind: "UNIQUE", columns: []string{"x"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}}},
			up:       []string{`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`},
			down:     []string{`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`},
		},
		{
			name:     "PostgreSQL makes a column UNIQUE",
			dialect:  platform.Postgres,
			caps:     capability.Postgres17(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}}}, nil),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}},
			up:       []string{`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`},
			down:     []string{`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`},
		},
		{
			name:     "PostgreSQL makes a column UNIQUE whose key has MySQL's name",
			dialect:  platform.Postgres,
			caps:     capability.Postgres17(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}}, []keyConstraint{{table: "c", name: "x", kind: "UNIQUE", columns: []string{"x"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}},
			up:       []string{`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`, `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "x"`},
			down:     []string{`ALTER TABLE "c" ADD CONSTRAINT "x" UNIQUE ("x")`, `ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`},
		},
		{
			name:    "PostgreSQL makes a column UNIQUE whose name a key over another column holds",
			dialect: platform.Postgres,
			caps:    capability.Postgres17(),
			database: keyDatabase(
				[]keyTable{{name: "c", columns: []string{"id", "x", "y"}, unique: []string{"x", "y"}}},
				[]keyConstraint{
					{table: "c", name: "c_x_uq", kind: "UNIQUE", columns: []string{"x"}},
					{table: "c", name: "c_x_key", kind: "UNIQUE", columns: []string{"y"}},
				},
			),
			desired: []keyTable{{name: "c", columns: []string{"id", "x", "y"}, unique: []string{"x"}}},
			up: []string{
				`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`,
				`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x")`,
				`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_uq"`,
			},
			down: []string{
				`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`,
				`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("y")`,
				`ALTER TABLE "c" ADD CONSTRAINT "c_x_uq" UNIQUE ("x")`,
			},
		},
		{
			name:     "PostgreSQL adds a UNIQUE column whose name a key over another column holds",
			dialect:  platform.Postgres,
			caps:     capability.Postgres17(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "y"}, unique: []string{"y"}}}, []keyConstraint{{table: "c", name: "c_x_key", kind: "UNIQUE", columns: []string{"y"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id", "y", "x"}, unique: []string{"x"}}},
			up:       []string{`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`, `ALTER TABLE "c" ADD COLUMN "x" INTEGER UNIQUE`},
			down: []string{
				`ALTER TABLE "c" DROP CONSTRAINT IF EXISTS "c_x_key"`,
				`ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("y")`,
				`ALTER TABLE "c" DROP COLUMN "x" CASCADE`,
			},
		},
		{
			// The control: the column's key goes with the column, and nothing
			// else takes its name, so the rollback drops the column alone.
			name:     "PostgreSQL adds a UNIQUE column",
			dialect:  platform.Postgres,
			caps:     capability.Postgres17(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id"}}}, nil),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}},
			up:       []string{`ALTER TABLE "c" ADD COLUMN "x" INTEGER UNIQUE`},
			down:     []string{`ALTER TABLE "c" DROP COLUMN "x" CASCADE`},
		},
		{
			name:     "MySQL drops a UNIQUE column",
			dialect:  platform.MySQL,
			caps:     capability.MySQL84(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}}, []keyConstraint{{table: "c", name: "x", kind: "UNIQUE", columns: []string{"x"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id"}}},
			up:       []string{"ALTER TABLE `c` DROP COLUMN `x`"},
			down:     []string{"ALTER TABLE `c` ADD COLUMN `x` integer", "ALTER TABLE `c` ADD CONSTRAINT `x` UNIQUE (`x`)"},
		},
		{
			name:     "MySQL takes UNIQUE off a column",
			dialect:  platform.MySQL,
			caps:     capability.MySQL84(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}}, []keyConstraint{{table: "c", name: "x", kind: "UNIQUE", columns: []string{"x"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}}},
			up:       []string{"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER", "ALTER TABLE `c` DROP INDEX `x`"},
			down:     []string{"ALTER TABLE `c` MODIFY COLUMN `x` integer", "ALTER TABLE `c` ADD CONSTRAINT `x` UNIQUE (`x`)"},
		},
		{
			name:     "MySQL makes a column UNIQUE",
			dialect:  platform.MySQL,
			caps:     capability.MySQL84(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "x"}}}, nil),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}},
			up:       []string{"ALTER TABLE `c` MODIFY COLUMN `x` INTEGER UNIQUE"},
			down:     []string{"ALTER TABLE `c` MODIFY COLUMN `x` integer", "ALTER TABLE `c` DROP INDEX `x`"},
		},
		{
			name:     "MySQL adds a UNIQUE column whose name a key over another column holds",
			dialect:  platform.MySQL,
			caps:     capability.MySQL84(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id", "y"}, unique: []string{"y"}}}, []keyConstraint{{table: "c", name: "x", kind: "UNIQUE", columns: []string{"y"}}}),
			desired:  []keyTable{{name: "c", columns: []string{"id", "y", "x"}, unique: []string{"x"}}},
			up:       []string{"ALTER TABLE `c` DROP INDEX `x`", "ALTER TABLE `c` ADD COLUMN `x` INTEGER UNIQUE", "ALTER TABLE `c` MODIFY COLUMN `y` INTEGER"},
			down: []string{
				"ALTER TABLE `c` MODIFY COLUMN `y` integer",
				"ALTER TABLE `c` DROP COLUMN `x`",
				"ALTER TABLE `c` ADD CONSTRAINT `x` UNIQUE (`y`)",
			},
		},
		{
			name:     "MySQL adds a UNIQUE column",
			dialect:  platform.MySQL,
			caps:     capability.MySQL84(),
			database: keyDatabase([]keyTable{{name: "c", columns: []string{"id"}}}, nil),
			desired:  []keyTable{{name: "c", columns: []string{"id", "x"}, unique: []string{"x"}}},
			up:       []string{"ALTER TABLE `c` ADD COLUMN `x` INTEGER UNIQUE"},
			down:     []string{"ALTER TABLE `c` DROP COLUMN `x`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := keyDeclaration(test.desired)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, test.database, test.dialect, runtime))

			up, upErr := upMigrationSQL(t.Context(), runtime, diff, desired, test.dialect, test.caps)
			down, downErr := downMigrationSQL(t.Context(), runtime, diff, desired, test.database, test.dialect, test.caps)

			c.Assert(upErr, qt.IsNil)
			c.Assert(downErr, qt.IsNil)
			c.Assert(statementsOf(up), qt.DeepEquals, test.up, qt.Commentf("%s", up))
			c.Assert(statementsOf(down), qt.DeepEquals, test.down, qt.Commentf("%s", down))
		})
	}
}
