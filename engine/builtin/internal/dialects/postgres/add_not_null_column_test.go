package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// renderAddColumn renders one ADD COLUMN on table for the dialect.
func renderAddColumn(c *qt.C, dialect string, caps capability.Capabilities, table string, column *ast.ColumnNode) string {
	c.Helper()
	got, err := postgres.NewWithCapabilities(caps, dialect).Render(&ast.AlterTableNode{
		Name: table, Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
	})
	c.Assert(err, qt.IsNil)
	return got
}

// Each row is a column that stops ADD COLUMN on a table holding a row, measured on
// PostgreSQL 17.11 and 18.6: `column "c" of relation "t" contains null values`. The
// statement is valid on an empty table, so it stays as it was and a comment sits
// above it. CockroachDB 26.1.8 and YugabyteDB 2026.1.2 stop the same way on a row.
func TestRenderer_AddColumnThatFailsOnARow_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		table   string
		column  *ast.ColumnNode
		want    string
	}{
		{
			name: "a text column", dialect: platform.Postgres, caps: capability.Postgres17(), table: "t",
			column: ast.NewColumn("c", "text").SetNotNull(),
			want: `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "c" text NOT NULL;` + "\n\n",
		},
		{
			name: "a sized type in upper case", dialect: platform.Postgres, caps: capability.Postgres17(), table: "t",
			column: ast.NewColumn("c", "VARCHAR(10)").SetNotNull(),
			want: `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "c" VARCHAR(10) NOT NULL;` + "\n\n",
		},
		{
			name: "an array", dialect: platform.Postgres, caps: capability.Postgres17(), table: "t",
			column: ast.NewColumn("c", "text[]").SetNotNull(),
			want: `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "c" text[] NOT NULL;` + "\n\n",
		},
		{
			name: "a primary key column", dialect: platform.Postgres, caps: capability.Postgres17(), table: "t",
			column: ast.NewColumn("c", "bigint").SetPrimary(),
			want: `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "c" bigint PRIMARY KEY NOT NULL;` + "\n\n",
		},
		{
			name: "a table in a schema", dialect: platform.Postgres, caps: capability.Postgres17(), table: "app.t",
			column: ast.NewColumn("c", "text").SetNotNull(),
			want: `-- POSTGRES: ADD COLUMN "c" fails if "app"."t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "app"."t" ADD COLUMN "c" text NOT NULL;` + "\n\n",
		},
		{
			name: "a line break in a name stays inside the comment", dialect: platform.Postgres, caps: capability.Postgres17(), table: "t",
			column: ast.NewColumn("c\nd", "text").SetNotNull(),
			want: `-- POSTGRES: ADD COLUMN "c d" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				"ALTER TABLE \"t\" ADD COLUMN \"c\nd\" text NOT NULL;\n\n",
		},
		{
			name: "CockroachDB", dialect: platform.CockroachDB, caps: capability.ForDialect(platform.CockroachDB), table: "t",
			column: ast.NewColumn("c", "text").SetNotNull(),
			want: `-- COCKROACHDB: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "c" text NOT NULL;` + "\n\n",
		},
		{
			name: "YugabyteDB", dialect: platform.YugabyteDB, caps: capability.ForDialect(platform.YugabyteDB), table: "t",
			column: ast.NewColumn("c", "text").SetNotNull(),
			want: `-- YUGABYTEDB: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "c" text NOT NULL;` + "\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderAddColumn(c, test.dialect, test.caps, test.table, test.column), qt.Equals, test.want)
		})
	}
}

// An enum column is flagged by the planner, and it has no default of its own, so it
// stops on a row as the column above does.
func TestRenderer_AddEnumColumnThatFailsOnARow_HappyPath(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("c", "mood").SetNotNull()
	column.EnumType = true

	got := renderAddColumn(c, platform.Postgres, capability.Postgres17(), "t", column)

	c.Assert(got, qt.Contains, `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row;`)
}

// Each row is a column the server gives a value, or one that leaves a row free to hold
// NULL, measured on a table with one row on PostgreSQL 17.11 and 18.6. A comment there
// would say a statement fails that runs. A domain is the reason a type somebody created
// stays silent: one with a default fills the row.
func TestRenderer_AddColumnThatRunsOnARow_FailurePath(t *testing.T) {
	identity := ast.NewColumn("c", "integer").SetNotNull()
	identity.IdentityGeneration = "ALWAYS"
	generated := ast.NewColumn("c", "integer").SetNotNull()
	generated.GeneratedExpression = "id + 1"
	tests := []struct {
		name   string
		column *ast.ColumnNode
		want   string
	}{
		{name: "a nullable column", column: ast.NewColumn("c", "text"), want: `ALTER TABLE "t" ADD COLUMN "c" text;` + "\n\n"},
		{name: "a literal default", column: ast.NewColumn("c", "text").SetNotNull().SetDefault("x"), want: `ALTER TABLE "t" ADD COLUMN "c" text NOT NULL DEFAULT 'x';` + "\n\n"},
		{name: "an expression default", column: ast.NewColumn("c", "timestamptz").SetNotNull().SetDefaultExpression("now()"), want: `ALTER TABLE "t" ADD COLUMN "c" timestamptz NOT NULL DEFAULT now();` + "\n\n"},
		{name: "an identity", column: identity, want: `ALTER TABLE "t" ADD COLUMN "c" integer NOT NULL GENERATED ALWAYS AS IDENTITY;` + "\n\n"},
		{name: "a generated column", column: generated, want: `ALTER TABLE "t" ADD COLUMN "c" integer NOT NULL GENERATED ALWAYS AS (id + 1) STORED;` + "\n\n"},
		{name: "a serial", column: ast.NewColumn("c", "serial").SetNotNull(), want: `ALTER TABLE "t" ADD COLUMN "c" serial NOT NULL;` + "\n\n"},
		{name: "a bigserial in upper case", column: ast.NewColumn("c", "BIGSERIAL").SetNotNull(), want: `ALTER TABLE "t" ADD COLUMN "c" BIGSERIAL NOT NULL;` + "\n\n"},
		{name: "a type somebody created", column: ast.NewColumn("c", "app.email").SetNotNull(), want: `ALTER TABLE "t" ADD COLUMN "c" app.email NOT NULL;` + "\n\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderAddColumn(c, platform.Postgres, capability.Postgres18(), "t", test.column), qt.Equals, test.want)
		})
	}
}

// Spanner adds such a column nullable and sets NOT NULL next, and that statement says
// what stops it. One comment is enough.
func TestRenderer_AddNotNullColumnOnSpannerSaysItOnce(t *testing.T) {
	c := qt.New(t)

	got := renderAddColumn(c, platform.Spanner, capability.ForDialect(platform.Spanner), "t", ast.NewColumn("c", "text").SetNotNull())

	c.Assert(got, qt.Equals, `ALTER TABLE "t" ADD COLUMN "c" text;`+"\n"+
		`-- SPANNER: SET NOT NULL fails if any row of "t" holds NULL in "c"; the column declares no default to fill it with.`+"\n"+
		`ALTER TABLE "t" ALTER COLUMN "c" SET NOT NULL;`+"\n\n")
}

// A line break in a name stays inside the comment on SET NOT NULL too. Written whole,
// the rest of the name reaches the server as a statement, and PostgreSQL 18.6 answers
// `syntax error at or near "b"`.
func TestRenderer_SetNotNullCommentStaysOneLine(t *testing.T) {
	c := qt.New(t)

	got, err := postgres.NewWithCapabilities(capability.Postgres18(), platform.Postgres).Render(&ast.AlterTableNode{
		Name: "t", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
			Column: ast.NewColumn("a\nb", "text").SetNotNull(), Changed: ast.ColumnProperties{Nullability: true}, HasChanged: true,
		}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, `-- POSTGRES: SET NOT NULL fails if any row of "t" holds NULL in "a b"; the column declares no default to fill it with.`+"\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"a\nb\" SET NOT NULL;\n\n")
}
