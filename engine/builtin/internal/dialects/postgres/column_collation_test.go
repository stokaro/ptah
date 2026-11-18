package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/postgres"
)

// collatedTable is a table whose second column names collation.
func collatedTable(collation string) *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name: "t",
		Columns: []*ast.ColumnNode{
			ast.NewColumn("id", "integer").SetPrimary(),
			ast.NewColumn("c", "text").SetNotNull().SetCollate(collation),
		},
	}
}

// TestPostgres_CreateTable_WritesTheColumnCollation pins the COLLATE clause a
// column's collation takes: right after the type, each part quoted, so a
// mixed-case name and a schema keep what they name.
func TestPostgres_CreateTable_WritesTheColumnCollation(t *testing.T) {
	tests := []struct {
		name      string
		collation string
		want      string
	}{
		{name: "a name", collation: "C", want: `  "c" text COLLATE "C" NOT NULL`},
		{name: "a dot in the name", collation: "en_US.utf8", want: `  "c" text COLLATE "en_US.utf8" NOT NULL`},
		{name: "a schema and a mixed-case name", collation: `"app"."MixedCase"`,
			want: `  "c" text COLLATE "app"."MixedCase" NOT NULL`},
		{name: "a quote in the name", collation: `"Mixed""Q"`, want: `  "c" text COLLATE "Mixed""Q" NOT NULL`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderPostgres(c, collatedTable(test.collation)), qt.Contains, test.want+"\n")
		})
	}
}

// TestPostgres_CreateTable_RefusesAMalformedCollation pins that a collation
// the renderer cannot spell is refused rather than written or dropped.
func TestPostgres_CreateTable_RefusesAMalformedCollation(t *testing.T) {
	c := qt.New(t)
	r := postgres.New()
	r.Reset()

	err := collatedTable(`"app"."C`).Accept(r)

	c.Assert(err, qt.ErrorMatches, `.*column c: invalid collation name: "app"."C has no closing quote`)
}

// TestPostgres_CreateTable_CockroachDBWritesNoColumnCollation is the control:
// the engines whose descriptor does not state Columns.Collations are handed
// no COLLATE, and the render reports the collation lost instead.
func TestPostgres_CreateTable_CockroachDBWritesNoColumnCollation(t *testing.T) {
	for _, dialect := range []string{platform.CockroachDB, platform.YugabyteDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			r := postgres.NewWithCapabilities(capability.ForDialect(dialect), dialect)
			r.Reset()

			c.Assert(collatedTable("C").Accept(r), qt.IsNil)

			c.Assert(r.Output(), qt.Not(qt.Contains), "COLLATE")
		})
	}
}

// TestPostgres_ModifyColumn_ChangesTheCollationThroughTheTypeClause pins how a
// collation changes: ALTER COLUMN ... TYPE with the column's COLLATE, and
// without one to give the column back its type's collation. A type change on
// a column that names a collation restates it, since a TYPE clause without
// COLLATE gives the column its new type's collation.
func TestPostgres_ModifyColumn_ChangesTheCollationThroughTheTypeClause(t *testing.T) {
	tests := []struct {
		name    string
		column  *ast.ColumnNode
		changed ast.ColumnProperties
		want    string
	}{
		{
			name:    "a collation set",
			column:  ast.NewColumn("c", "text").SetCollate("C"),
			changed: ast.ColumnProperties{Collation: true},
			want:    "ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE text COLLATE \"C\";\n\n",
		},
		{
			name:    "a collation removed",
			column:  ast.NewColumn("c", "text"),
			changed: ast.ColumnProperties{Collation: true},
			want:    "ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE text;\n\n",
		},
		{
			name:    "a schema's collation set",
			column:  ast.NewColumn("c", "varchar(20)").SetCollate(`"app"."MixedCase"`),
			changed: ast.ColumnProperties{Collation: true},
			want:    "ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE varchar(20) COLLATE \"app\".\"MixedCase\";\n\n",
		},
		{
			name:    "a type change keeps the collation",
			column:  ast.NewColumn("c", "varchar(40)").SetCollate("POSIX"),
			changed: ast.ColumnProperties{Type: true},
			want:    "ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE varchar(40) COLLATE \"POSIX\";\n\n",
		},
		{
			name:    "a type and a collation in one clause",
			column:  ast.NewColumn("c", "varchar(40)").SetCollate("C"),
			changed: ast.ColumnProperties{Type: true, Collation: true},
			want:    "ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE varchar(40) COLLATE \"C\";\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderPostgres(c, modifyColumn(test.column, test.changed, true)), qt.Equals, test.want)
		})
	}
}
