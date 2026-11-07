package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
)

// notNullTable is one table whose single column is NOT NULL, named when the
// caller passes a name and bare when it passes "".
func notNullTable(constraintName string) *ast.CreateTableNode {
	column := ast.NewColumn("a", "INTEGER").SetNotNull()
	column.SetNotNullConstraintName(constraintName)

	return ast.NewCreateTable("widget").AddColumn(column)
}

// nullableTableNamingItsNotNull is the inconsistent declaration: a constraint
// name on a column that has no such constraint.
func nullableTableNamingItsNotNull(constraintName string) *ast.CreateTableNode {
	column := ast.NewColumn("a", "INTEGER")
	column.SetNotNullConstraintName(constraintName)

	return ast.NewCreateTable("widget").AddColumn(column)
}

// primaryTableNamingItsNotNull is a primary-key column whose NOT NULL has a name,
// which PostgreSQL 18 keeps as it does on any other column.
func primaryTableNamingItsNotNull(constraintName string) *ast.CreateTableNode {
	column := ast.NewColumn("a", "INTEGER").SetNotNull().SetPrimary()
	column.SetNotNullConstraintName(constraintName)

	return ast.NewCreateTable("widget").AddColumn(column)
}

// addedNotNullColumn is one ALTER TABLE adding a NOT NULL column under the given name.
func addedNotNullColumn(table, column, constraintName string) *ast.AlterTableNode {
	return &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.AddColumnOperation{
			Column: ast.NewColumn(column, "INTEGER").SetNotNull().SetNotNullConstraintName(constraintName),
		}},
	}
}

// postgresCaps is a PostgreSQL 17 preset with the named-NOT-NULL answer under
// the caller's control, which is what separates the two targets: 17 and 18
// accept the identical syntax and only 18 keeps the name.
func postgresCaps(named bool) capability.Capabilities {
	return capability.Postgres17().With(capability.NamedNotNullConstraints, named)
}

// TestNamedNotNull_ATargetThatKeepsTheNameEmitsIt is the measured case.
//
// PostgreSQL 18 records one row per NOT NULL in pg_constraint with contype 'n',
// keyed to the column through conkey, and answers to the name (stokaro/ptah#2161).
func TestNamedNotNull_ATargetThatKeepsTheNameEmitsIt(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(true))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(notNullTable("c_keep"))

	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, `CONSTRAINT "c_keep" NOT NULL`)
}

// TestNamedNotNull_ATargetThatRecordsNothingRefuses is the other half, and the
// reason the key is about persistence rather than syntax.
//
// PostgreSQL 17 accepts `CONSTRAINT c NOT NULL` and stores nothing, so emitting
// the name there produces DDL that applies, reads back bare, and leaves every
// later comparison reporting a difference no apply can settle. It refuses
// rather than dropping the name, because a silent drop is that same drift with
// no diagnostic.
func TestNamedNotNull_ATargetThatRecordsNothingRefuses(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(false))
	c.Assert(err, qt.IsNil)

	_, renderErr := r.Render(notNullTable("c_keep"))

	c.Assert(renderErr, qt.IsNotNil)
	c.Assert(renderErr.Error(), qt.Contains, "does not keep a NOT NULL constraint name")
}

// TestNamedNotNull_ATargetThatRecordsNothingLeavesOutTheDefaultName takes the name a
// server gives a NOT NULL nobody named, which a read of PostgreSQL 18 reports on every
// column, as asking for nothing on a target that keeps no name. The last row is the
// same table on a target that keeps one, which writes the name as it was given.
func TestNamedNotNull_ATargetThatRecordsNothingLeavesOutTheDefaultName(t *testing.T) {
	const longColumn = "a_column_name_that_is_much_longer_than_fifty_four_bytes_x"
	tests := []struct {
		name string
		node ast.Node
		caps capability.Capabilities
		want string
	}{
		{
			name: "a created table",
			node: notNullTable("widget_a_not_null"),
			caps: postgresCaps(false),
			want: `"a" INTEGER NOT NULL`,
		},
		{
			name: "a created table in a schema",
			node: ast.NewCreateTable("app.widget").AddColumn(ast.NewColumn("a", "INTEGER").SetNotNull().SetNotNullConstraintName("widget_a_not_null")),
			caps: postgresCaps(false),
			want: `"a" INTEGER NOT NULL`,
		},
		{
			name: "an added column",
			node: addedNotNullColumn("widget", "a", "widget_a_not_null"),
			caps: postgresCaps(false),
			want: `ALTER TABLE "widget" ADD COLUMN "a" INTEGER NOT NULL;`,
		},
		{
			name: "an added column in a schema",
			node: addedNotNullColumn("app.widget", "a", "widget_a_not_null"),
			caps: postgresCaps(false),
			want: `ALTER TABLE "app"."widget" ADD COLUMN "a" INTEGER NOT NULL;`,
		},
		{
			name: "a name the server cuts to 63 bytes",
			node: addedNotNullColumn("t", longColumn, "t_a_column_name_that_is_much_longer_than_fifty_four_by_not_null"),
			caps: postgresCaps(false),
			want: `ALTER TABLE "t" ADD COLUMN "` + longColumn + `" INTEGER NOT NULL;`,
		},
		{
			name: "a primary-key column",
			node: primaryTableNamingItsNotNull("widget_a_not_null"),
			caps: postgresCaps(false),
			want: `"a" INTEGER PRIMARY KEY NOT NULL`,
		},
		{
			name: "a target that keeps the name",
			node: addedNotNullColumn("widget", "a", "widget_a_not_null"),
			caps: postgresCaps(true),
			want: `ALTER TABLE "widget" ADD COLUMN "a" INTEGER CONSTRAINT "widget_a_not_null" NOT NULL;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRendererWithCapabilities("postgres", test.caps)
			c.Assert(err, qt.IsNil)

			sql, renderErr := r.Render(test.node)

			c.Assert(renderErr, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestNamedNotNull_ATargetThatRecordsNothingRefusesEveryOtherName keeps the refusal for
// a name the target would lose: one the server numbered, the default of another table
// or column, the uncut spelling of a name the server cuts, and the contradiction a
// default spelling must not hide.
func TestNamedNotNull_ATargetThatRecordsNothingRefusesEveryOtherName(t *testing.T) {
	const longColumn = "a_column_name_that_is_much_longer_than_fifty_four_bytes_x"
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a name the server numbered",
			node: addedNotNullColumn("widget", "a", "widget_a_not_null1"),
			want: "does not keep a NOT NULL constraint name",
		},
		{
			name: "the default name of another table",
			node: addedNotNullColumn("widget", "a", "other_a_not_null"),
			want: "does not keep a NOT NULL constraint name",
		},
		{
			name: "the default name of another column",
			node: addedNotNullColumn("widget", "a", "widget_b_not_null"),
			want: "does not keep a NOT NULL constraint name",
		},
		{
			name: "a long name no server gives",
			node: addedNotNullColumn("t", longColumn, "t_"+longColumn+"_not_null"),
			want: "does not keep a NOT NULL constraint name",
		},
		{
			name: "a default name on a nullable column",
			node: nullableTableNamingItsNotNull("widget_a_not_null"),
			want: "names no constraint",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(false))
			c.Assert(err, qt.IsNil)

			sql, renderErr := r.Render(test.node)

			c.Assert(renderErr, qt.ErrorMatches, ".*"+test.want+".*")
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestNamedNotNull_AModificationStatesItsName writes the constraint under the name the
// operation carries, where SET NOT NULL would leave it to the server, and fills a NULL
// row from the column's default ahead of it. A name on the column is not a request: a
// read of PostgreSQL 18 puts one on every NOT NULL.
func TestNamedNotNull_AModificationStatesItsName(t *testing.T) {
	declaredDefault := ast.NewColumn("a", "TEXT").SetNotNull().SetDefault("x")
	tests := []struct {
		name string
		op   *ast.ModifyColumnOperation
		want string
	}{
		{
			name: "a name",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("a", "TEXT").SetNotNull(), NotNullConstraintName: "nn"},
			want: `-- POSTGRES: ADD CONSTRAINT "nn" NOT NULL fails if any row of "widget" holds NULL in "a"; the column declares no default to fill it with.
ALTER TABLE "widget" ADD CONSTRAINT "nn" NOT NULL "a";`,
		},
		{
			name: "a name beside a declared default",
			op:   &ast.ModifyColumnOperation{Column: declaredDefault, NotNullConstraintName: "nn"},
			want: `UPDATE "widget" SET "a" = 'x' WHERE "a" IS NULL;
ALTER TABLE "widget" ADD CONSTRAINT "nn" NOT NULL "a";`,
		},
		{
			name: "a name a plan does not fill for",
			op:   &ast.ModifyColumnOperation{Column: declaredDefault, NotNullConstraintName: "nn", OmitNullBackfill: true},
			want: `-- POSTGRES: ADD CONSTRAINT "nn" NOT NULL fails if any row of "widget" holds NULL in "a"; this plan does not fill it with the column's default.
ALTER TABLE "widget" ADD CONSTRAINT "nn" NOT NULL "a";`,
		},
		{
			name: "no name, with one on the column",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("a", "TEXT").SetNotNull().SetNotNullConstraintName("widget_a_not_null")},
			want: `-- POSTGRES: SET NOT NULL fails if any row of "widget" holds NULL in "a"; the column declares no default to fill it with.
ALTER TABLE "widget" ALTER COLUMN "a" SET NOT NULL;`,
		},
		{
			name: "a column made nullable",
			op:   &ast.ModifyColumnOperation{Column: ast.NewColumn("a", "TEXT"), NotNullConstraintName: "nn"},
			want: `ALTER TABLE "widget" ALTER COLUMN "a" DROP NOT NULL;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(true))
			c.Assert(err, qt.IsNil)
			test.op.HasChanged = true
			test.op.Changed = ast.ColumnProperties{Nullability: true}

			sql, renderErr := r.Render(&ast.AlterTableNode{Name: "widget", Operations: []ast.AlterOperation{test.op}})

			c.Assert(renderErr, qt.IsNil)
			c.Assert(strings.TrimSpace(sql), qt.Equals, test.want)
		})
	}
}

// TestNamedNotNull_AParsedClauseRendersAsItIsWritten renders what the parser reads of
// `ADD CONSTRAINT name NOT NULL column`, which keeps the name, beside the action without
// one.
func TestNamedNotNull_AParsedClauseRendersAsItIsWritten(t *testing.T) {
	tests := []struct {
		name string
		op   *ast.AlterColumnOperation
		want string
	}{
		{
			name: "a name",
			op:   &ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetNotNull, NotNullConstraintName: "nn"},
			want: `ALTER TABLE "widget" ADD CONSTRAINT "nn" NOT NULL "a";`,
		},
		{
			name: "no name",
			op:   &ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetNotNull},
			want: `ALTER TABLE "widget" ALTER COLUMN "a" SET NOT NULL;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(true))
			c.Assert(err, qt.IsNil)

			sql, renderErr := r.Render(&ast.AlterTableNode{Name: "widget", Operations: []ast.AlterOperation{test.op}})

			c.Assert(renderErr, qt.IsNil)
			c.Assert(strings.TrimSpace(sql), qt.Equals, test.want)
		})
	}
}

// TestNamedNotNull_AnUnnamedConstraintIsUnaffected is the control for the test
// above: without it, a renderer that refused every NOT NULL on a target without
// the key would pass.
func TestNamedNotNull_AnUnnamedConstraintIsUnaffected(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(false))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(notNullTable(""))

	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, "NOT NULL")
	c.Assert(sql, qt.Not(qt.Contains), "CONSTRAINT")
}

// TestNamedNotNull_ANameOnANullableColumnNamesNothing refuses a declaration
// that contradicts itself, rather than rendering a nullable column and
// reporting success.
func TestNamedNotNull_ANameOnANullableColumnNamesNothing(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(true))
	c.Assert(err, qt.IsNil)

	_, renderErr := r.Render(nullableTableNamingItsNotNull("c_keep"))

	c.Assert(renderErr, qt.IsNotNil)
	c.Assert(renderErr.Error(), qt.Contains, "names no constraint")
}

// TestNamedNotNull_APrimaryKeyColumnKeepsItsName writes the name on a target that keeps
// one. Measured on PostgreSQL 18.6, `id int PRIMARY KEY CONSTRAINT idnn NOT NULL` holds
// idnn beside the key's own constraint, and a column renamed on the server holds the
// name of the column it was.
func TestNamedNotNull_APrimaryKeyColumnKeepsItsName(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(true))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(primaryTableNamingItsNotNull("c_pk"))

	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, `"a" INTEGER PRIMARY KEY CONSTRAINT "c_pk" NOT NULL`)
}

// TestNamedNotNull_APrimaryKeyColumnOnATargetThatKeepsNoNameIsRefused holds the column to
// the refusal any other column meets, because the target would lose the name.
func TestNamedNotNull_APrimaryKeyColumnOnATargetThatKeepsNoNameIsRefused(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(false))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(primaryTableNamingItsNotNull("c_pk"))

	c.Assert(renderErr, qt.ErrorMatches, ".*does not keep a NOT NULL constraint name .c_pk.*")
	c.Assert(sql, qt.Equals, "")
}

// TestNamedNotNull_AnUnnamedPrimaryKeyColumnIsUnaffected is the control: with no name to
// state, the column writes no CONSTRAINT clause.
func TestNamedNotNull_AnUnnamedPrimaryKeyColumnIsUnaffected(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRendererWithCapabilities("postgres", postgresCaps(true))
	c.Assert(err, qt.IsNil)

	sql, renderErr := r.Render(primaryTableNamingItsNotNull(""))

	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, "PRIMARY KEY")
	c.Assert(sql, qt.Not(qt.Contains), "CONSTRAINT")
}
