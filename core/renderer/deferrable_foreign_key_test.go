package renderer_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// deferrableTable is the one node every row below renders: a child table whose
// foreign key declares the deferral, so the only thing that differs between
// rows is the target.
func deferrableTable(deferrable bool, initially string) ast.Node {
	table := ast.NewCreateTable("child")
	table.AddColumn(ast.NewColumn("parent_id", "int"))
	table.AddConstraint(ast.NewForeignKeyConstraint("fk_child_parent", []string{"parent_id"}, &ast.ForeignKeyRef{
		Table:      "parent",
		Column:     "id",
		Name:       "fk_child_parent",
		Deferrable: deferrable,
		Initially:  initially,
	}))
	return table
}

// TestDeferrableForeignKey_EveryDialectAnswersItsMeasurement pins one cell per
// dialect against what that engine actually does.
//
// stokaro/ptah#1624 added DEFERRABLE to the model, and the interesting part is
// that the PostgreSQL family splits on it: CockroachDB v26.2.5 answers
// `unimplemented: this syntax` to every form, including NOT DEFERRABLE, while
// PostgreSQL 18.4 and YugabyteDB 2026.1 accept the clause and report
// condeferrable and condeferred true. Spanner's PostgreSQL interface answers
// `<DEFERRABLE> constraints are not supported`.
//
// A target that cannot host one is REFUSED rather than rendered without the
// clause. Dropping it would emit a constraint that rejects exactly the writes
// the author deferred the check for -- at apply time, on data, rather than here
// on a line of DDL.
func TestDeferrableForeignKey_EveryDialectAnswersItsMeasurement(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{name: "postgres emits the clause", dialect: "postgres", wantSQL: "DEFERRABLE INITIALLY DEFERRED"},
		{name: "yugabytedb emits the clause", dialect: "yugabytedb", wantSQL: "DEFERRABLE INITIALLY DEFERRED"},
		{name: "sqlite emits the clause", dialect: "sqlite", wantSQL: "DEFERRABLE INITIALLY DEFERRED"},
		{name: "cockroachdb refuses it", dialect: "cockroachdb"},
		{name: "spanner refuses it", dialect: "spanner"},
		{name: "mysql refuses it", dialect: "mysql"},
		{name: "mariadb refuses it", dialect: "mariadb"},
		{name: "sqlserver refuses it", dialect: "sqlserver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(test.dialect, deferrableTable(true, "deferred"))

			c.Assert(errorText1624(err), qt.Contains, refusalFragment(test.wantSQL))
			c.Assert(sql, qt.Contains, test.wantSQL)
			// The arithmetic half: qt.Contains with "" passes on any string, so
			// without this a row expecting the clause and getting a refusal
			// would still be green.
			c.Assert(err == nil, qt.Equals, test.wantSQL != "",
				qt.Commentf("err=%v sql=%s", err, sql))
		})
	}
}

// TestDeferrableForeignKey_TimingSpellings pins the three shapes the clause
// takes, on the one dialect that hosts them.
func TestDeferrableForeignKey_TimingSpellings(t *testing.T) {
	tests := []struct {
		name       string
		deferrable bool
		initially  string
		want       string
	}{
		{
			// DEFERRABLE alone is legal: the check CAN be deferred and still
			// runs immediately by default, which is why Deferrable and
			// Initially are separate fields rather than one tri-state.
			name:       "deferrable without a timing",
			deferrable: true,
			want:       "DEFERRABLE",
		},
		{name: "initially deferred", deferrable: true, initially: "deferred", want: "DEFERRABLE INITIALLY DEFERRED"},
		{name: "initially immediate", deferrable: true, initially: "immediate", want: "DEFERRABLE INITIALLY IMMEDIATE"},
		{
			// The timing implies the clause, so a model carrying only the
			// timing still renders a legal constraint rather than a bare
			// INITIALLY.
			name:      "a timing alone implies deferrable",
			initially: "deferred",
			want:      "DEFERRABLE INITIALLY DEFERRED",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", deferrableTable(test.deferrable, test.initially))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestDeferrableForeignKey_UnchangedWithoutTheClause is the non-interference
// control: a foreign key that asks for no deferral renders exactly as it did
// before, on every dialect including the ones that refuse the clause.
func TestDeferrableForeignKey_UnchangedWithoutTheClause(t *testing.T) {
	for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb", "spanner", "sqlite", "mysql", "mariadb", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, deferrableTable(false, ""))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "fk_child_parent")
			c.Assert(strings.ToUpper(sql), qt.Not(qt.Contains), "DEFERRABLE")
		})
	}
}

// TestDeferrableForeignKey_RefusalNamesTheConstraint keeps the refusal
// actionable: an operator needs to know which constraint to change, not only
// that something in the schema wanted deferral.
func TestDeferrableForeignKey_RefusalNamesTheConstraint(t *testing.T) {
	c := qt.New(t)

	_, err := renderer.RenderSQL("mysql", deferrableTable(true, "deferred"))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fk_child_parent")
}

// TestDeferrableForeignKey_CapabilityDecidesRatherThanTheDialectName pins that
// the gate reads the capability set and not a list of dialect names: a
// PostgreSQL renderer handed a set without the key refuses, and one handed a
// set with it emits.
func TestDeferrableForeignKey_CapabilityDecidesRatherThanTheDialectName(t *testing.T) {
	c := qt.New(t)

	without, withoutErr := renderer.NewRendererWithCapabilities(
		"postgres", capability.Postgres17().With(capability.DeferrableConstraints, false))
	c.Assert(withoutErr, qt.IsNil)
	_, err := without.Render(deferrableTable(true, "deferred"))
	c.Assert(err, qt.IsNotNil)

	with, withErr := renderer.NewRendererWithCapabilities(
		"cockroachdb", capability.CockroachDB26().With(capability.DeferrableConstraints, true))
	c.Assert(withErr, qt.IsNil)
	sql, renderErr := with.Render(deferrableTable(true, "deferred"))
	c.Assert(renderErr, qt.IsNil)
	c.Assert(sql, qt.Contains, "DEFERRABLE INITIALLY DEFERRED")
}

// errorText1624 keeps the loop bodies branch-free: a nil error reads as the
// empty string, which is what the rows expecting DDL carry.
func errorText1624(err error) string {
	texts := map[bool]func() string{
		true:  func() string { return "" },
		false: func() string { return fmt.Sprint(err) },
	}
	return texts[err == nil]()
}

// refusalFragment is the sentence a refusing row expects and the empty string a
// rendering row expects, derived from the same column so the table carries one
// expectation per row rather than two that can disagree.
func refusalFragment(wantSQL string) string {
	fragments := map[bool]string{
		true:  "",
		false: "does not support DEFERRABLE foreign keys",
	}
	return fragments[wantSQL != ""]
}
