package mssql

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
)

// TestRenderer_CreateSequenceUsesTSQLClauseOrder pins the statement shape whose
// every clause was measured against SQL Server 2025 (RTM-CU8), 17.0.4075.5.
//
// The order is PostgreSQL's, and that is a measurement rather than a guess: the
// engine accepts `AS ... INCREMENT BY ... MINVALUE ... MAXVALUE ... START WITH
// ... CACHE ... CYCLE`, so the shared ordering needs no T-SQL variant.
func TestRenderer_CreateSequenceUsesTSQLClauseOrder(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateSequence("order_number_seq").
		SetSchema("app").
		SetAs("bigint").
		SetStart(1000).
		SetIncrement(5).
		SetMinValue(1).
		SetMaxValue(9999).
		SetCache(20).
		SetCycle(true)

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE SEQUENCE [app].[order_number_seq] AS bigint "+
		"INCREMENT BY 5 MINVALUE 1 MAXVALUE 9999 START WITH 1000 CACHE 20 CYCLE;\n")
}

// TestRenderer_CreateSequenceGuardsWithAnExistenceTest pins the answer to the
// clause T-SQL does not have.
//
// `CREATE SEQUENCE IF NOT EXISTS` is `Incorrect syntax near the keyword 'IF'`,
// so the guard becomes the sys.sequences existence test this renderer already
// uses for CREATE SCHEMA. Rendering the bare CREATE instead would turn a
// re-runnable statement into one that fails the second time.
func TestRenderer_CreateSequenceGuardsWithAnExistenceTest(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateSequence("order_number_seq").SetAs("bigint").SetIfNotExists()

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, ""+
		"IF NOT EXISTS (SELECT 1 FROM sys.sequences sq JOIN sys.schemas sc "+
		"ON sc.schema_id = sq.schema_id WHERE sc.name = 'dbo' AND sq.name = 'order_number_seq')\n"+
		"    EXEC('CREATE SEQUENCE [order_number_seq] AS bigint');\n")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE SEQUENCE IF NOT EXISTS")
}

// TestRenderer_SequenceCacheOfZeroBecomesNoCache pins the second clause the
// engine refuses: `CACHE 0` is `The cache size for sequence object must be
// greater than 0`, and NO CACHE is what a cache of zero means.
func TestRenderer_SequenceCacheOfZeroBecomesNoCache(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateSequence("s").SetCache(0)

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE SEQUENCE [s] NO CACHE;\n")
}

// TestRenderer_SequenceCacheOfOneIsStillACacheSize is the control. A renderer
// that answered NO CACHE for every declared cache would satisfy the row above
// and would never emit a cache size at all.
func TestRenderer_SequenceCacheOfOneIsStillACacheSize(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateSequence("s").SetCache(1)

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE SEQUENCE [s] CACHE 1;\n")
}

// TestRenderer_CreateSequenceReportsOwnedBy pins that the clause T-SQL has no
// form for is named rather than dropped.
//
// The association is genuinely not made. Saying so is the difference between a
// limitation an author can see and one they discover from a column that never
// advances.
func TestRenderer_CreateSequenceReportsOwnedBy(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateSequence("s").SetOwnedBy("orders.id")

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `OWNED BY "orders.id"`)
	c.Assert(sql, qt.Contains, "the association is not made")
	c.Assert(sql, qt.Contains, "CREATE SEQUENCE [s];")
}

// TestRenderer_AlterSequenceSpellsRestartWith pins the third refusal:
// `ALTER SEQUENCE ... START WITH` is `Argument 'START WITH' cannot be used in
// an ALTER SEQUENCE statement`. T-SQL spells it RESTART WITH, which also resets
// the current value.
func TestRenderer_AlterSequenceSpellsRestartWith(t *testing.T) {
	c := qt.New(t)
	node := ast.NewAlterSequence("s").SetStart(5).SetIncrement(2)

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "ALTER SEQUENCE [s] RESTART WITH 5 INCREMENT BY 2;\n")
	// "RESTART WITH" contains "START WITH", so the negative has to name the
	// clause position rather than the keyword.
	c.Assert(sql, qt.Not(qt.Contains), "[s] START WITH")
}

// TestRenderer_AlterSequenceReportsATypeChangeItCannotMake pins the fourth:
// `ALTER SEQUENCE ... AS <type>` is `Argument 'AS' cannot be used in an ALTER
// SEQUENCE statement`.
//
// A plan that silently omitted the option an author changed would report
// success and leave the sequence as it was.
func TestRenderer_AlterSequenceReportsATypeChangeItCannotMake(t *testing.T) {
	c := qt.New(t)
	node := ast.NewAlterSequence("s").SetAs("int").SetIncrement(2)

	sql, err := New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "cannot change its type in place")
	c.Assert(sql, qt.Not(qt.Contains), "AS int")
	c.Assert(sql, qt.Contains, "ALTER SEQUENCE [s] INCREMENT BY 2;")
}

// TestRenderer_AlterSequenceWithNoOptionsRendersNothing pins that an ALTER with
// nothing to change is no statement rather than a syntax error.
func TestRenderer_AlterSequenceWithNoOptionsRendersNothing(t *testing.T) {
	c := qt.New(t)

	sql, err := New().Render(ast.NewAlterSequence("s"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "")
}

// TestRenderer_DropSequenceKeepsIfExistsAndReportsCascade pins both halves of
// the drop: IF EXISTS is accepted, CASCADE is `Incorrect syntax near the
// keyword 'CASCADE'` and has nothing to render in its place.
func TestRenderer_DropSequenceKeepsIfExistsAndReportsCascade(t *testing.T) {
	c := qt.New(t)

	plain, err := New().Render(ast.NewDropSequence("s").SetIfExists())
	c.Assert(err, qt.IsNil)
	c.Assert(plain, qt.Equals, "DROP SEQUENCE IF EXISTS [s];\n")

	cascaded, err := New().Render(ast.NewDropSequence("s").SetIfExists().SetCascade())
	c.Assert(err, qt.IsNil)
	c.Assert(cascaded, qt.Contains, "T-SQL has no clause for")
	c.Assert(cascaded, qt.Contains, "DROP SEQUENCE IF EXISTS [s];")
	c.Assert(cascaded, qt.Not(qt.Contains), "CASCADE;")
}

// TestRenderer_SequenceStillRefusesWhenTheCapabilityIsOff is the gate's inverse
// control: the emission is reached because the preset claims the key, not
// because the visitor stopped asking.
//
// Without this, a renderer that ignored the capability entirely would look
// identical on every test above.
func TestRenderer_SequenceStillRefusesWhenTheCapabilityIsOff(t *testing.T) {
	c := qt.New(t)
	withoutSequences := capability.SQLServer2022().With(capability.Sequences, false)

	sql, err := NewWithCapabilities(withoutSequences).Render(ast.NewCreateSequence("s").SetAs("bigint"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "is not generated for this target; skipped.")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE SEQUENCE [s]")
}
