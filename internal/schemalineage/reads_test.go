package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemalineage"
)

// customersSchema is one table with three columns and one routine over it.
func customersSchema(kind, language, body string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "customers"}},
		Fields: []schemamodel.Field{
			{StructName: "C", Name: "id", Type: "INT"},
			{StructName: "C", Name: "email", Type: "TEXT"},
			{StructName: "C", Name: "country", Type: "TEXT"},
		},
		Functions: []schemamodel.Function{
			{Name: "r", Kind: kind, Language: language, Body: body},
		},
	}
}

// readTargets renders a result's reads as "table.column:statement".
func readTargets(result schemalineage.RoutineResult) []string {
	targets := make([]string, 0, len(result.Reads))
	for _, read := range result.Reads {
		targets = append(targets, read.Table+"."+read.Column+":"+read.Statement)
	}
	return targets
}

// TestDeriveRoutines_AnAssignedColumnIsWrittenAndNotRead is the distinction the
// whole read half turns on.
//
// In `SET country = 'CZ' WHERE id = 1` the column country is written and never
// read; id is read and never written. Counting every column a statement
// mentions would report country as depended upon, which is the opposite of what
// `lineage` is asked -- a column nothing reads is exactly the one it is safe to
// drop (stokaro/ptah#2394).
func TestDeriveRoutines_AnAssignedColumnIsWrittenAndNotRead(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(customersSchema("", "plpgsql",
		"BEGIN UPDATE customers SET country = 'CZ' WHERE id = 1; END;"), "postgres")

	c.Assert(readTargets(result), qt.DeepEquals, []string{"customers.id:update"})
	c.Assert(writeTargets(result), qt.DeepEquals, []string{"customers.country:update"})
}

// TestDeriveRoutines_AColumnAssignedFromItselfIsBoth is the other half of that
// distinction, and the one a position-blind rule gets wrong.
//
// `SET email = lower(email)` writes email and reads it. A rule that dropped
// every assigned column from the reads would lose the read; a rule that kept
// every mention would invent one in the test above. Only the position separates
// them.
func TestDeriveRoutines_AColumnAssignedFromItselfIsBoth(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(customersSchema("", "plpgsql",
		"BEGIN UPDATE customers SET email = lower(email) WHERE id = 1; END;"), "postgres")

	c.Assert(readTargets(result), qt.DeepEquals, []string{"customers.email:update", "customers.id:update"})
	c.Assert(writeTargets(result), qt.DeepEquals, []string{"customers.email:update"})
}

// TestDeriveRoutines_AReadInsideABranchIsReached matches what the write half
// already does.
//
// A SELECT inside an IF reads its columns. A walk that stopped at the top level
// would report the routine as reading nothing inside the branch.
func TestDeriveRoutines_AReadInsideABranchIsReached(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(customersSchema("", "plpgsql",
		"BEGIN IF true THEN SELECT email FROM customers WHERE id = 1; END IF; END;"), "postgres")

	c.Assert(readTargets(result), qt.DeepEquals, []string{"customers.email:select", "customers.id:select"})
}

// TestDeriveRoutines_TSQLReadsAreDerived is the other half of the dialect rule.
//
// A T-SQL variable carries an @ prefix and cannot collide with a column name,
// so the ambiguity that stops MySQL does not arise. Without this row the only
// evidence for the T-SQL arm is the MySQL refusal, and a rule that answered for
// PostgreSQL alone would pass that test unchanged.
//
// It reads whole only because stokaro/ptah#2451 stopped the splitter breaking
// an UPDATE at its SET clause; before that this statement arrived in two halves
// and neither was an update.
func TestDeriveRoutines_TSQLReadsAreDerived(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(customersSchema("procedure", "sql",
		"UPDATE customers SET country = 'CZ' WHERE id = 1;"), "sqlserver")

	c.Assert(readTargets(result), qt.DeepEquals, []string{"customers.id:update"})
	c.Assert(writeTargets(result), qt.DeepEquals, []string{"customers.country:update"})
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "the reads are those of its statements that name one table")
}

// TestDeriveRoutines_MySQLReadsAreRefusedWithTheirReason is the property that
// decides which dialects can be answered at all.
//
// MySQL resolves a declared variable ahead of a column of the same name, so an
// identifier in a MySQL body does not say which it is. PostgreSQL errors on the
// ambiguity by default and T-SQL prefixes variables with @, so both can be
// answered; MySQL cannot, and saying so is the only honest answer.
func TestDeriveRoutines_MySQLReadsAreRefusedWithTheirReason(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(customersSchema("procedure", "sql",
		"BEGIN UPDATE customers SET country = 'CZ' WHERE id = 1; END"), "mysql")

	c.Assert(result.Reads, qt.HasLen, 0)
	c.Assert(writeTargets(result), qt.DeepEquals, []string{"customers.country:update"})
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "a declared variable takes precedence over a column")
}

// TestDeriveRoutines_TwoTablesInScopeYieldNoRead is the boundary the view half
// already draws, asserted for a routine body.
//
// With a second table in scope an unqualified column cannot be attributed, and
// attributing it to the wrong table is worse than reporting nothing.
func TestDeriveRoutines_TwoTablesInScopeYieldNoRead(t *testing.T) {
	rows := []struct {
		name string
		body string
	}{
		{
			name: "a joined select",
			body: "BEGIN SELECT email FROM customers JOIN orders ON orders.id = customers.id; END;",
		},
		{
			name: "an update with a from clause",
			body: "BEGIN UPDATE customers SET country = o.country FROM orders o WHERE o.id = customers.id; END;",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			result := schemalineage.DeriveRoutines(customersSchema("", "plpgsql", row.body), "postgres")

			c.Assert(result.Reads, qt.HasLen, 0)
		})
	}
}

// TestDeriveRoutines_AColumnMentionedTwiceIsReadOnce keeps the list answering
// the question it was asked.
//
// "Does anything read this column" is a yes or no. A list that counted mentions
// would answer how often instead, and a caller filtering it would have to
// dedupe to get back to the question.
func TestDeriveRoutines_AColumnMentionedTwiceIsReadOnce(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(customersSchema("", "plpgsql",
		"BEGIN DELETE FROM customers WHERE id = 1 AND id < 9; END;"), "postgres")

	c.Assert(readTargets(result), qt.DeepEquals, []string{"customers.id:delete"})
}
