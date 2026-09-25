package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

// ratesReaders are routines that read the table rates: default_pct directly,
// and has_rate through rate_of. seed_pct reads settings, for a table whose own
// default makes it wait. Callers are listed before their callees and seed_pct
// last, so input order alone cannot put anything in the right place.
var ratesReaders = []schemamodel.Function{
	{Name: "has_rate", Language: "sql", Parameters: "c text", Returns: "boolean", Body: "SELECT rate_of(c) IS NOT NULL"},
	{Name: "rate_of", Language: "sql", Parameters: "c text", Returns: "integer", Body: "SELECT pct FROM rates WHERE code = c"},
	{Name: "default_pct", Language: "sql", Returns: "integer", Body: "SELECT coalesce((SELECT pct FROM rates WHERE code = 'std'), 0)"},
	{Name: "unused", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM rates"},
	{Name: "seed_pct", Language: "sql", Returns: "integer", Body: "SELECT max(pct) FROM settings"},
}

var ratesCallGraph = map[string][]string{"has_rate": {"rate_of"}}

// TestTablesWithRoutinesForCreate_PlacesACalledRoutineBetweenTables pins the
// one order PostgreSQL accepts for a column default or a CHECK calling a
// routine that reads another new table: that table, the routine, then the
// table that calls it (stokaro/ptah#3635). The rows vary where the call comes
// from.
func TestTablesWithRoutinesForCreate_PlacesACalledRoutineBetweenTables(t *testing.T) {
	tests := []struct {
		name   string
		tables []deporder.TableCreation
		want   []deporder.TableStep
	}{
		{
			name: "a column default, the calling table listed first",
			tables: []deporder.TableCreation{
				{Name: "invoices", Expressions: "default_pct()"},
				{Name: "rates"},
			},
			want: []deporder.TableStep{
				{Name: "rates"},
				{Name: "default_pct", Routine: true},
				{Name: "invoices"},
			},
		},
		{
			name: "a CHECK calling a routine that calls the reader",
			tables: []deporder.TableCreation{
				{Name: "rates"},
				{Name: "invoices", Expressions: "has_rate(code)"},
			},
			want: []deporder.TableStep{
				{Name: "rates"},
				{Name: "rate_of", Routine: true},
				{Name: "has_rate", Routine: true},
				{Name: "invoices"},
			},
		},
		{
			name: "a routine waits for a table that itself waits for a routine",
			tables: []deporder.TableCreation{
				{Name: "invoices", Expressions: "default_pct()"},
				{Name: "rates", Expressions: "seed_pct()"},
				{Name: "settings"},
			},
			want: []deporder.TableStep{
				{Name: "settings"},
				{Name: "seed_pct", Routine: true},
				{Name: "rates"},
				{Name: "default_pct", Routine: true},
				{Name: "invoices"},
			},
		},
		{
			name: "an existing table gaining a column creates nothing itself",
			tables: []deporder.TableCreation{
				{Name: "rates"},
				{Name: "orders", Expressions: "default_pct()", Existing: true},
			},
			want: []deporder.TableStep{
				{Name: "rates"},
				{Name: "default_pct", Routine: true},
			},
		},
		{
			name: "a routine no table calls stays out",
			tables: []deporder.TableCreation{
				{Name: "invoices", Expressions: "'default_pct'"},
				{Name: "rates"},
			},
			want: []deporder.TableStep{
				{Name: "invoices"},
				{Name: "rates"},
			},
		},
		{
			name: "a routine reading the table that calls it cannot be ordered",
			tables: []deporder.TableCreation{
				{Name: "rates", Expressions: "default_pct()"},
			},
			want: []deporder.TableStep{
				{Name: "rates"},
				{Name: "default_pct", Routine: true},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := deporder.TablesWithRoutinesForCreate(test.tables, ratesReaders, ratesCallGraph, platform.Postgres)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestTableExpressions_CarriesEveryClauseThatCanCallARoutine pins the text the
// ordering reads a table's calls from: a routine named in any of these clauses
// runs when the table is created or a row is written.
func TestTableExpressions_CarriesEveryClauseThatCanCallARoutine(t *testing.T) {
	c := qt.New(t)

	got := deporder.TableExpressions(
		[]string{"table_check()"},
		[]schemamodel.Field{{
			Default:             "literal_default",
			DefaultExpr:         "default_expr()",
			Check:               "column_check(v)",
			GeneratedExpression: "generated(v)",
			UpdateExpression:    "on_update()",
		}},
		[]schemamodel.Constraint{{CheckExpression: "constraint_check(v)", ExcludeElements: "exclude_element(v) WITH ="}},
	)

	for _, expression := range []string{
		"table_check()", "literal_default", "default_expr()", "column_check(v)", "generated(v)",
		"on_update()", "constraint_check(v)", "exclude_element(v) WITH =",
	} {
		c.Assert(got, qt.Contains, expression)
	}
}
