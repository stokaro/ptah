package roundingcheck_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/roundingcheck"
	"ptah.run/internal/roundingscan"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

var (
	prices = roundingscan.Change{Table: "public.prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"}
	// events names its column with a directive in it, which a comment must not
	// carry as one.
	events = roundingscan.Change{
		Table: "events", Column: "at +ptah no_transaction", HeldType: "timestamp(6)", DeclaredType: "timestamp(0)", Temporal: true,
	}
)

// TestChecked_HappyPath writes, for each change, the comment and the check a migration
// file carries, in the order of the changes.
func TestChecked_HappyPath(t *testing.T) {
	c := qt.New(t)

	block, err := roundingcheck.Checked("postgres", []roundingscan.Change{prices, events})

	c.Assert(err, qt.IsNil)
	c.Assert(block, qt.Equals, `-- column "amount" of table "public.prices" goes from numeric(8,3) to numeric(8,1), `+
		`which keeps fewer digits after the point; the check refuses the migration while a row holds a value the server `+
		`would round or cut
-- +ptah check kind=rounded-values name="public.prices.amount" assert="SELECT CASE WHEN EXISTS (SELECT 1 FROM `+
		`""public"".""prices"" WHERE CAST(""amount"" AS numeric) IS DISTINCT FROM CAST(""amount"" AS numeric(8,1))) `+
		`THEN 0 ELSE 1 END"
-- column "at + ptah no_transaction" of table "events" goes from timestamp(6) to timestamp(0), which keeps fewer `+
		`digits after the point; the check refuses the migration while a row holds a value the server would round or cut
-- +ptah check kind=rounded-values name="events.at +ptah no_transaction" assert="SELECT CASE WHEN EXISTS (SELECT 1 `+
		`FROM ""events"" WHERE CAST(""at +ptah no_transaction"" AS timestamp) IS DISTINCT FROM `+
		`CAST(""at +ptah no_transaction"" AS timestamp(0))) THEN 0 ELSE 1 END"
`)
}

// TestChecked_ReadsBackAsTheChecksTheMigratorRuns parses the block the way a run does:
// each change is one check of kind rounded-values, named after its column, whose
// assertion is the one the scan builds, and the block declares no other directive,
// whatever the names hold.
func TestChecked_ReadsBackAsTheChecksTheMigratorRuns(t *testing.T) {
	c := qt.New(t)
	block, err := roundingcheck.Checked("postgres", []roundingscan.Change{prices, events})
	c.Assert(err, qt.IsNil)
	pricesAssert, err := roundingscan.Assertion("postgres", prices)
	c.Assert(err, qt.IsNil)
	eventsAssert, err := roundingscan.Assertion("postgres", events)
	c.Assert(err, qt.IsNil)

	checks, err := migrator.ParseChecks(block+"ALTER TABLE t ALTER COLUMN c TYPE int;\n", "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.DeepEquals, []migrator.Check{
		{Name: "public.prices.amount", Assert: pricesAssert, OnFail: migrator.OnFailAbort, Phase: migrator.CheckPhaseBefore,
			Kind: migrator.CheckKindRoundedValues},
		{Name: "events.at +ptah no_transaction", Assert: eventsAssert, OnFail: migrator.OnFailAbort,
			Phase: migrator.CheckPhaseBefore, Kind: migrator.CheckKindRoundedValues},
	})
	c.Assert(migrationfile.ParseDirectivesForDialect(block, "postgres"), qt.HasLen, 0)
}

// TestChecked_WritesNothingWithoutAChange holds a plan with no such change to a file
// with nothing added.
func TestChecked_WritesNothingWithoutAChange(t *testing.T) {
	c := qt.New(t)

	block, err := roundingcheck.Checked("postgres", nil)

	c.Assert(err, qt.IsNil)
	c.Assert(block, qt.Equals, "")
}

// TestChecked_FailurePath refuses a change the scan cannot read, so a type spelling
// outside the grammar never reaches a file, and writes nothing for the changes before it.
func TestChecked_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		change  roundingscan.Change
		want    string
	}{
		{"SQL in a type", "postgres",
			roundingscan.Change{Table: "prices", Column: "amount", HeldType: "numeric(8,3)", DeclaredType: `numeric(8,1)"; DROP TABLE prices; --`},
			`read the values of column "amount" of table "prices": the declared type is not a type spelling: .*`},
		// The first change is the one refused, so nothing after it is read.
		{"an engine that reads no value", "oracle",
			roundingscan.Change{Table: "PRICES", Column: "AMOUNT", HeldType: "NUMBER(8,3)", DeclaredType: "NUMBER(8,1)"},
			`read the values of column "amount" of table "public.prices": dialect "oracle" reads no value of such a change`},
		{"a line break in a column name", "postgres",
			roundingscan.Change{Table: "prices", Column: "amount\nDROP TABLE prices;", HeldType: "numeric(8,3)", DeclaredType: "numeric(8,1)"},
			`write the check of column "amount\\nDROP TABLE prices;" of table "prices": a \+ptah check directive is one line, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			block, err := roundingcheck.Checked(test.dialect, []roundingscan.Change{prices, test.change})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(block, qt.Equals, "")
		})
	}
}

// TestAllowed_HappyPath writes the note a file generated with the rounding allowed
// carries in place of each check, and no directive.
func TestAllowed_HappyPath(t *testing.T) {
	c := qt.New(t)

	block := roundingcheck.Allowed([]roundingscan.Change{prices, events})

	c.Assert(block, qt.Equals, `-- column "amount" of table "public.prices" goes from numeric(8,3) to numeric(8,1), `+
		`which keeps fewer digits after the point; the rounding was allowed when this file was generated, so no check `+
		`reads the rows and the server rounds or cuts each value to the new digits
-- column "at + ptah no_transaction" of table "events" goes from timestamp(6) to timestamp(0), which keeps fewer `+
		`digits after the point; the rounding was allowed when this file was generated, so no check reads the rows and `+
		`the server rounds or cuts each value to the new digits
`)
	checks, err := migrator.ParseChecks(block, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.HasLen, 0)
	c.Assert(roundingcheck.Allowed(nil), qt.Equals, "")
}

// TestName names a check after its column, qualified as the table is, with a part that
// needs quoting quoted.
func TestName(t *testing.T) {
	tests := []struct {
		name   string
		change roundingscan.Change
		want   string
	}{
		{"a schema", roundingscan.Change{Table: "public.prices", Column: "amount"}, "public.prices.amount"},
		{"no schema", roundingscan.Change{Table: "prices", Column: "amount"}, "prices.amount"},
		{"a dot in a column", roundingscan.Change{Table: "prices", Column: "a.b"}, `prices."a.b"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(roundingcheck.Name(test.change), qt.Equals, test.want)
		})
	}
}
