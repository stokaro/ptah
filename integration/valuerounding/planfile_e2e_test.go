//go:build integration

package valuerounding_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A saved plan's rounding records are read before the plan is shown for
// approval: the scan converts each recorded column to the recorded type, on
// --dry-run too. Anyone can edit a plan file, so a record is refused at decode
// unless its types fit the type grammar, the pair keeps fewer digits on the
// plan's dialect, and its column and declared type agree with the parsed ALTER.

// sqlServerPrices is the table the plans below lower the scale of, with a row
// that fits, and the desired schema.
const (
	sqlServerPrices        = "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,3))"
	sqlServerPricesRow     = "INSERT INTO prices VALUES (4, 1.5)"
	sqlServerPricesDesired = "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,1));\n"
	// sqlServerPricesHeld reads the column's type, the row, and whether a
	// table named pwned exists.
	sqlServerPricesHeld = "SELECT CONCAT(TYPE_NAME(c.system_type_id), '(', c.precision, ',', c.scale, ')|', " +
		"(SELECT CONVERT(varchar(40), amount) FROM prices), '|', (SELECT COUNT(*) FROM sys.tables WHERE name = 'pwned')) " +
		"FROM sys.columns AS c WHERE c.object_id = OBJECT_ID('prices') AND c.name = 'amount'"
)

// savedPlan saves the plan that lowers the scale of prices on target.
func savedPlan(c *qt.C, target string) string {
	c.Helper()
	plan := filepath.Join(c.TempDir(), "rounding.plan.json")
	planned := ptah(c, "schema", "plan", "--db-url", target, "--schema-file", schemaFile(c, sqlServerPricesDesired),
		"--save", "--output", plan)
	c.Assert(planned.ExitCode, qt.Equals, 0, outcome(planned))
	return plan
}

// editRounding sets field of the plan's first rounding record to value.
func editRounding(c *qt.C, plan, field, value string) {
	c.Helper()
	contents, err := os.ReadFile(plan)
	c.Assert(err, qt.IsNil)
	var document map[string]any
	c.Assert(json.Unmarshal(contents, &document), qt.IsNil)
	roundings, _ := document["roundings"].([]any)
	c.Assert(roundings, qt.HasLen, 1)
	roundings[0].(map[string]any)[field] = value
	edited, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(plan, edited, 0o600), qt.IsNil)
}

// TestSchemaApplyRefusesAPlanWhoseRoundingRecordWasEditedE2E edits the record a
// saved plan holds, in the way that ran SQL of its own in the scan before the
// approval prompt, and in a way that names a column the plan does not change.
// The plan is refused as it is read, and the column, the row and the list of
// tables read back as they were.
func TestSchemaApplyRefusesAPlanWhoseRoundingRecordWasEditedE2E(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
		want  string
	}{
		{
			name:  "SQL in the declared type",
			field: "declared_type",
			value: "DECIMAL(8,1)) THEN 1 END) FROM prices; CREATE TABLE pwned (i int); SELECT COUNT(CASE WHEN 1 <> CAST(1 AS int",
			want:  "plan rounding 1 is not a change the plan records: the declared type is not a type spelling",
		},
		{
			name:  "SQL in the held type",
			field: "held_type",
			value: "DECIMAL(8,3)) THEN 1 END) FROM prices; CREATE TABLE pwned (i int); SELECT COUNT(CASE WHEN 1 <> CAST(1 AS int",
			want:  "plan rounding 1 is not a change the plan records: the held type is not a type spelling",
		},
		{
			name:  "a column the plan does not change",
			field: "column",
			value: "id",
			want:  `plan rounding 1 is not a change the plan records: no statement of the plan changes the type of column "id" of table "prices"`,
		},
		{
			name:  "a scan that keeps more digits than the ALTER",
			field: "declared_type",
			value: "DECIMAL(8,2)",
			want:  `declared type "DECIMAL(8,2)" differs from the statement's type "DECIMAL(8,1)"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := sqlServerServer.scratch(c, sqlServerPrices, sqlServerPricesRow)
			plan := savedPlan(c, target)
			editRounding(c, plan, test.field, test.value)

			shown := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--dry-run")

			c.Assert(shown.ExitCode, qt.Equals, 2, outcome(shown))
			c.Assert(shown.Stderr, qt.Contains, test.want)
			c.Assert(read(c, target, sqlServerPricesHeld), qt.Equals, "decimal(8,3)|1.500|0")
		})
	}
}

// TestSchemaApplyAppliesAPlanWhoseRoundingRecordIsItsOwnE2E is the control: the
// same saved plan, not edited, is shown and then applied.
func TestSchemaApplyAppliesAPlanWhoseRoundingRecordIsItsOwnE2E(t *testing.T) {
	c := qt.New(t)
	target := sqlServerServer.scratch(c, sqlServerPrices, sqlServerPricesRow)
	plan := savedPlan(c, target)

	shown := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--dry-run")
	applied := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--auto-approve")

	c.Assert(shown.ExitCode, qt.Equals, 0, outcome(shown))
	c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
	c.Assert(read(c, target, sqlServerPricesHeld), qt.Equals, "decimal(8,1)|1.5|0")
}

// A row written after planning fits the edited scan's scale but would be
// rounded by the saved ALTER. Both preview and execution refuse that mismatch,
// and the type and stored digits stay intact.
func TestSchemaApplyRefusesAMismatchedSavedPlanRoundingTypePostgresE2E(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	plan := filepath.Join(c.TempDir(), "rounding.plan.json")
	planned := ptah(c, "schema", "plan", "--db-url", target,
		"--schema-file", schemaFile(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,1));\n"),
		"--save", "--output", plan)
	c.Assert(planned.ExitCode, qt.Equals, 0, outcome(planned))
	editRounding(c, plan, "declared_type", "numeric(8,2)")
	run(c, target, "INSERT INTO prices VALUES (2, 1234.550)")

	shown := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--dry-run")
	applied := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--auto-approve")

	c.Assert(shown.ExitCode, qt.Equals, 2, outcome(shown))
	c.Assert(shown.Stderr, qt.Contains, `declared type "numeric(8,2)" differs from the statement's type "numeric(8,1)"`)
	c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
	c.Assert(applied.Stderr, qt.Contains, `declared type "numeric(8,2)" differs from the statement's type "numeric(8,1)"`)
	c.Assert(read(c, target, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'prices'::regclass AND attname = 'amount'"), qt.Equals, "numeric(8,3)")
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.550,1.500")
}
