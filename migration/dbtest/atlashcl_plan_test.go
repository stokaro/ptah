package dbtest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// planCaseSource is the shape the Atlas documentation shows for a plan test:
// establish a starting state, seed it, apply a reviewed plan, assert what the
// plan did.
const planCaseSource = `test "plan" "add_email" {
  schema {
    url = "file://snapshots/v1.sql"
  }

  exec {
    sql = "INSERT INTO users (id, name) VALUES (1, 'Ada')"
  }

  apply {
    url = "file://plans/add_email.plan.hcl"
  }

  exec {
    sql    = "SELECT name FROM users WHERE id = 1"
    output = "Ada"
  }
}
`

// TestParseAtlasTestCases_PlanKindPreservesStepOrder is the property the whole
// case depends on.
//
// A plan is only meaningful against the state it was computed for, so the
// schema block has to run before the seed and the seed before the apply.
// hclsyntax reports blocks in source order and the translation walks them
// directly; a schema-driven decode would group by block type and silently put
// both `exec` steps together, which reads the row before the plan that adds
// its column (stokaro/ptah#1211).
func TestParseAtlasTestCases_PlanKindPreservesStepOrder(t *testing.T) {
	c := qt.New(t)

	cases, err := dbtest.ParseAtlasTestCases(
		[]byte(planCaseSource), "plan.test.hcl", dbtest.AtlasTestKindPlan)

	c.Assert(err, qt.IsNil)
	c.Assert(cases, qt.HasLen, 1)
	c.Assert(cases[0].Name, qt.Equals, "add_email")
	steps := cases[0].Steps
	c.Assert(steps, qt.HasLen, 4)

	c.Assert(steps[0].EstablishSchema, qt.IsNotNil)
	c.Assert(steps[0].EstablishSchema.URL, qt.Equals, "file://snapshots/v1.sql")
	c.Assert(steps[1].Exec, qt.Contains, "INSERT INTO users")
	c.Assert(steps[2].ApplyPlan, qt.IsNotNil)
	c.Assert(steps[2].ApplyPlan.URL, qt.Equals, "file://plans/add_email.plan.hcl")
	c.Assert(steps[3].Assert, qt.IsNotNil)
	c.Assert(*steps[3].Assert.Scalar, qt.Equals, "Ada")
}

// TestParseAtlasTestCases_KindsStaySeparate is the rule the loader has always
// had, extended to the third kind: a file may hold all three, and a run gets
// only the kind it asked for.
func TestParseAtlasTestCases_KindsStaySeparate(t *testing.T) {
	source := planCaseSource + `
test "schema" "plain" {
  exec {
    sql = "SELECT 1"
  }
}

test "migrate" "moves" {
  migrate {
    to = "latest"
  }
}
`
	tests := []struct {
		name string
		kind dbtest.AtlasTestKind
		want string
	}{
		{name: "plan", kind: dbtest.AtlasTestKindPlan, want: "add_email"},
		{name: "schema", kind: dbtest.AtlasTestKindSchema, want: "plain"},
		{name: "migrate", kind: dbtest.AtlasTestKindMigrate, want: "moves"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cases, err := dbtest.ParseAtlasTestCases([]byte(source), "all.test.hcl", test.kind)

			c.Assert(err, qt.IsNil)
			c.Assert(cases, qt.HasLen, 1)
			c.Assert(cases[0].Name, qt.Equals, test.want)
		})
	}
}

// TestParseAtlasTestCases_PlanStepsBelongToPlanCases refuses the two new steps
// in another kind's case.
//
// Accepting them would run a plan the caller of that kind never asked for,
// which is the reason kinds are not interchangeable in the first place.
func TestParseAtlasTestCases_PlanStepsBelongToPlanCases(t *testing.T) {
	tests := []struct {
		name string
		step string
		kind dbtest.AtlasTestKind
		want string
	}{
		{
			name: "schema step in a schema case",
			step: `schema { url = "file://x.sql" }`,
			kind: dbtest.AtlasTestKindSchema,
			want: `.*step "schema" belongs to a ` + "`" + `test "plan"` + "`" + ` case, not ` + "`" + `test "schema"` + "`" + `.*`,
		},
		{
			name: "apply step in a migrate case",
			step: `apply { url = "file://x.plan.hcl" }`,
			kind: dbtest.AtlasTestKindMigrate,
			want: `.*step "apply" belongs to a ` + "`" + `test "plan"` + "`" + ` case.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := `test "` + string(test.kind) + `" "x" {
  ` + test.step + `
}
`

			_, err := dbtest.ParseAtlasTestCases([]byte(source), "x.test.hcl", test.kind)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestParseAtlasTestCases_PlanStepRefusals covers what a malformed plan case
// gets, answered where it is written rather than when it runs.
func TestParseAtlasTestCases_PlanStepRefusals(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "schema without url",
			body: `schema {}`,
			want: `.*schema.*url.*`,
		},
		{
			name: "apply without url",
			body: `apply {}`,
			want: `.*apply.*url.*`,
		},
		{
			name: "unknown attribute on schema",
			body: "schema {\n    url     = \"file://x.sql\"\n    dialect = \"postgres\"\n  }",
			want: `.*dialect.*`,
		},
		{
			name: "a step no kind has",
			body: `frobnicate {}`,
			want: ".*unsupported step \"frobnicate\": want `exec`, `migrate`, `schema` or `apply`.*",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "test \"plan\" \"x\" {\n  " + test.body + "\n}\n"

			_, err := dbtest.ParseAtlasTestCases([]byte(source), "x.test.hcl", dbtest.AtlasTestKindPlan)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestParseAtlasTestCases_UnknownKindNamesAllThree keeps the refusal current:
// a message listing two kinds after a third exists sends an author looking for
// a typo in the one they wanted.
func TestParseAtlasTestCases_UnknownKindNamesAllThree(t *testing.T) {
	c := qt.New(t)

	_, err := dbtest.ParseAtlasTestCases(
		[]byte("test \"frobnicate\" \"x\" {\n}\n"), "x.test.hcl", dbtest.AtlasTestKindPlan)

	c.Assert(err, qt.ErrorMatches, `.*unsupported test kind "frobnicate": want "schema", "migrate" or "plan".*`)
}
