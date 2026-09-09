package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// policyDocument renders a table and one policy over it, with the given
// attribute lines spliced into the policy body.
func policyDocument(policyAttrs string) []byte {
	return []byte(`
schema "public" {}

table "rooms" {
  schema = schema.public
  column "id" { type = int }
}

policy "p" {
  on    = table.rooms
  for   = ALL
  to    = ["PUBLIC"]
  using = "true"
  ` + policyAttrs + `
}
`)
}

// rowSecurityDocument renders a table whose row_security block carries the
// given attribute lines.
func rowSecurityDocument(rowSecurityAttrs string) []byte {
	return []byte(`
schema "public" {}

table "rooms" {
  schema = schema.public
  column "id" { type = int }
  row_security {
    enabled = true
    ` + rowSecurityAttrs + `
  }
}
`)
}

// TestParsePolicyAsSelectsHowItCombines_HappyPath pins that a policy's `as`
// reaches the rendered DDL.
//
// Permissive policies are OR-ed with each other and restrictive ones are AND-ed
// over the result, so the two cannot stand in for one another: a restrictive
// policy rendered without its clause admits every row some other policy admits.
// The attribute was refused outright, which left the stronger of the two
// unreachable from this format (stokaro/ptah#3121).
func TestParsePolicyAsSelectsHowItCombines_HappyPath(t *testing.T) {
	rows := []struct {
		name        string
		as          string
		restrictive bool
		wantSQL     string
	}{
		{name: "restrictive", as: `as = RESTRICTIVE`, restrictive: true, wantSQL: `AS RESTRICTIVE`},
		{name: "lowercase restrictive", as: `as = "restrictive"`, restrictive: true, wantSQL: `AS RESTRICTIVE`},
		{name: "permissive", as: `as = PERMISSIVE`, restrictive: false, wantSQL: `FOR ALL`},
		{name: "unstated", as: ``, restrictive: false, wantSQL: `FOR ALL`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(policyDocument(row.as), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.RLSPolicies, qt.HasLen, 1)
			c.Assert(db.RLSPolicies[0].Restrictive, qt.Equals, row.restrictive)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, row.wantSQL)
		})
	}
}

// TestParsePolicyAsLeavesAPermissiveOneUnmarked_HappyPath is the control for
// the reader above.
//
// PERMISSIVE is the server's own default, so writing the clause out would
// change the DDL of every policy that never asked about it. The rendered
// statement has to stay the one a policy without `as` already produced.
func TestParsePolicyAsLeavesAPermissiveOneUnmarked_HappyPath(t *testing.T) {
	c := qt.New(t)

	stated, err := atlashcl.Parse(policyDocument(`as = PERMISSIVE`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	unstated, err := atlashcl.Parse(policyDocument(``), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statedSQL := strings.Join(renderStatements(c, stated, "postgres"), "\n")
	unstatedSQL := strings.Join(renderStatements(c, unstated, "postgres"), "\n")
	c.Assert(statedSQL, qt.Equals, unstatedSQL)
	c.Assert(statedSQL, qt.Not(qt.Contains), `AS PERMISSIVE`)
}

// TestParsePolicyAsRefusesAnythingElse_FailurePath pins that an unrecognized
// value is refused rather than folded into the default.
//
// PERMISSIVE is the weaker of the two, so a misspelled RESTRICTIVE that fell
// back to it would grant the access the policy was written to withhold, and the
// document would be accepted at exit 0.
func TestParsePolicyAsRefusesAnythingElse_FailurePath(t *testing.T) {
	rows := []struct {
		name string
		as   string
	}{
		{name: "misspelled", as: `as = RESTRICTIV`},
		{name: "unrelated word", as: `as = STRICT`},
		{name: "empty string", as: `as = ""`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(policyDocument(row.as), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, `(?s).*policy "p" as must be PERMISSIVE or RESTRICTIVE.*`)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseRowSecurityEnforcedReachesTheOwner_HappyPath pins that a table's
// `enforced` renders FORCE ROW LEVEL SECURITY.
//
// Enabling row-level security leaves the table's owner exempt from every policy
// on it; FORCE is the separate flag that subjects the owner too. Without it a
// document that asked for the stronger posture was refused, so the posture had
// no spelling at all.
func TestParseRowSecurityEnforcedReachesTheOwner_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse(rowSecurityDocument(`enforced = true`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables[0].Forced, qt.IsTrue)
	sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `ENABLE ROW LEVEL SECURITY`)
	c.Assert(sql, qt.Contains, `FORCE ROW LEVEL SECURITY`)
}

// TestParseRowSecurityWithoutEnforcedStaysUnforced_HappyPath is the control for
// the reader above: a table that never asked for FORCE must not acquire it.
func TestParseRowSecurityWithoutEnforcedStaysUnforced_HappyPath(t *testing.T) {
	rows := []struct {
		name  string
		attrs string
	}{
		{name: "unstated", attrs: ``},
		{name: "stated false", attrs: `enforced = false`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(rowSecurityDocument(row.attrs), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
			c.Assert(db.RLSEnabledTables[0].Forced, qt.IsFalse)
			sql := strings.Join(renderStatements(c, db, "postgres"), "\n")
			c.Assert(sql, qt.Contains, `ENABLE ROW LEVEL SECURITY`)
			c.Assert(sql, qt.Not(qt.Contains), `FORCE ROW LEVEL SECURITY`)
		})
	}
}
