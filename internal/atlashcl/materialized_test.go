package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/matviewrefresh"
)

// TestParseMaterializedViewRefusesRetiredRefreshStrategy is what this file used
// to assert the opposite of.
//
// The attribute was carried into the model, canonicalized, defaulted to
// "manual" when absent, and then ignored by every renderer -- declarative state
// nothing could reconcile. Ptah does not refresh materialized views as part of
// schema reconciliation, so the attribute is refused rather than accepted and
// dropped (stokaro/ptah#1625).
//
// The refusal is on PRESENCE. An HCL attribute's value need not be a string --
// a bare identifier, a variable reference and sql(...) are all legal
// expressions -- so a refusal that read the value first would let two of those
// three spellings through.
func TestParseMaterializedViewRefusesRetiredRefreshStrategy(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "the value the model used to keep", value: `"concurrently"`},
		{name: "the value the model used to accept silently", value: `"manual"`},
		{name: "a schedule the shared model never carried", value: `"every 5 minutes"`},
		{name: "a bare identifier, which is not a string at all", value: `CONCURRENTLY`},
		{name: "an empty string", value: `""`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as               = "SELECT count(*) FROM users"
  refresh_strategy = `+test.value+`
}
`), "schema.hcl")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
			c.Assert(err, qt.ErrorMatches, `.*materialized view "user_stats" declares refresh_strategy.*`)
			c.Assert(err, qt.ErrorMatches, `.*`+matviewrefresh.Reason[:40]+`.*`)
		})
	}
}

// TestParseMaterializedViewWithoutRefreshStrategyIsAccepted is the control in
// the other direction: the refusal is scoped to the attribute, not to
// materialized views.
func TestParseMaterializedViewWithoutRefreshStrategyIsAccepted(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  schema  = schema.public
  as      = "SELECT count(*) FROM users"
  comment = "user stats"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Name, qt.Equals, "public.user_stats")
	c.Assert(db.MaterializedViews[0].Body, qt.Equals, "SELECT count(*) FROM users")
	c.Assert(db.MaterializedViews[0].Comment, qt.Equals, "user stats")
}

func TestParseMaterializedViewRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as       = "SELECT count(*) FROM users"
  populate = true
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*unsupported materialized attribute "populate".*`)
}

// TestMaterializedViewRetiredAttributeGoAnnotationParity keeps the parity
// control this file has carried since #684, on the answer that is now correct.
//
// The two frontends used to agree that the attribute was accepted; they agree
// now that it is refused, and with the same reason. A parity test that was
// deleted along with the behaviour would have let one frontend keep accepting
// it.
func TestMaterializedViewRetiredAttributeGoAnnotationParity(t *testing.T) {
	c := qt.New(t)

	_, goErr := goschema.ParseSource("user_stats.go", `package models

//ptah:schema:matview name="user_stats" body="SELECT count(*) FROM users" refresh_strategy="concurrently"
type UserStatsMatView struct{}
`)
	_, hclErr := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as               = "SELECT count(*) FROM users"
  refresh_strategy = "concurrently"
}
`), "schema.hcl")

	c.Assert(goErr, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
	c.Assert(hclErr, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
	c.Assert(goErr, qt.ErrorMatches, `.*`+matviewrefresh.Reason[:40]+`.*`)
	c.Assert(hclErr, qt.ErrorMatches, `.*`+matviewrefresh.Reason[:40]+`.*`)
}
