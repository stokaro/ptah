package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/matviewrefresh"
	"go.5x5.cz/ptah/internal/yamlschema"
)

// TestParse_RefusesTheRetiredRefreshStrategy pins the YAML half of the refusal,
// on PRESENCE rather than on a value.
//
// Ptah does not refresh materialized views as part of schema reconciliation, so
// the attribute is refused rather than accepted and dropped
// (stokaro/ptah#1625). The empty and null spellings are here because a refusal
// written against the VALUE lets both of them through -- and a document that
// says `refresh_strategy:` has still declared it.
//
// The field survives on the spec struct for the same reason: the decoder runs
// with KnownFields(true), so deleting it would answer `field refresh_strategy
// not found`, which reads as a typo and explains nothing.
func TestParse_RefusesTheRetiredRefreshStrategy(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "a value the model used to keep", value: "concurrently"},
		{name: "the value the model used to accept silently", value: "manual"},
		{name: "an empty string", value: `""`},
		{name: "no value at all", value: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := yamlschema.Parse([]byte(`
matviews:
  user_stats:
    body: SELECT count(*) FROM users
    refresh_strategy: ` + test.value + `
`))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
			c.Assert(err, qt.ErrorMatches, `.*materialized view "user_stats" declares refresh_strategy.*`)
			c.Assert(err, qt.ErrorMatches, `.*`+matviewrefresh.Reason[:40]+`.*`)
		})
	}
}

// TestParse_AcceptsAMaterializedViewWithoutTheAttribute keeps the refusal
// scoped to the attribute rather than to the object.
func TestParse_AcceptsAMaterializedViewWithoutTheAttribute(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
matviews:
  user_stats:
    body: SELECT count(*) FROM users
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Body, qt.Equals, "SELECT count(*) FROM users")
}
