package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

// TestParse_RowDeletionPolicyHasNoBareKey pins that the policy is not a key of
// the table itself: a document that writes it there is refused.
func TestParse_RowDeletionPolicyHasNoBareKey(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(`
tables:
  events:
    row_deletion_column: created_at
    row_deletion_interval: P30D
    columns:
      id: { type: bigint, primary: true }
`))

	c.Assert(err, qt.ErrorMatches, `(?s).*row_deletion_column.*`)
	c.Assert(db, qt.IsNil)
}
