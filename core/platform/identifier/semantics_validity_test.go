package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// Every dialect's own semantics must survive Normalize unchanged.
//
// Normalize falls back to ForDialect when a value fails validComparison, so a
// comparison mode that is not on that allowlist makes the semantics it belongs
// to silently inert: no compile error, no failing constructor, and planning
// quietly uses a different rule than the one the dialect declared. That is how
// this test came to exist -- adding a mode and forgetting the allowlist broke a
// MySQL foreign-key plan that names no identifier at all (stokaro/ptah#2768).
//
// The dialect list is derived rather than written here, so a dialect added
// later is covered by existing.
func TestForDialect_SemanticsSurviveNormalize(t *testing.T) {
	for _, dialect := range capability.DefaultDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			// A marker Normalize cannot invent. Comparing ForDialect against
			// its own Normalize would assert nothing: the fallback IS
			// ForDialect, so the comparison reduces to x == x and stays green
			// with the allowlist broken. DefaultSchema is unconstrained by the
			// validity rules, so it survives exactly when the modes are valid.
			marked := identifier.ForDialect(dialect)
			marked.DefaultSchema = "ptah-validity-marker"

			normalized := marked.Normalize(dialect)

			c.Assert(normalized.DefaultSchema, qt.Equals, "ptah-validity-marker",
				qt.Commentf("%s declares index=%q table=%q column=%q; Normalize discarded the "+
					"value, so one of those modes is missing from the validity allowlist",
					dialect, marked.IndexNames, marked.TableNames, marked.ColumnNames))
		})
	}
}
