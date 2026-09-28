package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"ptah.run/core/platform"
)

// CockroachDB is the one engine this reader serves with an index the optimizer
// does not use. Measured on v26.3.2, pg_get_indexdef ends such an index in the
// clause CREATE INDEX takes, after any WHERE:
//
//	CREATE INDEX kw ON public.t USING btree (a ASC) WHERE (a > 0) NOT VISIBLE
//	CREATE INDEX kv ON public.t USING btree (a ASC, b ASC) VISIBILITY 0.50
//
// and a visible index ends in neither. information_schema.statistics reports
// is_visible NO for both, so the definition is the one place that tells a
// hidden index from a partially visible one.

// partialVisibilityClause is the clause CockroachDB prints for an index the
// optimizer uses for a share of queries.
var partialVisibilityClause = regexp.MustCompile(`\sVISIBILITY\s+([0-9.]+)\s*$`)

// indexInvisible reads whether the optimizer is kept off an index from the
// definition the server printed for it.
//
// A partially visible index is refused by name rather than read as hidden or
// shown: the model holds one of the two, and either reading would let the next
// apply move the index to a visibility nobody declared.
func indexInvisible(dialect, name, definition string) (bool, error) {
	if dialect != platform.CockroachDB {
		return false, nil
	}
	trimmed := strings.TrimSpace(definition)
	if match := partialVisibilityClause.FindStringSubmatch(trimmed); match != nil {
		return false, fmt.Errorf(
			"index %q is partially visible (VISIBILITY %s), which Ptah does not model; "+
				"make it VISIBLE or NOT VISIBLE", name, match[1])
	}
	return strings.HasSuffix(trimmed, " NOT VISIBLE"), nil
}
