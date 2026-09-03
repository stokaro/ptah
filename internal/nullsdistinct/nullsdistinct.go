// Package nullsdistinct holds the one refusal for PostgreSQL's
// NULLS [NOT] DISTINCT unique semantics on dialects that have no spelling for
// them. The parser reaches it when it reads such SQL, and the renderer reaches
// it when a model authored or read elsewhere carries the clause; both report
// the same feature label so a caller branching on the refusal sees one answer.
package nullsdistinct

import (
	"fmt"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// Feature is the CapabilityError feature label the refusal carries. It is a
// diagnostic label meant to be reported rather than branched on; branch on
// [ptaherr.ErrUnsupportedFeature], which the returned error wraps.
const Feature = "NULLS DISTINCT unique-constraint semantics"

// clauses spells each NullsDistinct value as the SQL it stands for. It is a
// lookup rather than a branch because the value selects a name, not a code
// path -- which is also what keeps revive's flag-parameter rule satisfied.
var clauses = map[bool]string{
	true:  "NULLS DISTINCT",
	false: "NULLS NOT DISTINCT",
}

// Clause names the source spelling a NullsDistinct value stands for, so a
// refusal quotes the clause the author wrote rather than a boolean.
func Clause(nullsDistinct bool) string {
	return clauses[nullsDistinct]
}

// Refuse reports dialect rejecting the named clause. Measured 2026-09-03,
// MySQL 8.4.11 and MariaDB 11.8.9 answer error 1064 (SQLSTATE 42000) to every
// spelling of it -- named and bare table constraints, and CREATE UNIQUE INDEX
// alike -- so there is no fallback rendering: emitting the constraint without
// the clause would invert the null-equality semantics the model carries. The
// returned error satisfies errors.Is(err, [ptaherr.ErrUnsupportedFeature]) and
// errors.As against *[ptaherr.CapabilityError]. See stokaro/ptah#2788.
func Refuse(dialect, clause string) error {
	normalized := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: Feature,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s does not support the %s clause on a unique constraint or index: "+
				"it is a PostgreSQL feature %s rejects, so Ptah refuses it rather "+
				"than rendering a constraint with different null-equality semantics",
			normalized,
			clause,
			normalized,
		),
	}
}

// Validate refuses a carried NULLS [NOT] DISTINCT clause on a target whose
// capability set has no spelling for it, and answers nil for the far more
// common model that carries none. A nil nullsDistinct is the ordinary case:
// nothing defaults the field, so it is non-nil only where an author wrote
// nulls_distinct=, an atlas.hcl declared it, the parser read the clause, or
// PostgreSQL printed it back.
//
// Both spellings are refused, not just the one that inverts the default,
// because which spelling inverts it depends on the target.
// [capability.UniqueNullsDistinctClause] records the measurement: PostgreSQL,
// MySQL, MariaDB, SQLite and Oracle all treat nulls as distinct in a plain
// UNIQUE, so NULLS DISTINCT is their default and NULLS NOT DISTINCT is the
// one that changes meaning -- while SQL Server treats them as equal, which
// inverts the pair. A rule that dropped whichever value happened to be the
// default would therefore have to be a per-dialect rule, and dropping the
// other one silently is the defect stokaro/ptah#2820 reports.
//
// The returned error satisfies errors.Is(err, [ptaherr.ErrUnsupportedFeature])
// and errors.As against *[ptaherr.CapabilityError].
func Validate(dialect string, caps capability.Capabilities, nullsDistinct *bool) error {
	if nullsDistinct == nil {
		return nil
	}
	if caps.Has(capability.UniqueNullsDistinctClause) {
		return nil
	}
	return Refuse(dialect, Clause(*nullsDistinct))
}
