// Package risk is the severity scale a Ptah finding is expressed on, together
// with the ordering a gate reads it by.
//
// It is a vocabulary rather than an analysis: nothing here inspects a schema.
// Severity, Rank, IsBlocking and SARIFLevel answer "how bad is this, and does
// it block", and the packages that decide WHAT is bad -- migration/safety,
// migration/lint, internal/schemasecurity, the lint gate and cmd/viz -- all
// express their answers on this scale. migration/safety is one consumer among
// several rather than this package's subject (stokaro/ptah#2246 section 2.2).
package risk

// Severity is the level a finding carries, shared by every producer of them.
type Severity string

const (
	// Safe marks changes that should not remove data or tighten existing
	// constraints.
	Safe Severity = "safe"
	// Info marks findings a report should surface and a gate should never act
	// on. It exists so a rule can be introduced to a repository that still
	// violates it, and so a team can say "show me this, never block on it"
	// without the only alternatives being loud enough to fail or absent
	// entirely (stokaro/ptah#1633).
	Info Severity = "info"
	// Warning marks changes that deserve review before a production rollout.
	Warning Severity = "warning"
	// Error marks lint findings that should block by default.
	Error Severity = "error"
	// Destructive marks generated migration statements that remove data,
	// database objects, or protections.
	Destructive Severity = "destructive"
)

// Rank returns a comparable severity order. Error and Destructive are both
// blocking severities expressed in different output vocabularies.
func Rank(severity Severity) int {
	switch severity {
	case Destructive, Error:
		return 2
	case Warning:
		return 1
	default:
		// Safe and Info both land here, and so does an unrecognized value. That
		// is deliberate for the last one: a severity nothing understands must
		// not out-rank the levels that are understood, and Rank is read by
		// gates.
		return 0
	}
}

// IsBlocking reports whether severity should fail safety gates by default.
func IsBlocking(severity Severity) bool {
	return Rank(severity) >= Rank(Error)
}

// SARIFLevel maps Ptah severity values to SARIF result levels.
//
// SARIF has a level below "warning" and Info is what it is for: a result the
// viewer should show and no gate should act on. Collapsing Info into "warning"
// would put an advisory finding at the same level as one that asks for review,
// which is the distinction the level was added to make.
func SARIFLevel(severity Severity) string {
	switch {
	case IsBlocking(severity):
		return "error"
	case severity == Info:
		return "note"
	default:
		return "warning"
	}
}
