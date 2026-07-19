// Package risk defines shared severity vocabulary for migration safety checks.
package risk

// Severity is the shared risk level type used by lint and safety reports.
type Severity string

const (
	// Safe marks changes that should not remove data or tighten existing
	// constraints.
	Safe Severity = "safe"
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
		return 0
	}
}

// IsBlocking reports whether severity should fail safety gates by default.
func IsBlocking(severity Severity) bool {
	return Rank(severity) >= Rank(Error)
}

// SARIFLevel maps Ptah severity values to SARIF result levels.
func SARIFLevel(severity Severity) string {
	if IsBlocking(severity) {
		return "error"
	}
	return "warning"
}
