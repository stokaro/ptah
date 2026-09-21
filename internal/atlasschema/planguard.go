package atlasschema

import (
	"errors"
	"fmt"

	"ptah.run/internal/sqlreach"
)

// PlanEscapeError reports that a pre-planned statement matched a known
// dev-database escape construct.
//
// This is a lint result, not a containment verdict: see [escapeRules] for why
// the deny-list cannot be a boundary, and [CheckPlanStatementsSandboxable] for
// which dev databases get real enforcement instead.
type PlanEscapeError struct {
	// StatementIndex is the 1-based position of the offending statement.
	StatementIndex int
	// Construct is the SQL construct that escapes the dev database.
	Construct string
	// Reach describes what the construct can touch.
	Reach string
}

func (e *PlanEscapeError) Error() string {
	return fmt.Sprintf(
		"pre-planned migration was refused before it reached the dev database: statement %d uses %s, which %s. "+
			"A dev database executes plan SQL for real, so the plan is refused before anything runs. "+
			"Review the statement and run it deliberately outside `schema apply --plan`",
		e.StatementIndex, e.Construct, e.Reach)
}

// IsPlanEscape reports whether err wraps a dev-database escape refusal.
func IsPlanEscape(err error) bool {
	var target *PlanEscapeError
	return errors.As(err, &target)
}

// PlanScanDepthError reports that a plan statement nested code inside string
// literals more deeply than the scanner follows. The plan is refused rather
// than accepted unscanned: the point of the scan is that unreviewed SQL does
// not reach a dev database, and a document burying code this deep is not
// something a legitimate planner emits.
type PlanScanDepthError struct {
	StatementIndex int
}

func (e *PlanScanDepthError) Error() string {
	return fmt.Sprintf(
		"pre-planned migration was refused before it reached the dev database: statement %d nests executable code "+
			"inside string literals more than %d levels deep, which the plan scanner does not follow, so the "+
			"statement cannot be checked; review it and run it deliberately outside `schema apply --plan`",
		e.StatementIndex, sqlreach.MaxNesting)
}

// CheckPlanStatementsSandboxable refuses a pre-planned statement that reaches
// outside the dev database, which is the lint that runs in front of every
// dev-database replay.
//
// It is a best-effort lint, NOT a containment boundary. Real enforcement
// exists only on the ephemeral SQLite dev database Ptah creates itself (see
// dbschema.DatabaseConnection.WithUntrustedSQLSession); an operator-supplied
// --dev-url executes plan SQL for real and must be a database the operator is
// willing to expose to a foreign plan file.
//
// The recognition is [sqlreach.Scan]'s, shared with every other caller that
// has to refuse the same constructs. What belongs here is the sentence a plan
// refusal prints.
func CheckPlanStatementsSandboxable(statements []string, dialect string) error {
	for i, statement := range statements {
		finding, found, err := sqlreach.Scan(statement, dialect)
		if err != nil {
			return &PlanScanDepthError{StatementIndex: i + 1}
		}
		if found {
			return &PlanEscapeError{
				StatementIndex: i + 1,
				Construct:      finding.Construct,
				Reach:          finding.Reach,
			}
		}
	}
	return nil
}
