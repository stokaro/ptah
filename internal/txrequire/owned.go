package txrequire

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// OwnerRequirements binds each statement to the transaction requirements of the
// node that rendered it. A -1 provenance index denotes a trailing comment and
// has no requirements. Copies are independent of the nodes and each other.
// Invalid provenance or transaction values return no requirements.
func OwnerRequirements(nodes []ast.Node, statementNodes []int) ([][]plangraph.Transaction, error) {
	result := make([][]plangraph.Transaction, len(statementNodes))
	for i, index := range statementNodes {
		if index < -1 || index >= len(nodes) {
			return nil, fmt.Errorf("%w: transaction provenance %d is outside the planned nodes", renderer.ErrInvalidResult, index)
		}
		if index == -1 {
			continue
		}
		if requirements := ast.OwnerTransactions(nodes[index]); len(requirements) > 0 {
			result[i] = requirements
		}
		if err := ValidateRequirements(result[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ValidateRequirements refuses unrecognized transaction values. Unknown is a
// valid absence of assessment; it does not establish that either mode is safe.
func ValidateRequirements(requirements []plangraph.Transaction) error {
	for _, requirement := range requirements {
		switch requirement {
		case plangraph.TransactionUnknown, plangraph.TransactionAllowed, plangraph.TransactionRequired, plangraph.TransactionForbidden:
		default:
			return fmt.Errorf("%w: unknown owner transaction requirement %q", renderer.ErrInvalidResult, requirement)
		}
	}
	return nil
}

// OwnerRequirementError identifies a statement whose owner requirement conflicts
// with the selected transaction mode. Statement is a one-based position.
type OwnerRequirementError struct {
	Statement   int
	Requirement plangraph.Transaction
}

// Error describes the conflicting statement requirement.
func (e *OwnerRequirementError) Error() string {
	if e.Requirement == plangraph.TransactionRequired {
		return fmt.Sprintf("%s: planned statement %d requires a transaction for its complete operation", ptaherr.ErrUnsupportedFeature, e.Statement)
	}
	return fmt.Sprintf("%s: planned statement %d requires execution outside a transaction", ptaherr.ErrUnsupportedFeature, e.Statement)
}

// Unwrap identifies an unsupported execution mode.
func (e *OwnerRequirementError) Unwrap() error { return ptaherr.ErrUnsupportedFeature }

// CheckRequirements refuses execution that contradicts a known owner requirement.
// Transactional means that the complete plan shares one transaction. Unknown
// assessments remain unknown and require the caller's other execution checks.
// It validates the whole input before assessing the selected mode.
func CheckRequirements(requirements [][]plangraph.Transaction, transactional bool) error {
	for _, statement := range requirements {
		if err := ValidateRequirements(statement); err != nil {
			return err
		}
	}
	for i, statement := range requirements {
		for _, requirement := range statement {
			switch {
			case requirement == plangraph.TransactionRequired && !transactional:
				return &OwnerRequirementError{Statement: i + 1, Requirement: requirement}
			case requirement == plangraph.TransactionForbidden && transactional:
				return &OwnerRequirementError{Statement: i + 1, Requirement: requirement}
			}
		}
	}
	return nil
}
