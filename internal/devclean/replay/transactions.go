package replay

import "fmt"

// TransactionGuard is the part of a [Guard] that reads a whole migration file
// for the transaction control it carries.
//
// [Guard.Validate] reads one statement, and a statement alone cannot tell a
// BEGIN that a file closes from one it leaves open. A history written for a
// runner that sends each file as it is carries explicit transactions, so the
// replay of such a history reads the file as a unit.
type TransactionGuard interface {
	// TransactionBoundaries reads the statements of one migration file, in
	// order, and returns the indexes of the statements that are the file's
	// own transaction boundaries. It returns nil for a file that carries none.
	//
	// A file whose transaction control it cannot account for in full is
	// refused with a [*TransactionStructureError]: an unmatched or nested
	// boundary, a rollback, a savepoint, or any other form it does not model.
	// Nothing is returned for a refused file, so no boundary of it is accepted.
	//
	// A boundary returned here is accepted without [Guard.Validate] reading it.
	// Every other statement of the file is still validated as before.
	TransactionBoundaries(statements []string) ([]int, error)
}

// UnwrappedTransactionGuard is the part of a [Guard] that reads a whole
// migration file of a directory whose runner sends each file as it is, with no
// transaction around it, as golang-migrate does. Such a runner leaves more
// transaction control to the file than [TransactionGuard] accounts for, such
// as a COMMIT that ends the implicit transaction of the statements before it.
type UnwrappedTransactionGuard interface {
	// UnwrappedTransactionControl reads the statements of one migration file,
	// in order, and returns the indexes of the statements that are the file's
	// own transaction control. It returns nil for a file that carries none.
	// A file it cannot account for in full is refused with a
	// [*TransactionStructureError], as [TransactionGuard] refuses one.
	UnwrappedTransactionControl(statements []string) ([]int, error)
}

// TransactionStructureError is the refusal of a migration file whose
// transaction control a [TransactionGuard] does not accept.
type TransactionStructureError struct {
	// Dialect is the engine whose guard refused the file.
	Dialect string
	// Index is the position, in the file's statements, of the statement the
	// refusal names.
	Index int
	// Reason says what is wrong with the file's transaction control.
	Reason string
}

func (e *TransactionStructureError) Error() string {
	return fmt.Sprintf("%s migration replay rejects transaction control at statement %d: %s",
		e.Dialect, e.Index+1, e.Reason)
}

// StatementIndex returns the zero-based position of the statement the refusal
// names, for a caller that reports it against the file.
func (e *TransactionStructureError) StatementIndex() int { return e.Index }
