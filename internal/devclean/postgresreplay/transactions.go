package postgresreplay

import (
	"errors"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/filetx"
)

// TransactionBoundaries reads a migration file for the explicit transactions
// it opens and closes itself, and returns the indexes of those statements.
//
// A history written for a runner that sends each file as it is has files that
// begin with BEGIN and end with COMMIT. The strict reading refuses both
// statements, because on their own they change state the rest of the replay
// depends on. Read across the file they do not: a transaction the file closes
// leaves the session as it found it.
//
// The reading is [filetx.Read], the one the apply of such a directory uses, so
// a file replays without an enclosing transaction exactly when it would be
// applied without one. A file it refuses is refused here as a
// [*replay.TransactionStructureError]. Only PostgreSQL is read; on CockroachDB,
// YugabyteDB and Spanner each statement stays refused on its own.
//
// The statement that fails inside a transaction is not part of this reading:
// the replay rolls the session back when a file fails.
func (g guard) TransactionBoundaries(statements []string) ([]int, error) {
	if g.dialect != platform.Postgres {
		return nil, nil
	}
	blocks, err := filetx.Read(statements)
	if err != nil {
		return nil, g.structureError(err)
	}
	return blocks.Boundaries(), nil
}

// UnwrappedTransactionControl is [guard.TransactionBoundaries] for a
// directory whose runner sends each file as it is, as golang-migrate does. The
// reading is [filetx.ReadUnwrapped], the one the apply of such a directory
// uses, and every statement of the file's own transaction control it accepts
// is accepted here.
func (g guard) UnwrappedTransactionControl(statements []string) ([]int, error) {
	if g.dialect != platform.Postgres {
		return nil, nil
	}
	blocks, err := filetx.ReadUnwrapped(statements)
	if err != nil {
		return nil, g.structureError(err)
	}
	return blocks.Controls(), nil
}

// structureError is a refusal of the reading as the replay reports one.
func (g guard) structureError(err error) error {
	if structure, ok := errors.AsType[*filetx.StructureError](err); ok {
		return &replay.TransactionStructureError{Dialect: g.dialect, Index: structure.Index, Reason: structure.Reason}
	}
	return err
}
