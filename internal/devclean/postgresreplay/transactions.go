package postgresreplay

import (
	"errors"

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
	blocks, err := filetx.Read(g.dialect, statements)
	if structure, ok := errors.AsType[*filetx.StructureError](err); ok {
		return nil, &replay.TransactionStructureError{Dialect: structure.Dialect, Index: structure.Index, Reason: structure.Reason}
	}
	if err != nil {
		return nil, err
	}
	return blocks.Boundaries(), nil
}
