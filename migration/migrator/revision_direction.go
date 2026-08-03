package migrator

import "strings"

// revisionDirectionSeparator joins a recorded state to the direction that
// produced it inside the ptah layout's free-form state column.
//
// A failed up and a failed down used to write byte-identical rows -- same
// state, same shape of applied/total -- so nothing downstream could tell which
// body had stopped halfway. `migrations repair --resume-from` then replayed the
// up body over a partially reverted schema and recorded the migration applied
// (stokaro/ptah#995).
//
// The direction is encoded into the existing state column rather than stored in
// a new one. The state column is free-form text (VARCHAR(32), NVARCHAR(32) on
// SQL Server) and only ever compared against 'applied' in SQL, so a suffixed
// value keeps every predicate -- `state = 'applied'`, `state <> 'applied'` --
// answering exactly what it answered before, on every dialect, with no DDL and
// no rewrite of existing rows.
const revisionDirectionSeparator = ":"

// encodeRevisionState renders the value written to the ptah layout's state
// column for a revision recorded while running direction.
//
// Only the down direction is suffixed. An up-direction row is written with the
// same bytes it has always been written with, so upgrading Ptah changes no
// stored value, and a database written by an older Ptah reads back the same way
// it always did.
func encodeRevisionState(state string, direction MigrationDirection) string {
	if direction != MigrationDirectionDown {
		return state
	}
	return state + revisionDirectionSeparator + string(MigrationDirectionDown)
}

// decodeRevisionState splits a stored state column value into the state and the
// direction that produced it.
//
// Anything without a recognized direction suffix -- every row written before
// direction was recorded, and every up-direction row -- reads as the up
// direction, which is what those rows have always meant.
func decodeRevisionState(stored string) (string, MigrationDirection) {
	state, suffix, found := strings.Cut(stored, revisionDirectionSeparator)
	if !found {
		return stored, MigrationDirectionUp
	}
	switch MigrationDirection(suffix) {
	case MigrationDirectionUp, MigrationDirectionDown:
		return state, MigrationDirection(suffix)
	default:
		return stored, MigrationDirectionUp
	}
}
