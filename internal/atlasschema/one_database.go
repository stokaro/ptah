package atlasschema

import (
	"ptah.run/internal/atlassource"
	"ptah.run/internal/schemascope"
)

// refuseOutsideComparedDatabase is [schemascope.RefuseOutsideDatabase] for a diff: a side
// read from one database limits a document or a replayed directory on the
// other side.
func refuseOutsideComparedDatabase(dialect string, from, to atlassource.State) error {
	if limitsToOneDatabase(from) && to.Kind != atlassource.KindDatabase {
		return schemascope.RefuseOutsideDatabase(dialect, from.DefaultSchema, "--from", to.Schema)
	}
	if limitsToOneDatabase(to) && from.Kind != atlassource.KindDatabase {
		return schemascope.RefuseOutsideDatabase(dialect, to.DefaultSchema, "--to", from.Schema)
	}
	return nil
}

// limitsToOneDatabase reports a side read from one database.
func limitsToOneDatabase(state atlassource.State) bool {
	return state.Kind == atlassource.KindDatabase && !state.WholeServer
}
