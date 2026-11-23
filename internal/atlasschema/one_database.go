package atlasschema

import (
	"ptah.run/internal/atlassource"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/schemascope"
)

// refuseOutsideComparedDatabase is [schemascope.RefuseOutsideDatabase] for a diff: a side
// read from one database limits a document or a replayed directory on the
// other side.
//
// A side records no answer to whether its server folds database names, so the
// database a document qualifies a name with is compared as bytes. That refuses
// FA.items beside a side read from fa even on a server that folds names, which
// is the stricter answer: the refusal never admits a name the comparison would
// key apart from the database it read.
func refuseOutsideComparedDatabase(dialect string, from, to atlassource.State) error {
	var bytes mysqlname.Names
	if limitsToOneDatabase(from) && to.Kind != atlassource.KindDatabase {
		return schemascope.RefuseOutsideDatabase(dialect, from.DefaultSchema, bytes, "--from", to.Schema)
	}
	if limitsToOneDatabase(to) && from.Kind != atlassource.KindDatabase {
		return schemascope.RefuseOutsideDatabase(dialect, to.DefaultSchema, bytes, "--to", from.Schema)
	}
	return nil
}

// limitsToOneDatabase reports a side read from one database.
func limitsToOneDatabase(state atlassource.State) bool {
	return state.Kind == atlassource.KindDatabase && !state.WholeServer
}
