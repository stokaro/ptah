// Package replayguards lists the migration replay guards of the engines Ptah
// ships, for internal/devclean to look up by dialect. It holds no rule of its
// own: each guard lives in its engine's package and states the dialect it
// serves.
package replayguards

import (
	"slices"

	"ptah.run/internal/devclean/clickhousereplay"
	"ptah.run/internal/devclean/mysqlreplay"
	"ptah.run/internal/devclean/postgresreplay"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/sqlitereplay"
	"ptah.run/internal/devclean/sqlserverreplay"
	"ptah.run/internal/devclean/ydbreplay"
)

// All returns the replay guard of every built-in engine that has one. Each
// call returns a new slice.
func All() []replay.Service {
	return slices.Concat(
		postgresreplay.Services(),
		mysqlreplay.Services(),
		sqlitereplay.Services(),
		sqlserverreplay.Services(),
		clickhousereplay.Services(),
		ydbreplay.Services(),
	)
}
