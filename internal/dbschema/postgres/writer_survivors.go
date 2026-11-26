package postgres

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/internal/dbreset"
)

// RealmResetSurvivors reads the names [PostgreSQLWriter.DropDatabaseRealmKeeping]
// leaves in place whether or not a run created them, as the database stands
// now; see [dbreset.Survivors]. A dev database's claim records them, so the
// replay guard can tell an extension or a schema the run created, which every
// realm reset drops, from one whose comment would outlive the run.
//
// It reports false, and reads nothing, where a realm reset cannot tell the
// extensions a claim kept from the ones it drops: on a catalog that cannot say
// what an extension owns.
func (w *PostgreSQLWriter) RealmResetSurvivors(ctx context.Context) (dbreset.Survivors, bool, error) {
	if w.db == nil {
		return dbreset.Survivors{}, false, fmt.Errorf("no database connection")
	}
	if !extensionOwnershipReadable(w.caps) {
		return dbreset.Survivors{}, false, nil
	}
	capabilities, err := inspectCleanupCapabilities(ctx, w.db)
	if err != nil {
		return dbreset.Survivors{}, false, err
	}
	schemas, err := queryUserSchemas(ctx, w.db, `SELECT nspname FROM pg_namespace ORDER BY nspname`)
	if err != nil {
		return dbreset.Survivors{}, false, err
	}
	extensions, err := w.InstalledExtensions(ctx)
	if err != nil {
		return dbreset.Survivors{}, false, err
	}
	schemas = append(schemas, "public")
	if w.schema != "" {
		schemas = append(schemas, w.schema)
	}
	slices.Sort(schemas)
	return dbreset.Survivors{
		Schemas:    slices.Compact(schemas),
		Extensions: keptCleanupExtensions(capabilities.systemExtensions, extensions),
	}, true, nil
}
