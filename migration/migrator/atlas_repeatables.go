package migrator

import (
	"context"
	"fmt"
	"strings"
)

// atlasRepeatableVersion is the revision version the Atlas format gives a file
// named R__<name>.sql: the part of the name before its first underscore.
const atlasRepeatableVersion = "R"

// atlasRepeatableReader is implemented by a provider that reads
// R__<identity>.sql in an Atlas directory as a repeatable migration; see
// [WithAtlasRepeatableMigrations].
type atlasRepeatableReader interface {
	readsAtlasRepeatables() bool
}

// readsAtlasRepeatables reports whether [WithAtlasRepeatableMigrations] was
// given.
func (p *FSMigrationProvider) readsAtlasRepeatables() bool {
	return p.atlasRepeatables
}

// AtlasRepeatableVersionError reports a revision table that records version R
// while the directory's R__<identity>.sql files are read as repeatable
// migrations. The Atlas format reads such a file as the one-time version R, so
// the row says the file ran under that reading. Read as a repeatable
// migration, the file has no row in the record, so the next up run would
// execute it again, and the row R would stay with no file that owns it. The
// run and the status are refused before anything runs.
type AtlasRepeatableVersionError struct {
	// Table is the quoted name of the revision table that holds the row.
	Table string
	// Description is what the row records for the version, such as _view for
	// a file named R__view.sql.
	Description string
	// Identities are the repeatable migrations of the directory, in execution
	// order.
	Identities []string
}

// Error names the row, the files that would run again and the way out.
func (e *AtlasRepeatableVersionError) Error() string {
	files := "no R__ file of the directory"
	if len(e.Identities) > 0 {
		names := make([]string, 0, len(e.Identities))
		for _, identity := range e.Identities {
			names = append(names, "R__"+identity+".sql")
		}
		files = strings.Join(names, ", ")
	}
	return fmt.Sprintf(
		"the revision table %s records version %s (description %q), which the Atlas reading gives a file named "+
			"R__<name>.sql, but this run reads %s as repeatable migrations, which have no record in this database "+
			"and would run again; delete the row with version '%s' from %s and check that each R__ file can run "+
			"again against this database, or read the directory without repeatable migrations",
		e.Table, atlasRepeatableVersion, e.Description, files, atlasRepeatableVersion, e.Table)
}

// refuseAtlasRepeatableVersion refuses revisions that record version R when
// the provider reads R__<identity>.sql files as repeatable migrations. It
// answers nil for every other provider and revision format, so a directory
// read with the Atlas meaning of R__ keeps it.
func (m *Migrator) refuseAtlasRepeatableVersion(revisions []MigrationRevision) error {
	reader, ok := m.migrationProvider.(atlasRepeatableReader)
	if !ok || !reader.readsAtlasRepeatables() || !m.revisionTableFormat.isAtlas() {
		return nil
	}
	for _, revision := range revisions {
		if revision.RevisionVersion() != atlasRepeatableVersion {
			continue
		}
		return &AtlasRepeatableVersionError{
			Table:       m.qualifiedMigrationsTable(),
			Description: revision.Description,
			Identities:  repeatableIdentities(m.providerRepeatables()),
		}
	}
	return nil
}

// repeatableIdentities returns the identities of repeatables, in their order.
func repeatableIdentities(repeatables []*repeatableMigration) []string {
	identities := make([]string, 0, len(repeatables))
	for _, repeatable := range repeatables {
		identities = append(identities, repeatable.identity)
	}
	return identities
}

// upRevisions reads the revisions an up run selects from, refusing a revision
// table the provider's reading of R__ files cannot continue.
func (m *Migrator) upRevisions(ctx context.Context) ([]MigrationRevision, error) {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied migrations: %w", err)
	}
	if err := m.refuseAtlasRepeatableVersion(revisions); err != nil {
		return nil, err
	}
	return revisions, nil
}
