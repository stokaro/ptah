package migrator

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/migration/migrationfile"
)

// atlasRepeatableVersion is the revision version the Atlas format gives a file
// named R__<name>.sql: the part of the name before its first underscore.
const atlasRepeatableVersion = "R"

// atlasRepeatableReader is implemented by a provider that can say how it
// reads R__<identity>.sql in a directory read in the Atlas format.
type atlasRepeatableReader interface {
	// ignoresRepeatableRecord reports that the provider reads the directory in
	// the Atlas format and loads no repeatable migration, so it gives
	// R__<identity>.sql the one-time version R and never reads the record.
	ignoresRepeatableRecord() bool
	// readsAtlasRepeatables reports that it reads R__<identity>.sql there as
	// a repeatable migration; see [WithAtlasRepeatableMigrations].
	readsAtlasRepeatables() bool
}

// ignoresRepeatableRecord reports whether the provider reads the Atlas format
// with neither [WithRepeatableMigrations] nor [WithAtlasRepeatableMigrations].
func (p *FSMigrationProvider) ignoresRepeatableRecord() bool {
	return p.format == migrationfile.DirFormatAtlas && !p.loadsRepeatables
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

// AtlasRepeatableRecordError reports a database that records repeatable
// migrations while the directory is read in the Atlas format without them.
// That reading gives each R__<name>.sql file the one-time version R, so a file
// the record says applied would run again as a version, and the record would
// stay behind with no run that reads it. The run and the status are refused
// before anything runs, whether or not the directory still holds the files.
type AtlasRepeatableRecordError struct {
	// Table is the quoted name of the record.
	Table string
	// Identities are the identities the record holds, in identity order.
	Identities []string
}

// Error names the record and what it holds.
func (e *AtlasRepeatableRecordError) Error() string {
	return fmt.Sprintf(
		"this database records repeatable migrations %s in %s, which this run does not read: it reads a file "+
			"named R__<name>.sql as the one-time version R, so a recorded file would run again as a version and "+
			"its record would go stale; read the directory with its repeatable migrations",
		strings.Join(e.Identities, ", "), e.Table)
}

// refuseUnreadAtlasRepeatableRecord refuses a provider that reads the Atlas
// format without repeatable migrations over a database whose record holds a
// row. It costs one catalog lookup when the record does not exist, and answers
// nil for every other provider and for a target that keeps no record. A
// provider that loads repeatable migrations reads the record itself, and a
// rollback refuses over a row in it.
func (m *Migrator) refuseUnreadAtlasRepeatableRecord(ctx context.Context) error {
	reader, ok := m.migrationProvider.(atlasRepeatableReader)
	if !ok || !reader.ignoresRepeatableRecord() || m.refuseRepeatablesHere() != nil {
		return nil
	}
	rows, err := m.readRepeatableRows(ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	return &AtlasRepeatableRecordError{
		Table:      m.repeatableTable(),
		Identities: slices.Sorted(maps.Keys(rows)),
	}
}

// refuseAtlasRepeatableReading refuses a revision table and a record that the
// provider's reading of R__ files cannot continue: a version R recorded under
// the Atlas reading when the files are read as repeatable migrations, and a
// record of repeatable migrations when they are not.
func (m *Migrator) refuseAtlasRepeatableReading(ctx context.Context, revisions []MigrationRevision) error {
	if err := m.refuseAtlasRepeatableVersion(revisions); err != nil {
		return err
	}
	return m.refuseUnreadAtlasRepeatableRecord(ctx)
}

// upRevisions reads the revisions an up run selects from, refusing a revision
// table and a record the provider's reading of R__ files cannot continue.
func (m *Migrator) upRevisions(ctx context.Context) ([]MigrationRevision, error) {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied migrations: %w", err)
	}
	if err := m.refuseAtlasRepeatableReading(ctx, revisions); err != nil {
		return nil, err
	}
	return revisions, nil
}
