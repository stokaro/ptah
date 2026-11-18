package migrator

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
)

// VersionLabel returns the version a person names the migration that runs
// under version by. That is the version string of an Atlas file whose version
// is not a number, such as v1 for v1_x.sql or R for R__view.sql, and the
// decimal number otherwise, including for a version no file carries and on a
// nil Migrator.
//
// Such a file runs under an order key above every number in the directory,
// and the key moves when a numbered file is added, so output that prints the
// key names a version no file spells. Over a revision table that records
// numbers, the one such file the table accepts, an Atlas repeatable, is
// recorded under its key, so there the key is its name.
func (m *Migrator) VersionLabel(version int64) string {
	for _, migration := range m.numberlessMigrations() {
		if migration.Version == version {
			return migration.atlasRevisionVersion
		}
	}
	return strconv.FormatInt(version, 10)
}

// VersionLabels returns [Migrator.VersionLabel] of each version, in order.
func (m *Migrator) VersionLabels(versions []int64) []string {
	labels := make([]string, len(versions))
	for i, version := range versions {
		labels[i] = m.VersionLabel(version)
	}
	return labels
}

// ResolveVersion reads a version a person named, such as the target of a
// rollback, as the version the migrator runs it under.
//
// The version string of an Atlas file whose version is not a decimal number
// above zero, such as v1, 00 or -5, names that file, and is read first: 00
// names 00_x.sql, never version 0. Other text is read as a decimal number,
// which is that number. A canonical decimal above zero is never such a
// version, so 0, 1 and 01 read as numbers whatever the directory holds. A
// number is refused when it is negative, and when it reaches the order keys
// of the files whose version is not a number, since those keys are no file's
// version. Text that is neither is refused with the text in the error. Over a
// revision table that records numbers, a directory holding such a file is
// refused first, as every command that reads the revisions refuses it.
func (m *Migrator) ResolveVersion(label string) (int64, error) {
	if err := m.refuseNumberlessVersionsInANumericTable(); err != nil {
		return 0, err
	}
	numberless := m.numberlessMigrations()
	for _, migration := range numberless {
		if migration.atlasRevisionVersion == label {
			return migration.Version, nil
		}
	}
	if version, err := strconv.ParseInt(label, 10, 64); err == nil {
		if version < 0 {
			return 0, fmt.Errorf("no migration file carries version %d: a migration version is not negative", version)
		}
		for _, migration := range numberless {
			if version >= migration.Version {
				return 0, fmt.Errorf(
					"no migration file carries version %d: Ptah numbers %s after the highest numbered file, "+
						"so name that file by its version, %s",
					version, migration.atlasOrderKey, migration.atlasRevisionVersion,
				)
			}
		}
		return version, nil
	}
	return 0, fmt.Errorf("no migration file carries version %q", label)
}

// logVersion is how a log line names version under its "version" key, as
// [sourceVersionLabel] names it: the source tool's token for a directory read
// through a foreign layout, the version string of an Atlas file whose version
// carries no number (see [Migrator.VersionLabel]), and the number otherwise.
// The order key such a file runs under appears in no file name, so a line
// naming it names nothing anyone can look up.
func (m *Migrator) logVersion(version int64) any {
	if source, ok := m.sourceVersions[version]; ok {
		return source
	}
	for _, migration := range m.numberlessMigrations() {
		if migration.Version == version {
			return migration.atlasRevisionVersion
		}
	}
	return version
}

// logVersions names each of versions as [Migrator.logVersion] does.
func (m *Migrator) logVersions(versions []int64) []any {
	named := make([]any, len(versions))
	for i, version := range versions {
		named[i] = m.logVersion(version)
	}
	return named
}

// numberlessMigrations returns the provider's Atlas migrations whose version
// carries no number, lowest order key first, over an Atlas revision table,
// which records that version. A revision table that records numbers records
// their order keys, so there it returns none.
func (m *Migrator) numberlessMigrations() []*Migration {
	if m == nil || m.migrationProvider == nil || !m.revisionTableFormat.isAtlas() {
		return nil
	}
	var numberless []*Migration
	for _, migration := range m.migrationProvider.Migrations() {
		if migration.atlasNumberless {
			numberless = append(numberless, migration)
		}
	}
	slices.SortFunc(numberless, func(a, b *Migration) int { return cmp.Compare(a.Version, b.Version) })
	return numberless
}
