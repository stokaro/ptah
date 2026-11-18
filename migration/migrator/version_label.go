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
// key names a version no file spells.
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
// A decimal number is that number. It is refused when it reaches the order
// keys of the Atlas files whose version is not a number, since those keys are
// no file's version. Any other text is the version string of such a file, and
// the error names the text when no file carries it.
func (m *Migrator) ResolveVersion(label string) (int64, error) {
	numberless := m.numberlessMigrations()
	if version, err := strconv.ParseInt(label, 10, 64); err == nil {
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
	for _, migration := range numberless {
		if migration.atlasRevisionVersion == label {
			return migration.Version, nil
		}
	}
	return 0, fmt.Errorf("no migration file carries version %q", label)
}

// numberlessMigrations returns the provider's Atlas migrations whose version
// carries no number, lowest order key first.
func (m *Migrator) numberlessMigrations() []*Migration {
	if m == nil || m.migrationProvider == nil {
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
