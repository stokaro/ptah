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
	if label, ok := m.versionLabel(version); ok {
		return label
	}
	return strconv.FormatInt(version, 10)
}

// versionLabel returns the version string version runs under when that is not
// the number: the version of an Atlas file whose version is not a number, or
// of a recorded row no file carries (see [Migrator.recordedAtlasIdentityKey]).
func (m *Migrator) versionLabel(version int64) (string, bool) {
	for _, migration := range m.numberlessMigrations() {
		if migration.Version == version {
			return migration.atlasRevisionVersion, true
		}
	}
	if m == nil {
		return "", false
	}
	for _, identity := range m.recordedAtlasIdentities {
		if key, _ := m.recordedAtlasIdentityKey(identity); key == version {
			return identity, true
		}
	}
	return "", false
}

// KeyedByPosition reports that the migrations run under their positions in
// file-name order: an Atlas directory holds a version that is not a decimal
// number above zero, or a repeatable that shares its number with a numbered
// file. Every numeric version the migrator reports, in a status, a plan or a
// failure, is then a position, which moves when a file is added before it and
// names no migration outside this migrator. The version keys beside those
// numbers name each migration, and [Migrator.VersionLabel] spells one.
func (m *Migrator) KeyedByPosition() bool {
	return m.hasAtlasPositions()
}

// VersionAttribute returns the observation attribute that names the migration
// at version under name, such as migration.current_version: the number, or,
// where the migrations run under their positions (see
// [Migrator.KeyedByPosition]), the migration's version key under name with a
// _key suffix, since a position names no migration outside this migrator.
// There version 0, which names no migration, is the empty key. The migrator's
// own spans and metrics name versions through it, so an attribute a caller
// adds beside them is spelled the same way.
func (m *Migrator) VersionAttribute(name string, version int64) ObservationAttribute {
	if m.KeyedByPosition() {
		return attr(name+"_key", m.planVersionKey(version))
	}
	return attr(name, version)
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
// Over an Atlas revision table the version string of a file names that file,
// and is read first: 00 names 00_x.sql, never version 0, and 1R names
// 1R__rep.sql. Where the migrations run under their positions (a directory
// holding a version that is not a decimal number above zero), a number is
// such a string like any other, and only 0, the empty history, is read as a
// number. Otherwise text is read as a decimal number, which is that number, so
// 1 names 01_a.sql. A number is refused when it is negative, and when it
// reaches the order key of a repeatable R__x.sql, which runs above every
// number. Text that names nothing is refused with the text in the error. Over
// a revision table that records numbers, a directory keyed by position is
// refused first, as every command that reads the revisions refuses it.
func (m *Migrator) ResolveVersion(label string) (int64, error) {
	if err := m.refuseNumberlessVersionsInANumericTable(); err != nil {
		return 0, err
	}
	if version, ok := m.atlasIdentityVersion(label); ok {
		return version, nil
	}
	if m.hasAtlasPositions() && m.revisionTableFormat.isAtlas() {
		// A number names no file here: every key is a position.
		if label == "0" {
			return 0, nil
		}
		return 0, fmt.Errorf("no migration file carries version %q", label)
	}
	numberless := m.numberlessMigrations()
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

// atlasIdentityVersion returns the order key of the Atlas migration whose
// version string is label, over an Atlas revision table, which records that
// string. A repeatable such as 1R is found by its version string too.
func (m *Migrator) atlasIdentityVersion(label string) (int64, bool) {
	if m == nil || m.migrationProvider == nil || !m.revisionTableFormat.isAtlas() {
		return 0, false
	}
	for _, migration := range m.migrationProvider.Migrations() {
		if migration.hasAtlasRevisionVersion && !migration.atlasRevisionVersionMapped &&
			migration.atlasRevisionVersion == label {
			return migration.Version, true
		}
	}
	return 0, false
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
	if label, ok := m.versionLabel(version); ok {
		return label
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
