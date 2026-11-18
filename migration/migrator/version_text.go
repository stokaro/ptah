package migrator

import (
	"fmt"
	"io"
	"strconv"
)

// versionText prints a migration version under any formatting verb the way a
// person names it: the version string of an Atlas file whose version is not a
// number, such as v1 for v1_x.sql, and the number otherwise, formatted exactly
// as the verb formats an int64. A message that prints a version with %d passes
// one in place of the number, so it names such a file rather than the order
// key the file runs under, and its wording for a numbered file is unchanged.
type versionText struct {
	number int64
	// label is the version string of a file whose version is not a number,
	// and empty for a numbered file.
	label string
}

// Format implements [fmt.Formatter].
func (v versionText) Format(f fmt.State, verb rune) {
	if v.label != "" {
		_, _ = io.WriteString(f, v.label)
		return
	}
	_, _ = fmt.Fprintf(f, fmt.FormatString(f, verb), v.number)
}

// versionText names version as [Migrator.VersionLabel] does, for a message
// that prints it with %d.
func (m *Migrator) versionText(version int64) versionText {
	for _, migration := range m.numberlessMigrations() {
		if migration.Version == version {
			return versionText{number: version, label: migration.atlasRevisionVersion}
		}
	}
	return versionText{number: version}
}

// versionText names the migration's version for a message that prints it with
// %d, where no [Migrator] is at hand: the version string of an Atlas file whose
// version is not a number, and the number otherwise.
func (m *Migration) versionText() versionText {
	if m.atlasNumberless {
		return versionText{number: m.Version, label: m.atlasRevisionVersion}
	}
	return versionText{number: m.Version}
}

// versionText names the revision's version for a message that prints it with
// %d: the Atlas version the row records when it is not a decimal number, such
// as v1, and the number otherwise. A decimal spelling such as 001 prints as
// the number, as it always has.
func (r MigrationRevision) versionText() versionText {
	recorded := r.RevisionVersion()
	if _, err := strconv.ParseInt(recorded, 10, 64); err == nil {
		return versionText{number: r.Version}
	}
	return versionText{number: r.Version, label: recorded}
}
