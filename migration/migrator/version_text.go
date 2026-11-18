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

// String returns the version as %d prints it.
func (v versionText) String() string {
	return fmt.Sprintf("%d", v)
}

// Format implements [fmt.Formatter]. A set label prints bare under every verb
// and ignores width and flags, since the messages that print a version use %d
// alone; a number is formatted as the verb, width and flags format an int64.
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
	label, _ := m.versionLabel(version)
	return versionText{number: version, label: label}
}

// numberlessLabels returns the version string of each Atlas file whose
// version is not a number, by the order key it runs under, and nil when the
// directory holds none.
func (m *Migrator) numberlessLabels() map[int64]string {
	var labels map[int64]string
	for _, migration := range m.numberlessMigrations() {
		if labels == nil {
			labels = make(map[int64]string)
		}
		labels[migration.Version] = migration.atlasRevisionVersion
	}
	return labels
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

// VersionLabel returns the version a person names the revision by: the
// version string the row records when it is not a decimal number, such as v1,
// and the number otherwise, so a row for 001_init.sql reads 1. For a
// directory whose migrations run under their positions (see
// [migrationfile.AtlasKeyedByPosition]) it is always the recorded string,
// since the position names no version.
func (r MigrationRevision) VersionLabel() string {
	return r.versionText().String()
}

// versionText names the revision's version for a message that prints it with
// %d: the Atlas version the row records when it is not a decimal number, such
// as v1, and the number otherwise. A decimal spelling such as 001 prints as
// the number, as it always has.
func (r MigrationRevision) versionText() versionText {
	recorded := r.RevisionVersion()
	if r.positionKeyed {
		return versionText{number: r.Version, label: recorded}
	}
	if _, err := strconv.ParseInt(recorded, 10, 64); err == nil {
		return versionText{number: r.Version}
	}
	return versionText{number: r.Version, label: recorded}
}
