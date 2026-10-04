// Package ydbsequence names the sequence behind a YDB Serial column and reads
// the start and the increment a declaration gives it.
//
// The reader, the comparator, the planner, the renderer and the capability
// probe all ask these questions, and each has to answer them alike: the
// reader refuses a sequence whose name is not the one the planner would
// address, and the comparator calls two declarations equal exactly when the
// planner would write the same ALTER SEQUENCE for both. Two copies of either
// rule agree when the second is written and drift when the first changes, so
// there is one.
//
// What it encodes was measured on YDB 25.1.4.7 and 26.2.1.14. A Serial column
// c of table t owns the sequence `t/_serial_column_c`, which moves with the
// table on `RENAME TO` and goes with it on DROP TABLE. ALTER SEQUENCE takes
// that sequence only by its absolute path, and takes START, INCREMENT and
// RESTART only. The least value is 1 and cannot change, so a start below 1 is
// refused (`Start value: 0 cannot be less than min value: 1`), and an
// increment is positive (a negative one is a parse error, and 0 answers
// `Increment must not be zero`).
package ydbsequence

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbtype"
)

// namePrefix is what YDB puts in front of a Serial column's name to name its
// sequence.
const namePrefix = "_serial_column_"

// Name is the name of the sequence behind Serial column column, relative to
// its table.
func Name(column string) string {
	return namePrefix + column
}

// Path is the absolute path of the sequence behind Serial column column of
// table table, in directory schema of the database at database. schema is
// relative to the database root, and "" is the root itself.
func Path(database, schema, table, column string) string {
	return path.Join("/", database, schema, table, Name(column))
}

// Settings is what Ptah declares of a Serial column's sequence: the value it
// starts at and the step between two values.
type Settings struct {
	Start     int64
	Increment int64
}

// Default is a new sequence's settings: it starts at 1 and steps by 1.
var Default = Settings{Start: 1, Increment: 1}

// IsDefault reports whether s is what a new sequence has without an ALTER.
func (s Settings) IsDefault() bool { return s == Default }

// Parse reads the start and the increment a declaration gives a Serial
// column, as the identity_start and identity_increment attributes spell them.
// An empty value is the default, 1.
//
// It refuses a value YDB's ALTER SEQUENCE would refuse: one that is not a
// whole number, a start below 1 and an increment below 1.
func Parse(start, increment string) (Settings, error) {
	settings := Default
	var err error
	if settings.Start, err = parsePositive("identity_start", start, "a start",
		"`Start value: 0 cannot be less than min value: 1`"); err != nil {
		return Settings{}, err
	}
	if settings.Increment, err = parsePositive("identity_increment", increment, "an increment",
		"a negative increment is a parse error, and 0 answers `Increment must not be zero`"); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

// parsePositive reads one setting, which has to be a whole number of at
// least 1.
func parsePositive(attribute, value, setting, measured string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 1, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a whole number YDB's ALTER SEQUENCE takes", attribute, value)
	}
	if parsed < 1 {
		return 0, fmt.Errorf("%s %d: a YDB Serial's sequence takes %s of 1 or more (measured: %s)",
			attribute, parsed, setting, measured)
	}
	return parsed, nil
}

// Canonical is the form two spellings of one setting share, for a comparison:
// an empty value is 1, and a whole number is written in decimal without a sign
// or leading zeros. A value Parse refuses is kept as written, so a comparison
// still sees it differ and the plan refuses it with Parse's reason.
func Canonical(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "1"
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value
	}
	return strconv.FormatInt(parsed, 10)
}

// RangeRefusal says why Ptah does not give the sequence of a Serial column of
// YDB type serialType settings other than the default, or "" when it does.
//
// Every ALTER SEQUENCE raises the maximum of a Serial's or a SmallSerial's
// sequence to the Int64 maximum on a target without
// [capability.SerialSequenceKeepsRange], and nothing lowers it again; the
// column then wraps to negative values without an error. A BigSerial's
// sequence ends at the Int64 maximum already.
func RangeRefusal(serialType string, settings Settings, caps capability.Capabilities) string {
	if settings.IsDefault() || serialType == ydbtype.BigSerial || caps.Has(capability.SerialSequenceKeepsRange) {
		return ""
	}
	limit := "32767"
	if serialType == ydbtype.Serial {
		limit = "2147483647"
	}
	return fmt.Sprintf("an ALTER SEQUENCE raises the maximum of a %s's sequence from %s to the Int64 maximum, "+
		"and the column then stores the value after %s as a negative number without an error; "+
		"declare the column as a 64-bit Serial (BIGSERIAL, or BIGINT with auto_increment) to give its sequence "+
		"a start or an increment", serialType, limit, limit)
}
