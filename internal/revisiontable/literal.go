package revisiontable

import (
	"encoding/hex"
	"strconv"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// versionLiterals holds each descriptor's RevisionTable.VersionLiteral. The
// migrator renders a literal for every revision it records or reads, and
// copying the whole descriptor through platform.Lookup for each costs more than
// the literal.
var versionLiterals = descriptortable.New(func(described descriptor.Dialect) string {
	return described.RevisionTable.VersionLiteral
})

// VersionLiteral renders an exact Atlas revision identity as a SQL literal
// whose bytes do not depend on a connection's string-escape mode, in the
// style the dialect's descriptor states as its revision table's
// VersionLiteral. A dialect that states no style, or one this package does
// not declare, takes the standard literal.
func VersionLiteral(dialect, value string) string {
	switch versionLiterals.Get(dialect) {
	case "hex":
		return "X'" + hex.EncodeToString([]byte(value)) + "'"
	case "backslash":
		value = strings.ReplaceAll(value, `\`, `\\`)
	case "national":
		return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
	case "dollar":
		if strings.Contains(value, `\`) {
			// Dollar-quoted content is literal regardless of PostgreSQL's
			// standard_conforming_strings setting. Pick a delimiter absent
			// from the revision token so the token cannot terminate its own
			// SQL literal.
			return postgresDollarQuotedLiteral(value)
		}
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func postgresDollarQuotedLiteral(value string) string {
	tag := "$ptah$"
	for suffix := 1; strings.Contains(value, tag); suffix++ {
		tag = "$ptah" + strconv.Itoa(suffix) + "$"
	}
	return tag + value + tag
}
