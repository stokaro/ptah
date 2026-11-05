package revisiontable

import (
	"encoding/hex"
	"strconv"
	"strings"

	"ptah.run/core/platform"
)

// VersionLiteral renders an exact Atlas revision identity as a SQL literal
// whose bytes do not depend on a connection's string-escape mode, in the
// style the dialect's descriptor states as its revision table's
// VersionLiteral. A dialect that states no style, or one this package does
// not declare, takes the standard literal.
func VersionLiteral(dialect, value string) string {
	described, _ := platform.Lookup(dialect)
	switch described.RevisionTable.VersionLiteral {
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
