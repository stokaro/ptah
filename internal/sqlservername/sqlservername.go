// Package sqlservername derives the name Ptah writes for a SQL Server primary
// key that a declaration leaves unnamed.
//
// It is a package rather than a function inside the renderer because two
// callers need the same answer: the renderer, which writes the name into
// CREATE TABLE, and the shared namespace checks in
// migration/internal/identifiervalidation, which compare it with the names of
// the views, routines, sequences and other constraints of the schema. SQL
// Server keeps all of them in one namespace, so a view named as the key fails
// with error 2714.
package sqlservername

import (
	"crypto/sha256"
	"encoding/hex"
	"unicode/utf16"
)

// maxIdentifierLength is the longest name SQL Server takes, in UTF-16 code
// units: a sysname is an nvarchar(128). Msg 103 answers a longer one.
const maxIdentifierLength = 128

// primaryKeySuffix ends the name of a primary key nobody named.
const primaryKeySuffix = "_pkey"

// PrimaryKey is the name a primary key nobody named goes by on the table
// whose name, without its schema and its quotes, is table.
//
// SQL Server names an unnamed key itself, `PK__orders__3213E83F2B3A1E5C`, and
// the hash differs from one database to the next. A later file that drops the
// key names the one the dev database held, and another database answers
// `'PK__...' is not a constraint`. Under a name the file chose, it runs on any
// database the file built.
//
// A table's name can leave no room for the suffix. Leaving that key to the
// server would bring the failure back, so its name is the table's cut short,
// with a hash of the whole name, which keeps two long names that start alike
// apart. The result is never longer than SQL Server takes.
func PrimaryKey(table string) string {
	if identifierLength(table)+len(primaryKeySuffix) <= maxIdentifierLength {
		return table + primaryKeySuffix
	}
	sum := sha256.Sum256([]byte(table))
	tail := "_" + hex.EncodeToString(sum[:4]) + primaryKeySuffix
	return cutIdentifier(table, maxIdentifierLength-len(tail)) + tail
}

// identifierLength is a name's length as SQL Server counts it.
func identifierLength(name string) int {
	length := 0
	for _, r := range name {
		length += max(utf16.RuneLen(r), 1)
	}
	return length
}

// cutIdentifier is the longest start of name that SQL Server counts as limit
// or fewer.
func cutIdentifier(name string, limit int) string {
	length := 0
	for at, r := range name {
		length += max(utf16.RuneLen(r), 1)
		if length > limit {
			return name[:at]
		}
	}
	return name
}
