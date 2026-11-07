// Package sqlservertype maps a declared column type to the SQL Server spelling.
//
// It is a package rather than a function inside the renderer because two callers need
// the same answer: the renderer, which writes the type into DDL, and the schema
// comparison, which has to read a declared `timestamp` and a catalog DATETIME2 as one
// type before deciding whether a column changed. The comparison cannot import the
// renderer, which lives under engine/builtin/internal. A second copy of the table agrees
// when it is written and stops agreeing when the first is extended
// (internal/oracletype is the same shape).
package sqlservertype

import "strings"

// Map translates a portable type name to the type the renderer writes on SQL Server.
// A name it does not know stands as written.
//
// VARCHAR is the spelling a schema written for several engines uses, and PostgreSQL's
// and MySQL's varchar hold Unicode, so NVARCHAR is what preserves the meaning. TIMESTAMP
// is DATETIME2 because the server's own `timestamp` is a rowversion, a counter it fills
// in itself.
func Map(declaredType string) string {
	switch Base(declaredType) {
	case "INTEGER", "INT4", "SERIAL":
		return "INT"
	case "BIGSERIAL":
		return "BIGINT"
	case "BOOLEAN", "BOOL":
		return "BIT"
	case "TEXT", "CITEXT":
		return "NVARCHAR(MAX)"
	case "VARCHAR", "CHARACTER VARYING":
		return replaceTypeName(declaredType, "NVARCHAR")
	case "CHAR", "CHARACTER":
		return replaceTypeName(declaredType, "NCHAR")
	case "BYTEA", "BLOB":
		return "VARBINARY(MAX)"
	case "DOUBLE PRECISION":
		return "FLOAT"
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return "DATETIMEOFFSET"
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return "DATETIME2"
	default:
		return declaredType
	}
}

// Declaration is a column's declared type, as the renderer reads it.
type Declaration struct {
	// Type is the type as declared.
	Type string
	// Verbatim reports a type named for this target, by sql() or by a catalog. It is written
	// as it stands: applied to one of those, [Map] builds another column, since varchar is
	// one byte per character and nvarchar is two (stokaro/ptah#2147).
	Verbatim bool
}

// Rendered is the type the renderer writes for the declaration.
func (d Declaration) Rendered() string {
	if trimmed := strings.TrimSpace(d.Type); d.Verbatim && trimmed != "" {
		return trimmed
	}
	return Map(d.Type)
}

// ServerBuilt is the type the server builds from a portable name that is also the name
// of one of its own types, spelled as declared, and false for any other name. Measured
// on SQL Server 2025 by creating a column of each name and reading it back: `text` is
// TEXT, `varchar(50)` is VARCHAR(50), `character(5)` is CHAR(5) and `timestamp` is
// TIMESTAMP. A column built by hand under one of these words holds what the word means
// to the server, and declaring the same word changes nothing about it. The server
// refuses every other name [Map] rewrites, or builds what [Map] writes.
func ServerBuilt(declaredType string) (string, bool) {
	switch Base(declaredType) {
	case "TEXT", "TIMESTAMP":
		if strings.Contains(declaredType, "(") {
			return "", false
		}
		return declaredType, true
	case "VARCHAR", "CHARACTER VARYING":
		return replaceTypeName(declaredType, "VARCHAR"), true
	case "CHAR", "CHARACTER":
		return replaceTypeName(declaredType, "CHAR"), true
	default:
		return "", false
	}
}

// Base returns a type without its argument list, upper-cased.
func Base(typeName string) string {
	base := strings.ToUpper(strings.TrimSpace(typeName))
	if open := strings.Index(base, "("); open >= 0 {
		base = strings.TrimSpace(base[:open])
	}
	return base
}

func replaceTypeName(original, replacement string) string {
	if open := strings.Index(original, "("); open >= 0 {
		return replacement + original[open:]
	}
	return replacement
}
