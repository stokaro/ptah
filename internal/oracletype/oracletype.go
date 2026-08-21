// Package oracletype maps a declared column type to the Oracle spelling.
//
// It is a package rather than a function inside the renderer because two
// callers need the SAME answer: the renderer, which writes the type into DDL,
// and the schema comparison, which has to fold a declared TEXT and a catalog
// CLOB into one type before deciding whether a column changed. When they
// disagreed, comparing a declaration against a database Ptah had just built
// from it reported an ALTER for every column -- `clob -> text`,
// `number(10) -> integer`, `number(1) -> boolean` -- on a schema that matched.
//
// The comparison cannot import the renderer's own copy, which lives under
// core/renderer/internal. Copying the table into the comparison is what SQLite
// does today in migration/schemadiff, and a copy is exactly what drifts
// (stokaro/ptah#1875).
package oracletype

import "strings"

// Map translates a declared type to the Oracle spelling.
//
// Every mapping below was measured on 23.26 and on 21.3, by creating a column
// of the Oracle type and reading user_tab_columns back. Three of them are worth
// keeping the measurement next to:
//
//   - BOOLEAN is a real type on 23.26 and ORA-00902, invalid datatype, on 21.3.
//     It is rendered as NUMBER(1) on both lines rather than gated on a
//     capability key, because that is the spelling every supported line accepts
//     and the one an Oracle schema conventionally carries. Reading it back as a
//     boolean is the reader's job, the same normalization every dialect here
//     needs -- INTEGER also comes back as NUMBER.
//   - TEXT has no Oracle counterpart of the same shape: VARCHAR2 is capped at
//     32767 bytes even with extended sizes, so an uncapped text column becomes
//     CLOB.
//   - INTEGER, SMALLINT, DECIMAL and NUMERIC are all NUMBER in the catalog --
//     INTEGER reads back as NUMBER with a NULL precision and a scale of 0 --
//     so they are rendered as the NUMBER precisions that survive a round trip
//     rather than as the aliases Oracle accepts and then discards.
func Map(declaredType string) string {
	declared := strings.TrimSpace(declaredType)
	base, arguments := splitTypeArguments(strings.ToUpper(declared))

	if mapped, ok := fixedTypes[base]; ok {
		return mapped
	}
	return parameterizedType(base, arguments, declared)
}

// fixedTypes are the declared types whose Oracle spelling carries no
// argument from the declaration.
//
// The integer family is here rather than passed through because Oracle's
// INTEGER and SMALLINT are aliases it discards: measured, a column declared
// INTEGER reads back from user_tab_columns as NUMBER with a NULL precision and
// a scale of 0, which is a different type from the NUMBER(10) a reader would
// have to compare against. Writing the NUMBER precision that survives the round
// trip is what keeps a declared column and a catalog column the same column.
var fixedTypes = map[string]string{
	"BOOLEAN":          "NUMBER(1)",
	"BOOL":             "NUMBER(1)",
	"SMALLINT":         "NUMBER(5)",
	"INT2":             "NUMBER(5)",
	"INTEGER":          "NUMBER(10)",
	"INT":              "NUMBER(10)",
	"INT4":             "NUMBER(10)",
	"MEDIUMINT":        "NUMBER(10)",
	"BIGINT":           "NUMBER(19)",
	"INT8":             "NUMBER(19)",
	"SERIAL":           "NUMBER(10)",
	"AUTO_INCREMENT":   "NUMBER(10)",
	"BIGSERIAL":        "NUMBER(19)",
	"SMALLSERIAL":      "NUMBER(5)",
	"IDENTITY":         "NUMBER(19)",
	"REAL":             "BINARY_FLOAT",
	"FLOAT4":           "BINARY_FLOAT",
	"DOUBLE PRECISION": "BINARY_DOUBLE",
	"FLOAT8":           "BINARY_DOUBLE",
	"DOUBLE":           "BINARY_DOUBLE",
	// VARCHAR2 is capped at 32767 bytes even with extended sizes, so an
	// uncapped text column has to be a LOB.
	"TEXT":       "CLOB",
	"CITEXT":     "CLOB",
	"LONGTEXT":   "CLOB",
	"MEDIUMTEXT": "CLOB",
	"TINYTEXT":   "CLOB",
	"BYTEA":      "BLOB",
	"BLOB":       "BLOB",
	"LONGBLOB":   "BLOB",
	"MEDIUMBLOB": "BLOB",
	"TINYBLOB":   "BLOB",
	"VARBINARY":  "BLOB",
	"BINARY":     "BLOB",
	// 16 raw bytes rather than the 36-character text form: RAW(16) is what
	// Oracle's own SYS_GUID() produces, so a column declared UUID here and one
	// filled by the database agree.
	"UUID":                     "RAW(16)",
	"TIMESTAMPTZ":              "TIMESTAMP WITH TIME ZONE",
	"TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
	"DATETIME":                 "TIMESTAMP",
	// Measured accepted on 23.26 and 21.3 alike, reported back as
	// data_type=JSON.
	"JSON":  "JSON",
	"JSONB": "JSON",
	"XML":   "XMLTYPE",
}

// parameterizedType answers the types that carry the declaration's own
// argument list.
func parameterizedType(base, arguments, declared string) string {
	switch base {
	case "DECIMAL", "NUMERIC":
		return "NUMBER" + arguments
	case "VARCHAR", "CHARACTER VARYING":
		if arguments == "" {
			return "VARCHAR2(4000)"
		}
		return "VARCHAR2" + arguments
	case "CHAR", "CHARACTER":
		return "CHAR" + arguments
	default:
		return declared
	}
}

func splitTypeArguments(upper string) (base, arguments string) {
	index := strings.Index(upper, "(")
	if index < 0 {
		return upper, ""
	}
	return strings.TrimSpace(upper[:index]), strings.TrimSpace(upper[index:])
}

// Base returns the declared type without its argument list, upper-cased.
//
// It is exported because a caller that needs to recognize a declared type --
// the renderer deciding whether a default literal belongs to a BOOLEAN that
// became NUMBER(1), for instance -- must split it the same way [Map] does. A
// second splitter would be a second place for `DOUBLE PRECISION` and
// `TIMESTAMP WITH TIME ZONE` to be handled differently.
func Base(declaredType string) string {
	base, _ := splitTypeArguments(strings.ToUpper(strings.TrimSpace(declaredType)))
	return base
}
