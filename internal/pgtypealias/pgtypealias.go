// Package pgtypealias reduces a PostgreSQL type spelling to the one the
// catalog's format_type reports, so a comparison of a declared type and a read
// one does not report an alias as a difference. It serves the common
// user-type comparison and the PostgreSQL type owner, which have to agree on
// what counts as the same type.
package pgtypealias

import "strings"

// aliases maps accepted type spellings to the canonical form format_type
// emits.
var aliases = map[string]string{
	"varchar":     "character varying",
	"char":        "character",
	"int":         "integer",
	"int4":        "integer",
	"int8":        "bigint",
	"int2":        "smallint",
	"serial":      "integer",
	"serial4":     "integer",
	"serial8":     "bigint",
	"bigserial":   "bigint",
	"smallserial": "smallint",
	"serial2":     "smallint",
	"float8":      "double precision",
	"float4":      "real",
	"bool":        "boolean",
	"decimal":     "numeric",
	"timestamptz": "timestamp with time zone",
	"timestamp":   "timestamp without time zone",
	"timetz":      "time with time zone",
}

// Canonical lower-cases a type, removes the spaces from its parameter list,
// and maps a common alias to the spelling format_type emits. Two spellings of
// one type answer the same; `VARCHAR(20)` and `character varying (20)` both
// answer `character varying(20)`.
func Canonical(spelling string) string {
	if strings.ContainsAny(spelling, `."`) {
		if canonical, ok := referenceType(spelling); ok {
			return canonical
		}
	}
	spelling = strings.ToLower(strings.TrimSpace(spelling))
	base, params := spelling, ""
	if i := strings.IndexAny(spelling, "(["); i >= 0 {
		base = strings.TrimSpace(spelling[:i])
		params = strings.ReplaceAll(spelling[i:], " ", "")
	}
	if canonical, ok := Alias(base); ok {
		base = canonical
	}
	return base + params
}

// Alias returns the spelling format_type emits for a bare type name written as
// one of its aliases, matched without regard to case, and false for a name
// that is no alias. It reads the name alone: a parameter list or an array
// marker makes it no alias.
func Alias(name string) (string, bool) {
	canonical, ok := aliases[strings.ToLower(name)]
	return canonical, ok
}

// builtinTypeNames are PostgreSQL's own type names as a comparison spells a
// column's type, with no length, precision or array suffix.
var builtinTypeNames = map[string]bool{
	"smallint": true, "int2": true, "integer": true, "int": true, "int4": true, "bigint": true, "int8": true,
	"smallserial": true, "serial2": true, "serial": true, "serial4": true, "bigserial": true, "serial8": true,
	"decimal": true, "numeric": true, "real": true, "float4": true, "double precision": true, "float8": true, "float": true,
	"money": true, "text": true, "varchar": true, "character varying": true, "char": true, "character": true,
	"bpchar": true, "name": true, "bytea": true, "boolean": true, "bool": true, "date": true, "time": true,
	"timetz": true, "time without time zone": true, "time with time zone": true, "timestamp": true,
	"timestamptz": true, "timestamp without time zone": true, "timestamp with time zone": true,
	"interval": true, "uuid": true, "json": true, "jsonb": true, "xml": true, "inet": true, "cidr": true,
	"macaddr": true, "macaddr8": true, "bit": true, "varbit": true, "bit varying": true, "tsvector": true,
	"tsquery": true, "point": true, "line": true, "lseg": true, "box": true, "path": true, "polygon": true,
	"circle": true, "oid": true, "regclass": true,
}

// Builtin reports whether spelling names one of PostgreSQL's own types, or an
// array of one. Any other name is a type somebody created, such as a domain.
func Builtin(spelling string) bool {
	name := strings.ToLower(strings.TrimSpace(spelling))
	name = strings.TrimSpace(strings.TrimSuffix(name, "[]"))
	if open := strings.IndexByte(name, '('); open >= 0 {
		name = strings.TrimSpace(name[:open])
	}
	name = strings.TrimPrefix(name, "pg_catalog.")
	return builtinTypeNames[name]
}
