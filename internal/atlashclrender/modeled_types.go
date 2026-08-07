package atlashclrender

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// modeledColumnTypes lists, per dialect, the column type names the Atlas HCL
// schema models as a type expression. A type outside its dialect's list has to
// be written as a sql() call, because the pinned Atlas community binary v1.3.0
// refuses every other spelling of one.
//
// Both refusals are measured, not assumed. On SQLite, feeding that binary a
// column whose type is written bare:
//
//	type = integer                     accepted, exit 0
//	type = USER_DEFINED                refused, exit 1, `There is no type named "USER_DEFINED"`
//	type = timestamp                   refused, exit 1, the same complaint
//
// and on PostgreSQL, feeding it the spellings Ptah's inspect produces today for
// an ordinary table:
//
//	type = NUMERIC(10,2)               refused, exit 1, `There is no function named "NUMERIC"`
//	type = "timestamp with time zone"  refused, exit 1, `schemahcl: failed reading spec`
//	type = "text[]"                    refused, exit 1, the same complaint
//	type = sql("timestamp with time zone")  accepted, exit 0
//
// `timestamp` is why this list cannot be derived from Ptah's own type
// vocabulary, or from intuition: it is an ordinary type name that Ptah models
// and that binary does not model for SQLite. `NUMERIC` is why the case rule in
// modeledColumnType is load bearing rather than cosmetic: written upper case a
// sized type does not merely fail to resolve, it parses as a CALL, and the
// complaint that comes back names a missing function rather than a missing
// type.
//
// A hand-copied table mirroring another tool's registry drifts, and
// internal/atlashcl/sqlrawexpr.go records why this repository refused to carry
// one before: "a wrong row in that table breaks drop-in silently". Two things
// answer that here.
//
// The first is that the failure is asymmetric, so a list that is too SHORT is
// safe and a list that is too LONG is not: wrapping a type that did not need it
// produces sql("integer"), which round trips. That is measured rather than
// assumed -- every one of the 57 type names measured as accepted bare on
// PostgreSQL was re-fed to the same binary wrapped, and all 57 were accepted,
// with no counterexample.
//
// The second is that the list is not trusted:
// TestOracleModeledColumnTypesMatchTheBinary re-measures every entry against the
// pinned binary, so a row that stops being true turns the Atlas CE Oracle job
// red instead of silently degrading a round trip.
//
// What wrapping does NOT do is rescue a type the ENGINE does not have, and
// conflating the two would make this list look more powerful than it is. The
// binary evaluates a sql() body against the dev database, so on PostgreSQL
//
//	type = sql("hstore")               refused, exit 1, extension not installed
//	type = sql("citext")               refused, exit 1
//	type = sql("USER-DEFINED")         refused, exit 1
//	type = sql("ARRAY")                refused, exit 1
//
// all fail, exactly as their bare spellings do. Wrapping rescues a type the HCL
// schema does not MODEL. A column whose type does not exist in the dev database
// is a different defect, and the catalog placeholders `USER-DEFINED` and
// `ARRAY` are resolved before rendering -- see goSchemaFieldType in
// internal/convert/dbschematogo.
//
// Dialects absent from this map wrap nothing, which is exactly today's
// behavior for them (stokaro/ptah#1138).
var modeledColumnTypes = map[string]map[string]struct{}{
	platform.SQLite: namesToSet(
		"bigint",
		"blob",
		"bool",
		"boolean",
		"character",
		"date",
		"datetime",
		"decimal",
		"double",
		"float",
		"int",
		"integer",
		"json",
		"jsonb",
		"numeric",
		"real",
		"smallint",
		"text",
		"uuid",
		"varchar",
	),
	// Measured by feeding each name to the pinned binary bare, as
	// `type = <name>`, against a PostgreSQL 17 dev database. Names refused
	// there are deliberately absent: hstore, citext, ltree and geometry are
	// extension types the dev database does not carry, and they are refused
	// wrapped as well, so listing them would not have helped.
	//
	// The multi-word spellings PostgreSQL's own catalog reports -- `character
	// varying`, `double precision`, `timestamp with time zone`, `time without
	// time zone` -- are absent for a different reason: they are not writable
	// bare at all, because two identifiers separated by a space is not one HCL
	// expression. They reach the binary through sql(), which accepts all four.
	platform.Postgres: namesToSet(
		"bigint",
		"bigserial",
		"bit",
		"bool",
		"boolean",
		"box",
		"bytea",
		"char",
		"character",
		"cidr",
		"circle",
		"date",
		"daterange",
		"decimal",
		"float",
		"float4",
		"float8",
		"inet",
		"int",
		"int2",
		"int4",
		"int4range",
		"int8",
		"int8range",
		"integer",
		"interval",
		"json",
		"jsonb",
		"line",
		"lseg",
		"macaddr",
		"macaddr8",
		"money",
		"name",
		"numeric",
		"numrange",
		"oid",
		"path",
		"point",
		"polygon",
		"real",
		"regclass",
		"serial",
		"smallint",
		"smallserial",
		"text",
		"time",
		"timestamp",
		"timestamptz",
		"timetz",
		"tsquery",
		"tsrange",
		"tstzrange",
		"tsvector",
		"uuid",
		"varchar",
		"xml",
	),
}

func namesToSet(names ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

// modeledColumnType reports how a column type has to be written for the named
// dialect: the modeled spelling and true, or "" and false when it has to become
// a sql() call.
//
// The size argument is split off before the lookup and put back afterwards:
// `varchar(100)` is the modeled type `varchar` carrying a length, and the
// binary accepts it written that way. So are `numeric(10,2)`, `bit(8)`,
// `timestamptz(3)` and `character(10)`, all measured.
//
// The name comes back lower-cased because that binary's type names are CASE
// SENSITIVE. Measured on SQLite:
//
//	type = integer      accepted, exit 0
//	type = INTEGER      refused, exit 1, `There is no type named "INTEGER"`
//	type = Text         refused, exit 1, `Did you mean "text"?`
//
// and identically on PostgreSQL, where `integer` and `varchar` are accepted
// while `INTEGER` and `VARCHAR` are not.
//
// This is not a detail. Both readers upper-case: Ptah's SQLite reader
// upper-cases every type it reads, and its PostgreSQL reader reports
// `NUMERIC(10,2)` and `VARCHAR(100)` for an ordinary table. Before this, every
// such column rendered as HCL that binary could not read -- not just the
// user-defined column the conformance fixture happened to compare.
//
// A dialect with no measured list keeps whatever the IR holds.
func modeledColumnType(dialect, columnType string) (string, bool) {
	modeled, known := modeledColumnTypes[platform.NormalizeDialect(dialect)]
	if !known {
		return columnType, true
	}
	trimmed := strings.TrimSpace(columnType)
	if trimmed == "" {
		return trimmed, true
	}
	name, argument := trimmed, ""
	if open := strings.IndexByte(trimmed, '('); open >= 0 {
		name, argument = strings.TrimSpace(trimmed[:open]), trimmed[open:]
	}
	lowered := strings.ToLower(name)
	if _, ok := modeled[lowered]; !ok {
		return "", false
	}
	return lowered + argument, true
}
