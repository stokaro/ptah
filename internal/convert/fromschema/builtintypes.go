package fromschema

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// namesABuiltInType reports whether typeName is a name the target dialect
// resolves through its own catalog rather than through a declaration in the
// document.
//
// It is the difference between "this column's type IS the declared user type"
// and "this column's type name COLLIDES with one", and nothing else in the IR
// can tell them apart. A column carries a type name, not a type identity; the
// declaration carries the schema. Matching the two by bare name alone retypes
// a built-in column the moment somebody declares a user type that answers to
// the same word, and PostgreSQL has plenty of one-word type names people reuse
// -- money, point, line, box, path, text, name, date, interval, json, xml.
//
// Measured on PostgreSQL 17.10, source seeded by hand:
//
//	CREATE SCHEMA advm; CREATE DOMAIN advm.money AS numeric(12,2);
//	CREATE TABLE advm.prices (id integer, domain_col advm.money, builtin_col money);
//
// The read is unambiguous -- inspect writes `type = money` for the built-in
// column and `type = sql("advm.money")` for the domain column -- and without
// this predicate the plan was not: `builtin_col` came back as `advm.money`,
// replayed at exit 0, and the catalog of the replayed database reported
// `advm.money | advm | d` where the source had `money | pg_catalog | b`. A base
// type became a domain silently. The same happened for `money[]`, for a
// composite named `point` in both spellings, and -- through [handleEnumTypes],
// which is stokaro/ptah#1276 rather than stokaro/ptah#1138 -- for an enum named
// `money`.
//
// The direction this predicate refuses in is deliberate. A name that could mean
// either thing is left exactly as the source spelled it, which is what the rest
// of [declaredUserTypeQualifiers] already does for a name two schemas both
// declare. Leaving a built-in alone costs a user type whose name shadows a
// catalog type its qualifier, and that column plans the way it planned before
// stokaro/ptah#1138. Rewriting it the other way changes a column's type in a
// live database and reports success.
//
// # Where this predicate is NOT applied, and why
//
// [handleEnumTypes] re-points a bare enum name and is left unguarded, so the
// SCALAR spelling of an enum whose name shadows a catalog type still resolves to
// the enum. That line is stokaro/ptah#1276's, it predates stokaro/ptah#1138, and
// guarding it would not help: measured on PostgreSQL 17.10 with
// `CREATE TYPE adve.money AS ENUM ('lo','hi')` beside an ordinary `money`
// column, an inspected document writes `type = enum.money` for BOTH columns,
// because the renderer resolves a column type against the declared enum blocks.
// By the time a name reaches that line the two columns are one string, so a
// guard would only move which of them is wrong -- and on the annotation path it
// would be a plain regression, because `type="money"` beside a declared enum
// `money` is an author naming their own type.
//
// The array spelling of the same enum IS guarded, because there the catalog
// keeps the two apart: the same read wrote `sql("money[]")` for the built-in
// array and `sql("adve.money[]")` for the enum array.
// TestFromDatabaseKeepsTheScalarEnumHalfWithIssue1276 pins both halves so this
// split is a decision rather than an accident.
func namesABuiltInType(dialect, typeName string) bool {
	names, known := builtInTypeNames[platform.NormalizeDialect(dialect)]
	if !known {
		return false
	}
	trimmed := strings.TrimSpace(typeName)
	if open := strings.IndexByte(trimmed, '('); open >= 0 {
		trimmed = strings.TrimSpace(trimmed[:open])
	}
	_, found := names[strings.ToLower(trimmed)]
	return found
}

// builtInTypeNames maps a dialect to the type names its own catalog answers to.
//
// A dialect absent from this map treats no name as built in, which is exactly
// the behavior it had before stokaro/ptah#1138 -- the same convention
// internal/atlashclrender/modeled_types.go uses for the same reason. Only
// PostgreSQL is listed because only PostgreSQL was measured; CockroachDB,
// YugabyteDB and Spanner reuse much of the PostgreSQL type namespace, and
// listing them here on that resemblance would be a claim nobody ran.
var builtInTypeNames = map[string]map[string]struct{}{
	platform.Postgres: builtInNameSet(postgresCatalogTypeNames, postgresGrammarTypeNames),
}

func builtInNameSet(lists ...[]string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, names := range lists {
		for _, name := range names {
			set[strings.ToLower(name)] = struct{}{}
		}
	}
	return set
}

// postgresCatalogTypeNames is every non-array type name pg_catalog holds,
// read out of a live server rather than remembered:
//
//	SELECT typname FROM pg_type
//	WHERE typnamespace = 'pg_catalog'::regnamespace
//	  AND typtype IN ('b', 'r', 'm', 'p')
//	  AND typname NOT LIKE '\_%'
//	ORDER BY typname;
//
// run against PostgreSQL 17.10, which returned these 107 rows. Array names are
// excluded because [splitArraySuffix] has already removed the brackets by the
// time this list is consulted, and pg_catalog spells an array type `_money`
// rather than `money[]`.
//
// Internal and pseudo names -- cstring, internal, record, trigger, void and the
// anycompatible family -- are kept rather than filtered. They cost nothing: the
// only effect of a name being here is that a user type declared under it is not
// re-pointed, and a schema that declares a composite called `record` is a schema
// where refusing to guess is right.
//
// Unlike internal/atlashclrender/modeledColumnTypes, this list has no
// conformance run behind it, and it does not need one, because its two failure
// directions are not symmetric and neither is worse than not having the list:
//
//   - a name here that is NOT built in costs that user type its qualifier, which
//     is how the column planned before stokaro/ptah#1138
//   - a name MISSING from here re-exposes stokaro/ptah#1138's collision for that
//     one name, and only when a schema declares a user type answering to it
//
// So a list that is too long is safe and a list that is too short degrades to
// the previous behavior, name by name. No entry can produce a spelling the
// source did not already carry. That is the opposite of the modeled-types table,
// where a wrong row makes a document the pinned binary cannot read.
var postgresCatalogTypeNames = []string{
	"aclitem", "any", "anyarray", "anycompatible", "anycompatiblearray",
	"anycompatiblemultirange", "anycompatiblenonarray", "anycompatiblerange",
	"anyelement", "anyenum", "anymultirange", "anynonarray", "anyrange", "bit",
	"bool", "box", "bpchar", "bytea", "char", "cid", "cidr", "circle", "cstring",
	"date", "datemultirange", "daterange", "event_trigger", "fdw_handler",
	"float4", "float8", "gtsvector", "index_am_handler", "inet", "int2",
	"int2vector", "int4", "int4multirange", "int4range", "int8", "int8multirange",
	"int8range", "internal", "interval", "json", "jsonb", "jsonpath",
	"language_handler", "line", "lseg", "macaddr", "macaddr8", "money", "name",
	"numeric", "nummultirange", "numrange", "oid", "oidvector", "path",
	"pg_brin_bloom_summary", "pg_brin_minmax_multi_summary", "pg_ddl_command",
	"pg_dependencies", "pg_lsn", "pg_mcv_list", "pg_ndistinct", "pg_node_tree",
	"pg_snapshot", "point", "polygon", "record", "refcursor", "regclass",
	"regcollation", "regconfig", "regdictionary", "regnamespace", "regoper",
	"regoperator", "regproc", "regprocedure", "regrole", "regtype",
	"table_am_handler", "text", "tid", "time", "timestamp", "timestamptz",
	"timetz", "trigger", "tsm_handler", "tsmultirange", "tsquery", "tsrange",
	"tstzmultirange", "tstzrange", "tsvector", "txid_snapshot", "unknown", "uuid",
	"varbit", "varchar", "void", "xid", "xid8", "xml",
}

// postgresGrammarTypeNames are the spellings PostgreSQL's parser accepts that
// pg_catalog does not store under that name: the SQL standard aliases and the
// SERIAL shorthands.
//
// They are a separate list because the query above cannot produce them:
// `integer` is int4 in the catalog and `bigserial` is not a type at all. Each
// was measured on the same PostgreSQL 17.10 server instead. The seventeen
// aliases through to_regtype, every one resolving into pg_catalog:
//
//	SELECT n.name, tn.nspname FROM (VALUES ('bigint'), ...) AS n(name)
//	LEFT JOIN pg_type t ON t.oid = to_regtype(n.name)
//	LEFT JOIN pg_namespace tn ON tn.oid = t.typnamespace;
//
// and the six SERIAL spellings through a CREATE TABLE, because to_regtype does
// not know them:
//
//	CREATE TABLE p (a serial, b serial2, c serial4,
//	                d serial8, e bigserial, f smallserial);
//	  -> integer, smallint, integer, bigint, bigint, smallint, all pg_catalog
var postgresGrammarTypeNames = []string{
	"bigint", "bigserial", "bit varying", "boolean", "character",
	"character varying", "decimal", "double precision", "int", "integer",
	"national character", "national character varying", "real", "serial",
	"serial2", "serial4", "serial8", "smallint", "smallserial",
	"time with time zone", "time without time zone",
	"timestamp with time zone", "timestamp without time zone",
}
