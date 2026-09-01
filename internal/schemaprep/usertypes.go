package schemaprep

import (
	"maps"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
)

// DeclaredUserTypes is the vocabulary used to qualify column references to
// schema-scoped user types.
type DeclaredUserTypes struct {
	Domains        []schemamodel.Domain
	CompositeTypes []schemamodel.CompositeType
	Ranges         []schemamodel.Range
	Enums          []schemamodel.Enum
}

// QualifyDeclaredUserTypes returns a shallow database clone whose fields name
// unambiguous schema-scoped user types. The declaration is never mutated.
//
// The whole schema is required because a column carries only a type name while
// the declaration carries its schema. Bare built-in names and names declared
// more than once stay untouched: guessing there can silently retype a column.
// This was measured on PostgreSQL 17.10 with a user domain named money beside
// a pg_catalog money column (stokaro/ptah#1138).
func QualifyDeclaredUserTypes(database *schemamodel.Database, targetPlatform string) *schemamodel.Database {
	if database == nil {
		return nil
	}
	clone := *database
	clone.Fields = QualifyFieldUserTypes(database.Fields, DeclaredUserTypes{
		Domains:        database.Domains,
		CompositeTypes: database.CompositeTypes,
		Ranges:         database.Ranges,
		Enums:          database.Enums,
	}, targetPlatform)
	return &clone
}

// QualifyFieldUserTypes returns fields whose type names identify the schema of
// an unambiguously declared user type. It preserves already-qualified,
// ambiguous, and built-in names.
func QualifyFieldUserTypes(
	fields []schemamodel.Field,
	declared DeclaredUserTypes,
	targetPlatform string,
) []schemamodel.Field {
	scalars, arrays := declaredUserTypeQualifiers(declared, targetPlatform)
	if len(scalars) == 0 && len(arrays) == 0 {
		return fields
	}
	qualified := make([]schemamodel.Field, len(fields))
	copy(qualified, fields)
	for i := range qualified {
		qualified[i].Type = qualifyUserTypeReference(qualified[i].Type, scalars, arrays)
	}
	return qualified
}

func declaredUserTypeQualifiers(
	declared DeclaredUserTypes,
	targetPlatform string,
) (scalars, arrays map[string]string) {
	declare := func(into map[string]string, name, schema, qualified string) {
		name = strings.TrimSpace(name)
		if name == "" || strings.TrimSpace(schema) == "" {
			return
		}
		if namesABuiltInType(targetPlatform, name) {
			into[name] = ""
			return
		}
		if _, seen := into[name]; seen {
			into[name] = ""
			return
		}
		into[name] = qualified
	}
	base := make(map[string]string)
	for _, domain := range declared.Domains {
		declare(base, domain.Name, domain.Schema, domain.QualifiedName())
	}
	for _, composite := range declared.CompositeTypes {
		declare(base, composite.Name, composite.Schema, composite.QualifiedName())
	}
	for _, rangeType := range declared.Ranges {
		declare(base, rangeType.Name, rangeType.Schema, rangeType.QualifiedName())
	}

	scalars = make(map[string]string, len(base))
	maps.Copy(scalars, base)
	for _, enum := range declared.Enums {
		if name := strings.TrimSpace(enum.Name); name != "" {
			scalars[name] = ""
		}
	}
	if !EmitsStandaloneEnumDefinitions(targetPlatform) {
		return scalars, scalars
	}

	arrays = make(map[string]string, len(base)+len(declared.Enums))
	maps.Copy(arrays, base)
	for _, enum := range declared.Enums {
		name := strings.TrimSpace(enum.Name)
		if strings.TrimSpace(enum.Schema) == "" {
			if name != "" {
				arrays[name] = ""
			}
			continue
		}
		declare(arrays, name, enum.Schema, enum.QualifiedName())
	}
	return scalars, arrays
}

func qualifyUserTypeReference(columnType string, scalars, arrays map[string]string) string {
	trimmed := strings.TrimSpace(columnType)
	name, brackets := splitArraySuffix(trimmed)
	if name == "" || strings.Contains(name, ".") {
		return columnType
	}
	qualifiers := scalars
	if brackets != "" {
		qualifiers = arrays
	}
	qualified := qualifiers[name]
	if qualified == "" {
		return columnType
	}
	return qualified + brackets
}

func splitArraySuffix(columnType string) (name, brackets string) {
	end := len(columnType)
	for end > 0 && columnType[end-1] == ']' {
		open := strings.LastIndexByte(columnType[:end-1], '[')
		if open < 0 || !isArrayDimension(columnType[open+1:end-1]) {
			break
		}
		end = open
	}
	return strings.TrimRight(columnType[:end], " "), columnType[end:]
}

func isArrayDimension(text string) bool {
	for _, r := range strings.TrimSpace(text) {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// EmitsStandaloneEnumDefinitions reports whether the target represents enums
// as schema objects instead of lowering their values into each column.
func EmitsStandaloneEnumDefinitions(targetPlatform string) bool {
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle:
		return false
	default:
		return true
	}
}

// namesABuiltInType guards the dangerous side of an ambiguous bare name. A
// false negative can turn a built-in column into a same-named user type at
// exit 0; a conservative false positive leaves the spelling unchanged and
// fails loudly if the target cannot resolve it.
func namesABuiltInType(dialect, typeName string) bool {
	names := builtInTypeNamesFor(dialect)
	if names == nil {
		return false
	}
	trimmed := strings.TrimSpace(typeName)
	if open := strings.IndexByte(trimmed, '('); open >= 0 {
		trimmed = strings.TrimSpace(trimmed[:open])
	}
	_, found := names[strings.ToLower(trimmed)]
	return found
}

func builtInTypeNamesFor(dialect string) map[string]struct{} {
	if !platform.IsPostgresFamily(dialect) {
		return nil
	}
	return postgresBuiltInTypeNames
}

var postgresBuiltInTypeNames = builtInNameSet(postgresCatalogTypeNames, postgresGrammarTypeNames)

func builtInNameSet(lists ...[]string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, names := range lists {
		for _, name := range names {
			set[strings.ToLower(name)] = struct{}{}
		}
	}
	return set
}

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

var postgresGrammarTypeNames = []string{
	"bigint", "bigserial", "bit varying", "boolean", "character",
	"character varying", "decimal", "double precision", "int", "integer",
	"national character", "national character varying", "real", "serial",
	"serial2", "serial4", "serial8", "smallint", "smallserial",
	"time with time zone", "time without time zone",
	"timestamp with time zone", "timestamp without time zone",
}
