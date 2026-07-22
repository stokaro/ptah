package graphqlrender

import "github.com/stokaro/ptah/internal/schemaexport"

// Built-in and custom GraphQL scalar names.
const (
	scalarInt      = "Int"
	scalarFloat    = "Float"
	scalarString   = "String"
	scalarBoolean  = "Boolean"
	scalarID       = "ID"
	scalarDateTime = "DateTime" // custom
	scalarJSON     = "JSON"     // custom
)

// graphQLScalar is the GraphQL scalar a Ptah column type maps to.
type graphQLScalar struct {
	Name string
	// Custom names a custom scalar that must be declared (DateTime, JSON) when
	// this mapping is used; empty for built-in scalars.
	Custom string
	// Known is false when the source type was not recognized and defaulted to
	// String, so the caller can emit a diagnostic.
	Known bool
}

// mapGraphQLScalar maps a Ptah column type to a GraphQL scalar. Integer widths
// collapse to Int, exact/approximate numerics to Float, date/time types to a
// custom DateTime scalar and json to a custom JSON scalar. Unknown types default
// to String with Known false.
func mapGraphQLScalar(raw string) graphQLScalar {
	base, _ := schemaexport.NormalizeType(raw)

	switch base {
	case "SMALLINT", "SMALLSERIAL", "SERIAL2", "INT2", "TINYINT", "YEAR",
		"INT", "INTEGER", "INT4", "SERIAL", "SERIAL4", "MEDIUMINT",
		"BIGINT", "BIGSERIAL", "SERIAL8", "INT8":
		return graphQLScalar{Name: scalarInt, Known: true}
	case "BOOL", "BOOLEAN":
		return graphQLScalar{Name: scalarBoolean, Known: true}
	case "DECIMAL", "NUMERIC", "MONEY", "REAL", "FLOAT4",
		"DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8":
		return graphQLScalar{Name: scalarFloat, Known: true}
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "BPCHAR", "NCHAR", "NVARCHAR",
		"TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "CLOB", "CITEXT", "UUID",
		"INET", "CIDR", "MACADDR", "MACADDR8",
		"BYTEA", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "BINARY", "VARBINARY", "BIT":
		return graphQLScalar{Name: scalarString, Known: true}
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE",
		"TIME", "TIMETZ", "TIME WITH TIME ZONE", "TIME WITHOUT TIME ZONE":
		return graphQLScalar{Name: scalarDateTime, Custom: scalarDateTime, Known: true}
	case "JSON", "JSONB":
		return graphQLScalar{Name: scalarJSON, Custom: scalarJSON, Known: true}
	default:
		return graphQLScalar{Name: scalarString, Known: false}
	}
}
