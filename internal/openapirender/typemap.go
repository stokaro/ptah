package openapirender

import (
	"strconv"
	"strings"

	"github.com/stokaro/ptah/internal/schemaexport"
)

// openAPIType is the OpenAPI Schema Object shape a Ptah column type maps to.
type openAPIType struct {
	Type      string
	Format    string
	MaxLength *int
	// Minimum is set (to 0) for unsigned integer types so a consumer knows the
	// value is non-negative even though OpenAPI has no unsigned format.
	Minimum *int
	// Known is false when the source type was not recognized and was defaulted
	// to string, so the caller can emit a diagnostic.
	Known bool
}

// mapOpenAPIType maps a Ptah column type (e.g. "VARCHAR(255)", "BIGINT",
// "TIMESTAMP") to an OpenAPI 3.0 Schema Object type/format, extracting a
// maxLength from character types. Unknown types default to string with Known
// false. The lookup is dialect-agnostic: the Postgres and MySQL spellings Ptah
// emits (SERIAL, "INT AUTO_INCREMENT", DOUBLE PRECISION) all normalize here.
func mapOpenAPIType(raw string) openAPIType {
	base, args := schemaexport.NormalizeType(raw)
	mapped := mapOpenAPIBase(base, args)
	// Unsigned integers are non-negative; OpenAPI has no unsigned format, so
	// record a minimum of 0. NormalizeType strips the modifier, so detect it on
	// the raw type.
	if mapped.Type == "integer" && strings.Contains(strings.ToUpper(raw), "UNSIGNED") {
		zero := 0
		mapped.Minimum = &zero
	}
	return mapped
}

func mapOpenAPIBase(base string, args []string) openAPIType {
	switch base {
	case "SMALLINT", "SMALLSERIAL", "SERIAL2", "INT2", "TINYINT", "YEAR":
		return openAPIType{Type: "integer", Format: "int32", Known: true}
	case "INT", "INTEGER", "INT4", "SERIAL", "SERIAL4", "MEDIUMINT":
		return openAPIType{Type: "integer", Format: "int32", Known: true}
	case "BIGINT", "BIGSERIAL", "SERIAL8", "INT8":
		return openAPIType{Type: "integer", Format: "int64", Known: true}
	case "BOOL", "BOOLEAN":
		return openAPIType{Type: "boolean", Known: true}
	case "DECIMAL", "NUMERIC", "MONEY":
		return openAPIType{Type: "number", Known: true}
	case "REAL", "FLOAT4":
		return openAPIType{Type: "number", Format: "float", Known: true}
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8":
		return openAPIType{Type: "number", Format: "double", Known: true}
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "BPCHAR", "NCHAR", "NVARCHAR":
		return openAPIType{Type: "string", MaxLength: firstIntArg(args), Known: true}
	case "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "CLOB", "CITEXT":
		return openAPIType{Type: "string", Known: true}
	case "UUID":
		return openAPIType{Type: "string", Format: "uuid", Known: true}
	case "DATE":
		return openAPIType{Type: "string", Format: "date", Known: true}
	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE":
		return openAPIType{Type: "string", Format: "date-time", Known: true}
	case "TIME", "TIMETZ", "TIME WITH TIME ZONE", "TIME WITHOUT TIME ZONE":
		return openAPIType{Type: "string", Known: true}
	case "JSON", "JSONB":
		return openAPIType{Type: "object", Known: true}
	case "BYTEA", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "BINARY", "VARBINARY", "BIT":
		return openAPIType{Type: "string", Format: "byte", Known: true}
	case "INET", "CIDR", "MACADDR", "MACADDR8":
		return openAPIType{Type: "string", Known: true}
	default:
		return openAPIType{Type: "string", Known: false}
	}
}

func firstIntArg(args []string) *int {
	if len(args) == 0 {
		return nil
	}
	n, err := strconv.Atoi(args[0])
	if err != nil {
		return nil
	}
	return &n
}
