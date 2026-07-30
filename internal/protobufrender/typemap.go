package protobufrender

import (
	"strings"

	"github.com/stokaro/ptah/internal/schemaexport"
)

// Well-known type imports the exporter can emit.
const (
	importTimestamp = "google/protobuf/timestamp.proto"
	importStruct    = "google/protobuf/struct.proto"
)

// protoType is the Protobuf type a Ptah column type maps to.
type protoType struct {
	// Name is the type as written in the .proto file.
	Name string
	// Import is the file the type needs, empty for built-in scalars.
	Import string
	// Known is false when the source type was not recognized and was defaulted
	// to string, so the caller can emit a diagnostic.
	Known bool
	// Lossy carries a diagnostic message for a recognized-but-lossy mapping,
	// empty when the projection is faithful.
	Lossy string
}

// mapProtoType maps a Ptah column type (e.g. "VARCHAR(255)", "BIGINT",
// "TIMESTAMPTZ") to a Protobuf type. The lookup is dialect-agnostic and mirrors
// internal/openapirender/typemap.go so the API export targets agree on what a
// column is. Arrays are handled by the caller through schemaexport.ElementType.
func mapProtoType(raw string) protoType {
	base, _ := schemaexport.NormalizeType(raw)
	mapped := mapProtoBase(base)
	// NormalizeType strips the modifier, so unsigned is detected on the raw
	// type exactly as the OpenAPI exporter does.
	if strings.Contains(strings.ToUpper(raw), "UNSIGNED") {
		switch mapped.Name {
		case "int32":
			mapped.Name = "uint32"
		case "int64":
			mapped.Name = "uint64"
		}
	}
	return mapped
}

func mapProtoBase(base string) protoType {
	switch base {
	case "SMALLINT", "SMALLSERIAL", "SERIAL2", "INT2", "TINYINT", "YEAR",
		"INT", "INTEGER", "INT4", "SERIAL", "SERIAL4", "MEDIUMINT":
		return protoType{Name: "int32", Known: true}
	case "BIGINT", "BIGSERIAL", "SERIAL8", "INT8":
		return protoType{Name: "int64", Known: true}
	case "BOOL", "BOOLEAN":
		return protoType{Name: "bool", Known: true}
	case "REAL", "FLOAT4":
		return protoType{Name: "float", Known: true}
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8":
		return protoType{Name: "double", Known: true}
	case "DECIMAL", "NUMERIC", "MONEY":
		return protoType{
			Name:  "string",
			Known: true,
			Lossy: "exact numeric mapped to string; Protobuf has no decimal type and float/double would lose precision",
		}
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "BPCHAR", "NCHAR", "NVARCHAR",
		"TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "CLOB", "CITEXT",
		"UUID", "INET", "CIDR", "MACADDR", "MACADDR8":
		return protoType{Name: "string", Known: true}
	case "BYTEA", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "BINARY", "VARBINARY", "BIT":
		return protoType{Name: "bytes", Known: true}
	case "JSON", "JSONB":
		return protoType{Name: "google.protobuf.Value", Import: importStruct, Known: true}
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return protoType{Name: "google.protobuf.Timestamp", Import: importTimestamp, Known: true}
	case "TIMESTAMP", "DATETIME", "TIMESTAMP WITHOUT TIME ZONE":
		return protoType{
			Name:  "string",
			Known: true,
			Lossy: "timezone-ambiguous timestamp mapped to string; google.protobuf.Timestamp is only used for types with explicit time-zone semantics",
		}
	case "DATE", "TIME", "TIMETZ", "TIME WITH TIME ZONE", "TIME WITHOUT TIME ZONE":
		return protoType{
			Name:  "string",
			Known: true,
			Lossy: "date/time mapped to string; Protobuf has no wire-native equivalent",
		}
	default:
		return protoType{Name: "string"}
	}
}
