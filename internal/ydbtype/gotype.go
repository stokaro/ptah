package ydbtype

import "strings"

// GoType is the Go type a struct field holding a YDB column is generated as:
// the type name as Go source writes it, and the import path that name needs,
// empty when it needs none.
type GoType struct {
	Name   string
	Import string
}

// decimalImport is the package that holds the one Go type ydb-go-sdk scans a
// Decimal into.
const decimalImport = "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

// goTypes are the Go types of the YDB types the schema reader reports, and of
// the Serial types a declaration may name.
//
// Each was measured on local-ydb 26.2.1.14 through ydb-go-sdk v3.153.2's
// database/sql driver, by selecting a value of the type and scanning it into
// `any`: the value the driver hands back is the type below. The exceptions
// are deliberate. Json and JsonDocument come back as []byte and are generated
// as string, which the driver also scans into and which is what a JSON
// document is. A Decimal comes back as a value of an unexported SDK type, and
// scanning it into a string or a float64 fails (`unsupported Scan, storing
// driver.Value type *value.decimalValue into type *string`); the SDK's
// types.Decimal is the one Go type it scans into.
var goTypes = map[string]GoType{
	Bool:         {Name: "bool"},
	Int8:         {Name: "int8"},
	Int16:        {Name: "int16"},
	Int32:        {Name: "int32"},
	Int64:        {Name: "int64"},
	Uint8:        {Name: "uint8"},
	Uint16:       {Name: "uint16"},
	Uint32:       {Name: "uint32"},
	Uint64:       {Name: "uint64"},
	Float:        {Name: "float32"},
	Double:       {Name: "float64"},
	DyNumber:     {Name: "string"},
	String:       {Name: "[]byte"},
	Utf8:         {Name: "string"},
	JSON:         {Name: "string"},
	JSONDocument: {Name: "string"},
	Yson:         {Name: "[]byte"},
	UUID:         {Name: "string"},
	Date:         {Name: "time.Time", Import: "time"},
	Datetime:     {Name: "time.Time", Import: "time"},
	Timestamp:    {Name: "time.Time", Import: "time"},
	Date32:       {Name: "time.Time", Import: "time"},
	Datetime64:   {Name: "time.Time", Import: "time"},
	Timestamp64:  {Name: "time.Time", Import: "time"},
	Interval:     {Name: "time.Duration", Import: "time"},
	Interval64:   {Name: "time.Duration", Import: "time"},
	SmallSerial:  {Name: "int16"},
	Serial:       {Name: "int32"},
	BigSerial:    {Name: "int64"},
}

// GoTypeOf answers the Go type a field holding a column of ydbType is
// generated as. ydbType is a YDB spelling as the schema reader reports it and
// [Map] writes it: Utf8, Uint64, Decimal(22,9). It answers false for a name
// that is not a YDB type, including a declaration such as VARCHAR(255), which
// [Map] has to read first.
func GoTypeOf(ydbType string) (GoType, bool) {
	trimmed := strings.TrimSpace(ydbType)
	if base, _, ok := splitArguments(trimmed); ok && base == "Decimal" {
		return GoType{Name: "types.Decimal", Import: decimalImport}, true
	}
	goType, ok := goTypes[trimmed]
	return goType, ok
}
