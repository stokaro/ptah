package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// TestFromField_OracleEnumUsesVarchar2WithCheck pins the enum model for Oracle,
// which has no enum type of any kind.
//
// An engine must answer one of the two enumerations in this file: it either
// emits a standalone type or rewrites the column inline. Answering neither
// leaves the column carrying the enum's own name as its type, which is a type
// no Oracle server has. Measured on 23.26:
//
//	CREATE TABLE e (id NUMBER(10), status status_kind)  ->  ORA-00902: invalid datatype
//	CREATE TABLE e (id NUMBER(10), status VARCHAR2(255)
//	                CHECK (status IN ('active', 'archived')))  ->  created
//	INSERT ... VALUES (2, 'bogus')  ->  ORA-02290: check constraint violated
//
// which is the same failure the comment on handleEnumTypes records for
// `--dialect sqlite3` (stokaro/ptah#931 item 1, stokaro/ptah#1875).
func TestFromField_OracleEnumUsesVarchar2WithCheck(t *testing.T) {
	c := qt.New(t)

	field := schemamodel.Field{Name: "status", Type: "enum_status"}
	enums := []schemamodel.Enum{{Name: "enum_status", Values: []string{"active", "blocked"}}}

	column := fromschema.FromField(field, enums, platform.Oracle)

	c.Assert(column.Type, qt.Equals, "VARCHAR2(255)")
	c.Assert(column.Check, qt.Equals, "status IN ('active', 'blocked')")
}

// TestFromField_OracleEnumCheckMatchesTheColumnSpelling pins the one thing that
// makes an Oracle CHECK usable: it has to name the column the same way the
// declaration does.
//
// Measured, the two disagreeing forms are refused and the two agreeing ones are
// accepted, so a reserved column name has to be quoted on BOTH sides:
//
//	"view_count" NUMBER(10) CHECK (view_count >= 0)    ORA-00904, invalid identifier
//	view_count NUMBER(10) CHECK ("view_count" >= 0)    refused
//	view_count NUMBER(10) CHECK (view_count >= 0)      accepted
func TestFromField_OracleEnumCheckMatchesTheColumnSpelling(t *testing.T) {
	c := qt.New(t)

	// "comment" is a word Oracle refuses bare, so the renderer quotes the
	// declaration and the CHECK has to follow it.
	field := schemamodel.Field{Name: "comment", Type: "enum_kind"}
	enums := []schemamodel.Enum{{Name: "enum_kind", Values: []string{"note"}}}

	column := fromschema.FromField(field, enums, platform.Oracle)

	c.Assert(column.Check, qt.Equals, `"comment" IN ('note')`)
}
