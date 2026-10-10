package mssqlproperty

import (
	"fmt"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
)

// Directive declares an extended property in a Go annotation.
const Directive = "ptah:schema:extendedproperty"

// Annotations is the owner's contribution to the Go annotation frontend: the
// directive, its decoder, and the claim that a Go annotation source describes
// every extended property, so a property the source leaves out is dropped.
//
// There is no dialect attribute: an extended property is a SQL Server object
// and nothing else, so the declaration is bound to SQL Server and another
// target leaves it out.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner:      Owner,
		Directives: []annotation.Directive{directive()},
		Kinds:      []schemaext.Kind{Kind},
		Decode:     decode,
		Coverage: func() (schemaext.Coverage, error) {
			return Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
		},
	}
}

// decode reads one declaration. MS_Description is refused because Ptah
// models it as the object's comment: accepting it here would give one live row
// two owners, the comment comparison and this one, each planning against it
// without seeing the other. An address SQL Server cannot compose, a table
// without its schema or a column without its table, is refused too.
func decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	if strings.EqualFold(strings.TrimSpace(kv["name"]), "MS_Description") {
		return nil, fmt.Errorf("extended property MS_Description is the object comment, which Ptah already manages; " +
			"declare it with the comment attribute of the object it belongs to")
	}
	property := DesiredProperty{StructName: declaration.Struct, Comment: kv["comment"], Property: Property{
		Name: kv["name"], Schema: kv["schema"], Table: kv["table"], Column: kv["column"], Value: kv["value"],
	}}
	if err := Validate(property.Property); err != nil {
		return nil, err
	}
	object := DeclaredObject(property)
	return []annotation.Contribution{{Object: &object, Label: "extended property " + property.Label()}}, nil
}

func directive() annotation.Directive {
	return annotation.Directive{
		Name:        Directive,
		Description: "Declares a SQL Server extended property on the database, a schema, a table, or a column.",
		Scopes:      []annotation.Scope{annotation.ScopeStruct},
		Attributes: []annotation.Attribute{
			{Name: "name", Description: "Property name. MS_Description is refused: it is the object comment, " +
				"which Ptah already manages.", Value: "string", Required: true},
			{Name: "schema", Description: "Schema the property is on, or that owns the addressed table. " +
				"Omit for a database-scoped property.", Value: "string"},
			{Name: "table", Description: "Table the property is on. Requires schema; omit for a schema-scoped " +
				"property.", Value: "string"},
			{Name: "column", Description: "Column the property is on. Requires table.", Value: "string"},
			{Name: "value", Description: "The value, written back as an N'' literal.", Value: "string", Required: true},
			{Name: "comment", Description: "Extended property comment.", Value: "string"},
		},
	}
}
