// Package mssqlproperty owns SQL Server extended properties: the name and
// value pairs sp_addextendedproperty attaches to the database, a schema, a
// table or a column. It holds the desired and observed models with their
// codecs, the change and the operation, and every stage's service, which
// engine/builtin registers on the SQL Server target. No other target knows
// what an extended property is.
package mssqlproperty

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// Owner is the provider identity the bundled runtime registers this owner
// under.
const Owner = "ptah.run/mssql/extended-properties"

// Kind identifies one SQL Server extended property.
const Kind schemaext.Kind = "ptah.run/mssql/extended-property"

// Property is one extended property and the object it is attached to. A
// property with no schema is on the database, one with a schema and no table
// is on the schema, one with a table is on the table, and one with a column is
// on that column of the table.
type Property struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Column string `json:"column,omitempty"`
	Name   string `json:"name"`
	// Value is the text the property holds, written back as an N'' literal.
	Value string `json:"value"`
}

// DesiredProperty is an extended property a declaration asks for.
type DesiredProperty struct {
	Property
	// Comment documents the declaration; a script writes it above the
	// statement and the server keeps nothing of it.
	Comment string `json:"comment,omitempty"`
	// StructName is the Go struct that declared the property, empty for
	// another source.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedProperty is an extended property a read found. A property held
// under a value type Ptah cannot write back is not one: the read records it in
// coverage as unrepresentable instead, see [UnrepresentableValue].
type ObservedProperty struct {
	Property
	// ValueType is the sql_variant base type SQL_VARIANT_PROPERTY reports,
	// such as nvarchar, in lower case.
	ValueType string `json:"value_type"`
}

// Kind returns the owned identity.
func (*DesiredProperty) Kind() schemaext.Kind { return Kind }

// Kind returns the owned identity.
func (*ObservedProperty) Kind() schemaext.Kind { return Kind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredProperty) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredProperty)(nil)
	}
	clone := *v
	return &clone
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedProperty) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedProperty)(nil)
	}
	clone := *v
	return &clone
}

// Equal compares declarations field by field.
func (v *DesiredProperty) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredProperty)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations field by field.
func (v *ObservedProperty) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedProperty)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures an observation as a declaration that keeps its value. A
// nil receiver remains nil.
func (v *ObservedProperty) Desired() *DesiredProperty {
	if v == nil {
		return nil
	}
	return &DesiredProperty{Property: v.Property}
}

// UnrepresentableValue is the knowledge a read records for a property held
// under a value type Ptah cannot write back, such as int or date:
// sp_addextendedproperty takes a sql_variant, and an N'...' literal would turn
// the value into text. A comparison neither changes nor drops such a
// property, and does not add a declaration of the same one.
func UnrepresentableValue(valueType string) schemaext.Knowledge {
	return schemaext.Knowledge{State: schemaext.Unrepresentable,
		Reason: fmt.Sprintf("the property holds a %s value, which Ptah cannot write back", valueType)}
}

// Observed projects a declaration as the property a statement leaves: the
// text it declares, held as nvarchar.
func (v *DesiredProperty) Observed() (*ObservedProperty, error) {
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	return &ObservedProperty{Property: v.Property, ValueType: "nvarchar"}, nil
}

// Label names the property and the object it is attached to, as
// schema.table.column/name, with "(database)" for a database property.
func (p Property) Label() string {
	if p.Schema == "" {
		return "(database)/" + p.Name
	}
	parts := []string{p.Schema}
	if p.Table != "" {
		parts = append(parts, p.Table)
	}
	if p.Column != "" {
		parts = append(parts, p.Column)
	}
	return strings.Join(parts, ".") + "/" + p.Name
}

// Ref is the identity of the property. Every part is folded to lower case:
// SQL Server's default collation compares names without case, so a
// declaration that spells a table `Docs` and a read that reports `docs` name
// one table. The schema and the name take their slots, and the table and the
// column the signature, each quoted so no spelling of one can pass for
// another. The parent slot stays empty: a property is not a child the table's
// own transition creates or drops, and its owner plans it alone, ordered by
// what it reads.
func (p Property) Ref() objectidentity.ID {
	ref := objectidentity.ID{Kind: objectidentity.Kind(Kind), Schema: part(p.Schema), Name: part(p.Name)}
	if strings.TrimSpace(p.Table) != "" {
		ref.Signature = strconv.Quote(fold(p.Table)) + " " + strconv.Quote(fold(p.Column))
	}
	return ref
}

// RefAddress recovers the property [Property.Ref] made an identity from: its
// schema and name as written, and its table and column folded, with no value.
// ok is false for any other identity.
func RefAddress(ref objectidentity.ID) (Property, bool) {
	if ref.Kind != objectidentity.Kind(Kind) {
		return Property{}, false
	}
	address := Property{Schema: ref.Schema.Source, Name: ref.Name.Source}
	if ref.Signature == "" {
		return address, true
	}
	quotedTable, quotedColumn, found := strings.Cut(ref.Signature, " ")
	table, tableErr := strconv.Unquote(quotedTable)
	column, columnErr := strconv.Unquote(quotedColumn)
	if !found || tableErr != nil || columnErr != nil {
		return Property{}, false
	}
	address.Table, address.Column = table, column
	return address, true
}

func part(value string) objectidentity.Part {
	if value == "" {
		return objectidentity.Part{}
	}
	return objectidentity.Part{Source: value, Normalized: fold(value)}
}

func fold(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

// DesiredObject records one declared property.
func DesiredObject(property DesiredProperty) schemaext.Object {
	return schemaext.Object{Ref: property.Ref(), Value: property.Clone()}
}

// DeclaredObject records one property a source declares, bound to SQL
// Server: an extended property is a SQL Server object, so a schema rendered or
// planned for another target leaves it out rather than refusing it.
func DeclaredObject(property DesiredProperty) schemaext.Object {
	object := DesiredObject(property)
	object.Targets = []string{platform.SQLServer}
	return object
}

// ObservedObject records one property a read found.
func ObservedObject(property ObservedProperty) schemaext.Object {
	return schemaext.Object{Ref: property.Ref(), Value: property.Clone()}
}

// ValidateDesired refuses a declaration SQL Server cannot hold; see
// [Validate]. Nil is invalid.
func ValidateDesired(v *DesiredProperty) error {
	if v == nil {
		return invalid(schemaext.Desired, fmt.Errorf("%w: nil extended property declaration", schemaext.ErrInvalidValue))
	}
	return invalid(schemaext.Desired, Validate(v.Property))
}

// ValidateObserved refuses an observation SQL Server cannot report; see
// [Validate]. Nil is invalid.
func ValidateObserved(v *ObservedProperty) error {
	if v == nil {
		return invalid(schemaext.Observed, fmt.Errorf("%w: nil extended property observation", schemaext.ErrInvalidValue))
	}
	return invalid(schemaext.Observed, Validate(v.Property))
}

// Validate refuses a property without a name, a table without the schema
// that holds it, a column without its table, and text that is not valid
// UTF-8 or holds a NUL. Errors wrap schemaext.ErrInvalidValue.
func Validate(p Property) error {
	for _, text := range []struct{ field, value string }{
		{"extended property name", p.Name}, {"extended property schema", p.Schema}, {"extended property table", p.Table},
		{"extended property column", p.Column}, {"extended property value", p.Value},
	} {
		if err := schemaext.ValidText(text.field, text.value); err != nil {
			return err
		}
	}
	switch {
	case strings.TrimSpace(p.Name) == "":
		return fmt.Errorf("%w: an extended property needs a name", schemaext.ErrInvalidValue)
	case strings.TrimSpace(p.Table) != "" && strings.TrimSpace(p.Schema) == "":
		return fmt.Errorf("%w: extended property %q names table %q and no schema; SQL Server addresses a table through the schema that holds it",
			schemaext.ErrInvalidValue, p.Name, p.Table)
	case strings.TrimSpace(p.Column) != "" && strings.TrimSpace(p.Table) == "":
		return fmt.Errorf("%w: extended property %q names column %q and no table; SQL Server addresses a column through the table that holds it",
			schemaext.ErrInvalidValue, p.Name, p.Column)
	}
	return nil
}

// ValidateRef requires the identity [Property.Ref] gives a property.
func ValidateRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(Kind) || ref.Name.Normalized == "" {
		return fmt.Errorf("%w: invalid extended property identity %v", schemaext.ErrInvalidValue, ref)
	}
	return nil
}

func invalid(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: Kind, Representation: representation, Message: err.Error()}
}
