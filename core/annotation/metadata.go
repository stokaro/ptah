// Package annotation is the contract between the Go annotation frontend and
// the feature owners that declare directives of their own.
//
// The frontend, ptah.run/core/goschema, parses the directives every target
// shares. A directive that belongs to one feature, such as a TimescaleDB
// hypertable, is declared, decoded and covered by that feature's owner: the
// owner describes the directive's grammar in a [Directive], turns one parsed
// [Declaration] into the feature objects and table facets it declares, and
// states the knowledge a Go annotation source holds about the models it
// produces. A [Set] freezes the owners one parse selects. The frontend imports
// no owner; whoever assembles the providers hands it the set.
package annotation

import (
	"strings"

	"ptah.run/internal/tableref"
)

// Scope is where in Go source a directive may be attached.
type Scope string

// The scopes a directive may name.
const (
	// ScopeFile is a comment of the file outside any declaration.
	ScopeFile Scope = "file"
	// ScopeStruct is the doc comment of a struct type.
	ScopeStruct Scope = "struct"
	// ScopeField is the doc comment of a struct field.
	ScopeField Scope = "field"
)

// Attribute describes one attribute of a directive.
type Attribute struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Value names the value's shape for documentation and the editor, such
	// as "string" or "boolean".
	Value    string `json:"value"`
	Required bool   `json:"required,omitempty"`
	// Boolean marks an attribute that may be written bare, meaning true.
	Boolean bool `json:"boolean,omitempty"`
	// AliasFor names the attribute this one is another spelling of.
	AliasFor string `json:"alias_for,omitempty"`
	// Sensitive marks an attribute whose VALUE is a credential. Display
	// surfaces must redact it; planning and destructive writes keep using the
	// original bytes.
	Sensitive bool `json:"sensitive,omitempty"`
	// Retired carries the reason an attribute Ptah still recognizes is
	// refused. Empty for every ordinary attribute.
	//
	// Recognizing it is the point. Deleting the attribute instead would make
	// the parser answer "unknown annotation attribute", which reads as a typo
	// and says nothing about why a correctly spelled attribute is refused, and
	// a bare spelling of it would be dropped rather than refused
	// (stokaro/ptah#1625).
	Retired string `json:"retired,omitempty"`
}

// Directive describes one //ptah annotation directive.
type Directive struct {
	// Name is the directive without the comment marker, such as
	// "ptah:schema:hypertable".
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Scopes      []Scope     `json:"scopes"`
	Attributes  []Attribute `json:"attributes"`
	// AllowPlatform admits platform.<dialect>.<key> attributes beside the
	// declared ones.
	AllowPlatform bool `json:"allow_platform,omitempty"`
}

// Clone returns an independent copy of the directive.
func (d Directive) Clone() Directive {
	d.Scopes = append([]Scope(nil), d.Scopes...)
	d.Attributes = append([]Attribute(nil), d.Attributes...)
	return d
}

// QualifiedName splits a directive's name the way the frontend splits a table
// directive's. A schema attribute names the schema, and the name keeps its
// spelling apart from surrounding quotes. Without one, a schema-qualified
// name such as `metrics.hourly` names the schema too, rather than an object
// whose name holds a dot.
func QualifiedName(schema, name string) (schemaName, objectName string) {
	schemaName = strings.TrimSpace(schema)
	objectName = strings.TrimSpace(name)
	if schemaName != "" {
		if ref, ok := tableref.Parse(objectName); ok && !ref.Qualified {
			objectName = ref.Name
		}
		return schemaName, objectName
	}
	ref, ok := tableref.Parse(objectName)
	if !ok {
		return "", objectName
	}
	if !ref.Qualified {
		return "", ref.Name
	}
	return ref.Schema, ref.Name
}
