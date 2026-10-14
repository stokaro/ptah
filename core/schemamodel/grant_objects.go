package schemamodel

import "strings"

// The kinds of object [Grant.OnObject] names, as [Grant.ObjectKind], a
// catalog grant's object type and a GRANT node spell them. Each is a YDB
// scheme object with an access list of its own.
const (
	// GrantObjectTopic is a standalone topic.
	GrantObjectTopic = "TOPIC"
	// GrantObjectCoordinationNode is a coordination node.
	GrantObjectCoordinationNode = "COORDINATION NODE"
	// GrantObjectSecret is a secret.
	GrantObjectSecret = "SECRET"
	// GrantObjectExternalDataSource is an external data source.
	GrantObjectExternalDataSource = "EXTERNAL DATA SOURCE"
	// GrantObjectExternalTable is an external table.
	GrantObjectExternalTable = "EXTERNAL TABLE"
)

// GrantObjectKind is one kind of object [Grant.OnObject] names, with the
// attribute a declaration names such an object by.
type GrantObjectKind struct {
	// Kind is the kind as [Grant.ObjectKind] spells it, such as TOPIC.
	Kind string
	// Attribute is the attribute of a grant or revoke annotation, and the key
	// of a YAML grant or revoke, that names an object of the kind, such as
	// on_topic.
	Attribute string
}

// grantObjectKinds is every kind OnObject names, in the order a declaration
// lists its attributes.
var grantObjectKinds = []GrantObjectKind{
	{Kind: GrantObjectTopic, Attribute: "on_topic"},
	{Kind: GrantObjectCoordinationNode, Attribute: "on_coordination_node"},
	{Kind: GrantObjectSecret, Attribute: "on_secret"},
	{Kind: GrantObjectExternalDataSource, Attribute: "on_external_data_source"},
	{Kind: GrantObjectExternalTable, Attribute: "on_external_table"},
}

// GrantObjectKinds returns every kind of object [Grant.OnObject] names, each
// with the attribute it is declared with, in a fixed order. The annotation
// parser, the YAML reader and the Go annotation writer read this one list, so
// a kind one of them takes the others take too. The slice is a copy.
func GrantObjectKinds() []GrantObjectKind {
	return append([]GrantObjectKind(nil), grantObjectKinds...)
}

// GrantObjectKindOf returns the kind of object attribute names, and false for
// an attribute that names none of [GrantObjectKinds].
func GrantObjectKindOf(attribute string) (string, bool) {
	for _, kind := range grantObjectKinds {
		if kind.Attribute == strings.TrimSpace(attribute) {
			return kind.Kind, true
		}
	}
	return "", false
}

// GrantObjectAttribute returns the attribute an object of kind is declared
// with, and false for a kind that is none of [GrantObjectKinds]. The kind is
// matched as [Grant.Canonicalize] writes it, without regard to case or
// spacing.
func GrantObjectAttribute(kind string) (string, bool) {
	normalized := strings.Join(strings.Fields(strings.ToUpper(kind)), " ")
	for _, known := range grantObjectKinds {
		if known.Kind == normalized {
			return known.Attribute, true
		}
	}
	return "", false
}
