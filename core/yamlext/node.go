package yamlext

// NodeKind describes the syntax shape of a captured YAML value. The zero value
// describes no node. The contract does not depend on a YAML parser library.
type NodeKind string

const (
	// ScalarNode holds a scalar, including an explicitly authored null.
	ScalarNode NodeKind = "scalar"
	// MappingNode holds alternating key and value nodes in Content.
	MappingNode NodeKind = "mapping"
	// SequenceNode holds ordered values in Content.
	SequenceNode NodeKind = "sequence"
	// AliasNode refers to the node named by a YAML alias.
	AliasNode NodeKind = "alias"
)

// Node preserves the source syntax that an owner interprets. Numbers retain
// their authored scalar spelling and are never converted through floating point.
// Explicit numeric tags do not impose machine integer or floating-point ranges.
// Tags distinguish strings, numbers, booleans and explicit nulls. Nodes may
// share alias targets. Original bytes, quoting and comments remain available in
// Document.Source; Line and Column locate the value in those bytes.
type Node struct {
	// Kind selects the node shape. The zero value is invalid.
	Kind NodeKind
	// Tag is the resolved YAML tag, such as !!str, !!int or !!null.
	Tag string
	// Value is decoded scalar text, or the anchor name on an alias node.
	Value string
	// Anchor names an anchor declared on this node, or is empty.
	Anchor string
	// Content holds mapping key/value pairs or sequence values in source order.
	Content []*Node
	// Alias is the referenced node for AliasNode and nil for other shapes.
	Alias *Node
	// Line is the one-based source line where the value starts.
	Line int
	// Column is the one-based source character column where the value starts.
	Column int
}
