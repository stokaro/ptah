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
// Document.Source; Range locates the value in those original bytes.
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
	// Range spans this node's authored content. An alias has its own range;
	// Alias.Range identifies the declaration it references.
	Range SourceRange
}

// SourcePosition locates an original source byte. ByteOffset is zero-based;
// Line and Column are one-based, and Column counts bytes in the source encoding.
// A BOM contributes bytes to the first column. The zero value is unknown.
type SourcePosition struct {
	// ByteOffset indexes Document.Source, including an encoding BOM if present.
	ByteOffset int
	// Line is the YAML source line, treating CRLF as one line break.
	Line int
	// Column counts original bytes from the start of Line.
	Column int
}

// SourceRange bounds authored content with an inclusive Start and exclusive
// End. Collection ranges include their values and any closing flow delimiter.
// A block scalar ends after its last content character, before trailing line
// breaks and comments. An implicit empty scalar can have equal endpoints.
// The zero value is unknown; parsing source produces known ranges.
type SourceRange struct {
	// Start is the first byte of the authored node, including tags and anchors.
	Start SourcePosition
	// End is the first byte after the node's content.
	End SourcePosition
}
