package schemamodel

import (
	"ptah.run/core/ast"
	"ptah.run/internal/tableref"
)

// StreamingQuery declares a YDB streaming query at a database-relative path.
// Changing its text resets aggregation state and requires AllowStateReset.
// Read offsets are retained by YDB's ALTER operation.
type StreamingQuery struct {
	// StructName associates the declaration with its Go annotation holder.
	StructName string
	// Name is the final path segment.
	Name string
	// Schema is the database-relative directory, or empty at the root.
	Schema string
	// Spec contains the query text and persistent settings.
	Spec ast.StreamingQuerySpec
	// AllowStateReset explicitly permits a text change to reset aggregation
	// state. It is a planning permission, not persistent database metadata.
	AllowStateReset bool `json:",omitempty"`
}

// QualifiedName returns the canonical directory-qualified reference.
func (s StreamingQuery) QualifiedName() string { return tableref.Canonical(s.Schema, s.Name) }
