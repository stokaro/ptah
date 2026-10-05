package catalog

import (
	"ptah.run/core/ast"
	"ptah.run/internal/tableref"
)

// StreamingQuery is a YDB streaming query read from .sys/streaming_queries.
// Execution status, retry counts, and checkpoints are runtime data, not schema.
type StreamingQuery struct {
	// Name is the final path segment.
	Name string `json:"name"`
	// Schema is the database-relative directory, or empty at the root.
	Schema string `json:"schema,omitempty"`
	// Spec preserves the text, run setting, and resource pool.
	Spec ast.StreamingQuerySpec `json:"spec"`
}

// QualifiedName returns the canonical directory-qualified reference.
func (s StreamingQuery) QualifiedName() string { return tableref.Canonical(s.Schema, s.Name) }
