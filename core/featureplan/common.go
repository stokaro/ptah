package featureplan

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

// CommonStep describes an accepted host operation before feature planning.
// Its identity and effects let an owner request ordering or propose a rewrite
// without comparing common objects again. Empty Effects remains unknown.
// Parent identifies a captured table for a table-bound operation.
type CommonStep struct {
	ID          plangraph.StepID
	Parent      objectidentity.ID
	Effects     []plangraph.Effect
	Transaction plangraph.Transaction
	Impact      schemaext.Effect
	// AddedColumn supplies the exact planned definition when the step adds a
	// column. Nil supplies no column-addition operand; it never means an empty
	// definition. Providers must refuse rewrites whose required facts are absent.
	// This is local planning data. A process adapter maps common definitions to
	// its explicit protocol schema rather than serializing Go AST structs.
	AddedColumn *ast.ColumnNode
}

// Clone returns independent metadata and common column operands. Facets retain
// their immutable value semantics; the runtime validates their local codecs.
func (s CommonStep) Clone() CommonStep {
	s.Effects = slices.Clone(s.Effects)
	if s.AddedColumn != nil {
		s.AddedColumn = new(*s.AddedColumn)
		if s.AddedColumn.Default != nil {
			s.AddedColumn.Default = new(*s.AddedColumn.Default)
		}
		if s.AddedColumn.ForeignKey != nil {
			s.AddedColumn.ForeignKey = new(*s.AddedColumn.ForeignKey)
			s.AddedColumn.ForeignKey.Columns = slices.Clone(s.AddedColumn.ForeignKey.Columns)
			s.AddedColumn.ForeignKey.OnDeleteColumns = slices.Clone(s.AddedColumn.ForeignKey.OnDeleteColumns)
		}
	}
	return s
}
