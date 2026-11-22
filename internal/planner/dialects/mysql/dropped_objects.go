package mysql

import (
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
)

// droppedObjects keys the tables, views, materialized views, sequences and
// routines a plan drops, the way the target compares the object a grant names.
// The zero value holds nothing.
//
// It keys every kind by its name alone. That is sound where it is read, on
// Oracle, which keeps tables, views, sequences and routines in one namespace per
// schema and reports a grant on any of them under its bare name.
type droppedObjects struct {
	semantics identifier.Semantics
	names     map[string]bool
}

// droppedObjects reads the objects diff drops: those it removes, and each
// changed routine the plan replaces by DROP and CREATE
// ([Planner.replacesRoutine]).
func (p *Planner) droppedObjects(diff *difftypes.SchemaDiff) droppedObjects {
	dropped := droppedObjects{
		semantics: diff.EffectiveIdentifierSemantics(p.targetDialect()),
		names:     make(map[string]bool),
	}
	for _, name := range diff.TablesRemoved.Names() {
		dropped.add(name)
	}
	for _, view := range diff.ViewsRemoved {
		dropped.add(view.Name)
	}
	for _, view := range diff.MaterializedViewsRemoved {
		dropped.add(view.Name)
	}
	for _, sequence := range diff.SequencesRemoved {
		dropped.add(sequence.QualifiedName())
	}
	for _, routine := range slices.Concat(diff.FunctionsRemoved, diff.ProceduresRemoved) {
		dropped.add(routine.Name)
	}
	for _, change := range diff.FunctionsModified {
		if p.replacesRoutine(change) {
			dropped.add(change.Desired.Name)
		}
	}
	return dropped
}

func (d droppedObjects) add(name string) {
	d.names[d.semantics.QualifiedTableIdentityKey(name)] = true
}

// holds reports whether grant is on an object the plan drops, or on a column
// of one.
func (d droppedObjects) holds(grant difftypes.GrantRef) bool {
	return len(d.names) > 0 && d.names[d.semantics.QualifiedTableIdentityKey(grant.ObjectName)]
}
