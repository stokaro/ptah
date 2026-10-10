package mssqldiff

import (
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Edit is what a change does to the policy, statement by statement. The
// renderer writes it, and the planner runs a change of more than one
// statement in one transaction, so the two read one answer. Measured on SQL
// Server 2025:
//
//   - schema binding and NOT FOR REPLICATION cannot be altered (`ALTER
//     SECURITY POLICY ... WITH (SCHEMABINDING = OFF)` and `... NOT FOR
//     REPLICATION` are syntax errors), so a change to either drops the policy
//     and creates it again;
//   - otherwise one ALTER SECURITY POLICY drops, alters and adds predicates,
//     finding each by its table, type and operation, so a block predicate
//     whose operation changes is a drop and an addition;
//   - one statement may not drop and add one slot: SQL Server refuses it with
//     Msg 33262 however the table is spelled, while drops of other slots
//     apply before the additions. A drop that frees a slot an addition takes
//     under another spelling of its table therefore goes into a statement of
//     its own, first. The two work as separate batches, which is how the
//     migrator runs statements; sent as one batch, the addition is refused
//     before the drop runs;
//   - the state is set by its own ALTER: before the predicates when it
//     disables the policy, after them when it enables it, so the policy never
//     enforces a predicate set it holds only halfway.
type Edit struct {
	// Drop and Create drop the observed policy and create the declared one.
	// Both are set for a replacement.
	Drop, Create bool
	// Disable and Enable set the state around the predicate statements.
	Disable, Enable bool
	// Drops, Alters and Adds are the predicate clauses, each in canonical
	// order. An alteration carries the declared invocation.
	Drops, Alters, Adds []mssqlschema.Predicate
	// SeparateDrops puts Drops in a statement before the one holding Alters
	// and Adds.
	SeparateDrops bool
}

// Statements counts the statements the edit renders to.
func (e Edit) Statements() int {
	if e.Drop || e.Create {
		return count(e.Drop, e.Create)
	}
	clauses := len(e.Alters)+len(e.Adds) > 0 || !e.SeparateDrops && len(e.Drops) > 0
	return count(e.Disable, e.Enable, e.SeparateDrops && len(e.Drops) > 0, clauses)
}

// Edit derives the statements of a valid change. Predicates are paired by the
// exact spelling of their slot: SQL Server finds a predicate to alter or drop
// by its table under the database collation, which an offline reading does not
// know, so a slot spelled differently on the two sides is dropped and added
// rather than altered. A paired predicate is altered unless its invocation is
// the same by [mssqlschema.CompareInvocation].
func (v *SecurityPolicy) Edit() Edit {
	switch {
	case v.Before == nil:
		return Edit{Create: true}
	case v.After == nil:
		return Edit{Drop: true}
	}
	enabled, schemaBinding, notForReplication := v.After.Resolved()
	if schemaBinding != v.Before.SchemaBinding || notForReplication != v.Before.NotForReplication {
		return Edit{Drop: true, Create: true}
	}
	edit := Edit{Disable: v.Before.Enabled && !enabled, Enable: !v.Before.Enabled && enabled}
	semantics := identifier.ForDialect(platform.SQLServer)
	after := mssqlschema.SortedPredicates(v.After.Predicates)
	for _, old := range mssqlschema.SortedPredicates(v.Before.Predicates) {
		index := slices.IndexFunc(after, func(candidate mssqlschema.Predicate) bool { return sameSpelledSlot(old, candidate) })
		switch {
		case index < 0:
			edit.Drops = append(edit.Drops, old)
		case mssqlschema.CompareInvocation(semantics, after[index], old) != mssqlschema.Agree:
			edit.Alters = append(edit.Alters, after[index])
		}
	}
	for _, added := range after {
		if !slices.ContainsFunc(v.Before.Predicates, func(old mssqlschema.Predicate) bool { return sameSpelledSlot(old, added) }) {
			edit.Adds = append(edit.Adds, added)
		}
	}
	for _, dropped := range edit.Drops {
		edit.SeparateDrops = edit.SeparateDrops || slices.ContainsFunc(edit.Adds, func(added mssqlschema.Predicate) bool {
			return sameConflictSlot(dropped, added)
		})
	}
	return edit
}

func sameSpelledSlot(a, b mssqlschema.Predicate) bool {
	return a.Type == b.Type && a.Operation == b.Operation && a.Table == b.Table
}

func sameConflictSlot(a, b mssqlschema.Predicate) bool {
	return a.Type == b.Type && a.Operation == b.Operation && a.Table.ConflictKey() == b.Table.ConflictKey()
}

func count(flags ...bool) int {
	n := 0
	for _, flag := range flags {
		if flag {
			n++
		}
	}
	return n
}
