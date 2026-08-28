package mysql

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// planSequences emits real sequence DDL for a target in this family that hosts
// sequences, and nothing at all for one that does not.
//
// The capability is the switch, not the dialect name. MySQL and MariaDB leave
// it off and keep the named skip comment reportUnsupportedObjects writes; SQL
// Server turns it on, and the same declaration becomes CREATE SEQUENCE. That
// is what the key's own doc comment asks for -- a preset may claim it only
// where a path emits, reads back and plans the object -- and the three halves
// land together here, in internal/dbschema/mssql and in the SQL Server
// renderer (stokaro/ptah#1626).
//
// Creations run before tables because a column DEFAULT may draw from the
// sequence, and on SQL Server that dependency is enforced: the engine refuses
// `DROP SEQUENCE` while a default still references it.
func (p *Planner) planSequences(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.Sequences) {
		return result
	}
	// No lookup on either branch: the change carries the sequence
	// (stokaro/ptah#2315), so a name the desired schema could not resolve no
	// longer plans nothing.
	for _, sequence := range diff.SequencesAdded {
		result = append(result, fromschema.FromSequence(sequence))
	}
	for _, sequenceDiff := range diff.SequencesModified {
		sequence := sequenceDiff.Desired
		if sequence.Name == "" {
			continue
		}
		node := alterSequenceFromDiff(sequence, sequenceDiff.Changes)
		node.SetComment(fmt.Sprintf("Modify sequence %s: %s",
			sequenceDiff.SequenceName, summarizeSequenceChanges(sequenceDiff)))
		result = append(result, node)
	}
	return result
}

// removeSequences emits DROP SEQUENCE for sequences the desired schema no
// longer carries. It runs after table removal so a table whose column default
// draws from the sequence is gone first.
func (p *Planner) removeSequences(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.Sequences) {
		return result
	}
	for _, sequence := range diff.SequencesRemoved {
		schemaName, sequenceName := splitQualifiedSequenceName(sequence.QualifiedName())
		node := ast.NewDropSequence(sequenceName).
			SetIfExists().
			SetComment("WARNING: Ensure no column default still draws from this sequence")
		if schemaName != "" {
			node.SetSchema(schemaName)
		}
		result = append(result, node)
	}
	return result
}

// alterSequenceFromDiff builds an ALTER SEQUENCE node carrying only the options
// the diff reports as changed, sourced from the declaration.
func alterSequenceFromDiff(target schemamodel.Sequence, changes map[string]string) *ast.AlterSequenceNode {
	node := ast.NewAlterSequence(target.Name)
	if target.Schema != "" {
		node.SetSchema(target.Schema)
	}
	if _, ok := changes["as"]; ok && target.AsType != "" {
		node.SetAs(target.AsType)
	}
	if _, ok := changes["start"]; ok && target.Start != nil {
		node.SetStart(*target.Start)
	}
	if _, ok := changes["increment"]; ok && target.Increment != nil {
		node.SetIncrement(*target.Increment)
	}
	if _, ok := changes["minvalue"]; ok && target.MinValue != nil {
		node.SetMinValue(*target.MinValue)
	}
	if _, ok := changes["maxvalue"]; ok && target.MaxValue != nil {
		node.SetMaxValue(*target.MaxValue)
	}
	if _, ok := changes["cache"]; ok && target.Cache != nil {
		node.SetCache(*target.Cache)
	}
	if _, ok := changes["cycle"]; ok {
		node.SetCycle(target.Cycle)
	}
	if _, ok := changes["owned_by"]; ok && target.OwnedBy != "" {
		node.SetOwnedBy(target.OwnedBy)
	}
	return node
}

// summarizeSequenceChanges produces a deterministic one-line summary of the
// changed options, for the comment above the statement.
func summarizeSequenceChanges(sequenceDiff difftypes.SequenceDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(sequenceDiff.Changes)), ", ")
}

func splitQualifiedSequenceName(name string) (schema, sequence string) {
	ref, ok := tableref.Parse(name)
	if !ok {
		return "", name
	}
	return ref.Schema, ref.Name
}
