package generator

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff/difftypes"
)

// projectIndexes retains observed siblings and applies only accepted changes.
// An omitted declaration or a skipped removal cannot remove a captured index.
// Partitioning transitions require the owning feature's projection semantics.
func projectIndexes(ctx context.Context, diff *difftypes.SchemaDiff, table schemacapture.TableObservation, dialect string, semantics identifier.Semantics, runtime Runtime) ([]catalog.Index, error) {
	p := indexProjection{table: table.Table.QualifiedName(), semantics: semantics}
	for _, index := range table.Indexes {
		if !p.owns(index.QualifiedTableName()) {
			return nil, fmt.Errorf("cannot project index %q captured under another table", index.Name)
		}
		if p.position(index.Name) >= 0 {
			return nil, fmt.Errorf("cannot project duplicate captured index %q", index.Name)
		}
		p.indexes = append(p.indexes, index.Clone())
	}
	if err := p.remove(diff.IndexesRemoved); err != nil {
		return nil, err
	}
	if err := p.visibility(diff.IndexVisibilityChanged); err != nil {
		return nil, err
	}
	if err := p.rename(diff.IndexesRenamed); err != nil {
		return nil, err
	}
	if err := p.add(ctx, diff.IndexesAdded, dialect, runtime); err != nil {
		return nil, err
	}
	if err := p.comments(diff.IndexCommentsChanged, diff.IndexesRemoved); err != nil {
		return nil, err
	}
	return p.indexes, nil
}

type indexProjection struct {
	table     string
	semantics identifier.Semantics
	indexes   []catalog.Index
}

func (p *indexProjection) owns(table string) bool {
	return p.semantics.QualifiedTableIdentityKey(table) == p.semantics.QualifiedTableIdentityKey(p.table)
}

func (p *indexProjection) position(name string) int {
	key := p.semantics.IndexIdentityKey(name)
	return slices.IndexFunc(p.indexes, func(index catalog.Index) bool { return p.semantics.IndexIdentityKey(index.Name) == key })
}

func (p *indexProjection) conflicts(name string) bool {
	key := p.semantics.IndexConflictKey(name)
	return slices.ContainsFunc(p.indexes, func(index catalog.Index) bool {
		return p.semantics.IndexConflictUnresolved(name) || p.semantics.IndexConflictUnresolved(index.Name) ||
			p.semantics.IndexConflictKey(index.Name) == key
	})
}

func (p *indexProjection) remove(changes []difftypes.IndexRef) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		position := p.position(change.Name)
		if position < 0 {
			return fmt.Errorf("cannot project removal of missing index %q on %q", change.Name, p.table)
		}
		p.indexes = slices.Delete(p.indexes, position, position+1)
	}
	return nil
}

func (p *indexProjection) visibility(changes []difftypes.IndexVisibilityChange) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		position := p.position(change.Name)
		if position < 0 {
			return fmt.Errorf("cannot project visibility of missing index %q on %q", change.Name, p.table)
		}
		p.indexes[position].Invisible = change.Invisible
		p.indexes[position].Definition = ""
	}
	return nil
}

func (p *indexProjection) rename(changes []difftypes.IndexRename) error {
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		position := p.position(change.From)
		if position < 0 || change.To == "" || p.conflicts(change.To) {
			return fmt.Errorf("cannot project conflicting rename of index %q to %q on %q", change.From, change.To, p.table)
		}
		p.indexes[position].Name = change.To
		p.indexes[position].Definition = ""
	}
	return nil
}

func (p *indexProjection) add(ctx context.Context, changes difftypes.IndexChanges, dialect string, runtime Runtime) error {
	var definitions []schemamodel.Index
	for _, change := range changes {
		if !p.owns(change.TableName) {
			continue
		}
		index := change.Index
		if index.Name == "" || len(index.Fields)+len(index.Parts) == 0 {
			return fmt.Errorf("cannot project incomplete addition of index %q on %q", index.Name, p.table)
		}
		// The accepted change owns the resolved table binding. No lookup in
		// a mutable desired document may redirect this definition.
		index.TableName = p.table
		definitions = append(definitions, index)
	}
	if len(definitions) == 0 {
		return nil
	}
	converted, err := goschematodb.ToDBSchema(ctx, &schemamodel.Database{Indexes: definitions}, dialect, runtime)
	if err != nil {
		return err
	}
	if len(converted.Indexes) != len(definitions) {
		return fmt.Errorf("cannot project incomplete indexes on %q", p.table)
	}
	for _, index := range converted.Indexes {
		if !p.owns(index.QualifiedTableName()) || p.conflicts(index.Name) {
			return fmt.Errorf("cannot project conflicting addition of index %q on %q", index.Name, p.table)
		}
		p.indexes = append(p.indexes, index.Clone())
	}
	return nil
}
