package providerhost

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// observedIndexRules are the index rules of a provider that reports the
// indexes its tables hold. Registering them has the shared comparison compare
// an index's uniqueness, key columns and covered columns, so a held index
// whose key differs from its declaration under the same name is dropped and
// built again. Without rules the comparison pairs indexes by name alone.
//
// The provider spells an index's kind on both sides, and the host has no rule
// to read either spelling. With methods, the provider's table preparation
// states the kind of each declared index as it reports a held index's method
// (schema.index-methods), the comparison hands that kind to OptionsEqual in
// the declared index's Type, and the two spellings are compared exactly, so
// a change of kind alone drops the index and builds it again. Without
// methods no kind is compared, and such a change plans nothing. The build
// settings are not compared.
//
// With facets (schema.observed-index-facets) the engine reports the settings
// of the indexes it holds as facets of each index, and its own rename step
// keeps them: the comparison then pairs an index that carries facets, and
// compares them under the name the declaration gives it. Without facets an
// index that carries a facet is not renamed in place, because the host cannot
// say the rename keeps it.
type observedIndexRules struct {
	methods bool
	facets  bool
}

func (r observedIndexRules) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	return !r.methods || desired.Type == current.Method
}

func (r observedIndexRules) RenameCarries(schemaext.Kind) bool { return r.facets }

// ownsColumns reports whether the provider owns the lifecycle of its tables,
// and so the columns it adds and drops: it assesses each altered table whole
// and leaves no table operation to the host.
func (p commonPlanner) ownsColumns() bool {
	return p.assessParents && !p.commonTables
}

// ownsIndexes reports whether the provider plans the common indexes of the
// tables it owns. A provider that leaves table operations to the host has no
// index step on the wire, and one that does not report held indexes gives
// the comparison no current side, so both keep the index fields refused.
func (p commonPlanner) ownsIndexes() bool {
	return p.ownsColumns() && p.indexes
}

// pendingIndexChange stands in for an index addition or removal on a table
// whose provider owns the table lifecycle. The host has no common-table
// interpretation to render it with, so only the provider's graph step can.
// The step carries no operand: the provider reads the declared index from
// the altered parent's desired table and the held one from its current
// table. Rendering it is always an error.
type pendingIndexChange struct {
	table string
	index string
	added bool
}

func (n *pendingIndexChange) Accept(ast.Visitor) error {
	if n.added {
		return unsupported(fmt.Sprintf("unclaimed provider addition of index %s to table %s", n.index, n.table))
	}
	return unsupported(fmt.Sprintf("unclaimed provider removal of index %s from table %s", n.index, n.table))
}

// pendingIndexRename stands in for an index the comparison paired with one
// declared under another name, on a table whose provider owns the table
// lifecycle and renames indexes. The owner's graph step keeps the index under
// its new name; rendering the placeholder is always an error.
type pendingIndexRename struct {
	table    string
	from, to string
}

func (n *pendingIndexRename) Accept(ast.Visitor) error {
	return unsupported(fmt.Sprintf("unclaimed provider rename of index %s of table %s to %s", n.from, n.table, n.to))
}

// indexSteps are the placeholders of the index changes on the tables a plan
// keeps.
type indexSteps struct {
	removals, renames, additions []ast.Node
}

// indexChanges stands in for the index additions, removals and renames on
// the tables the plan keeps. A created table's indexes travel in its
// declaration, and a removed table takes its indexes with it, so neither gets
// a step of its own. The comparison renames an index only on a table both
// sides hold.
func (p commonPlanner) indexChanges(diff *difftypes.SchemaDiff) (indexSteps, error) {
	var result indexSteps
	if !p.ownsIndexes() {
		return result, nil
	}
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
	created := make(map[objectidentity.Key]bool, len(diff.TablesAdded))
	for _, creation := range diff.TablesAdded {
		created[builder.TableParts(creation.Table.Schema, creation.Table.Name).Key()] = true
	}
	removed := make(map[objectidentity.Key]bool, len(diff.TablesRemoved))
	for _, name := range diff.TablesRemoved.Names() {
		key, err := tableKey(builder, name)
		if err != nil {
			return indexSteps{}, err
		}
		removed[key] = true
	}
	for _, ref := range diff.IndexesRemoved {
		key, err := tableKey(builder, ref.TableName)
		if err != nil {
			return indexSteps{}, err
		}
		if !removed[key] {
			result.removals = append(result.removals, &pendingIndexChange{table: ref.TableName, index: ref.Name})
		}
	}
	for _, rename := range diff.IndexesRenamed {
		key, err := tableKey(builder, rename.TableName)
		if err != nil {
			return indexSteps{}, err
		}
		if created[key] || removed[key] {
			return indexSteps{}, fmt.Errorf("provider planner received a rename of index %s of table %s, which the plan does not keep", rename.From, rename.TableName)
		}
		result.renames = append(result.renames, &pendingIndexRename{table: rename.TableName, from: rename.From, to: rename.To})
	}
	for _, change := range diff.IndexesAdded {
		key, err := tableKey(builder, change.TableName)
		if err != nil {
			return indexSteps{}, err
		}
		if !created[key] {
			result.additions = append(result.additions, &pendingIndexChange{table: change.TableName, index: change.Index.Name, added: true})
		}
	}
	return result, nil
}
