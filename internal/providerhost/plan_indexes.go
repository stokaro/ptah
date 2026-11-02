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
// The kind and the build settings are spelled by the provider on both sides,
// and the host has no rule to read either spelling, so they are not compared
// here. An index that carries a facet is not renamed in place, because the
// host cannot say the rename keeps it.
type observedIndexRules struct{}

func (observedIndexRules) OptionsEqual(schemamodel.Index, catalog.Index) bool { return true }

func (observedIndexRules) RenameCarries(schemaext.Kind) bool { return false }

// ownsIndexes reports whether the provider plans the common indexes of the
// tables it owns. A provider that leaves table operations to the host has no
// index step on the wire, and one that does not report held indexes gives
// the comparison no current side, so both keep the index fields refused.
func (p commonPlanner) ownsIndexes() bool {
	return p.assessParents && !p.commonTables && p.indexes
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

// indexChanges stands in for the index additions and removals on the tables
// the plan keeps. A created table's indexes travel in its declaration, and a
// removed table takes its indexes with it, so neither gets a step of its own.
func (p commonPlanner) indexChanges(diff *difftypes.SchemaDiff) (removals, additions []ast.Node, err error) {
	if !p.ownsIndexes() {
		return nil, nil, nil
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
			return nil, nil, err
		}
		removed[key] = true
	}
	for _, ref := range diff.IndexesRemoved {
		key, err := tableKey(builder, ref.TableName)
		if err != nil {
			return nil, nil, err
		}
		if !removed[key] {
			removals = append(removals, &pendingIndexChange{table: ref.TableName, index: ref.Name})
		}
	}
	for _, change := range diff.IndexesAdded {
		key, err := tableKey(builder, change.TableName)
		if err != nil {
			return nil, nil, err
		}
		if !created[key] {
			additions = append(additions, &pendingIndexChange{table: change.TableName, index: change.Index.Name, added: true})
		}
	}
	return removals, additions, nil
}
