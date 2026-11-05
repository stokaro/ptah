package generator

import (
	"context"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// reverseTablePreparation turns the prepared tables the forward comparison
// captured into the ones the reverse plan reads. A prepared table is the
// planning parent of a change that leaves the table itself alone, such as an
// index added to it, and the forward capture holds the table the forward plan
// starts from: the declaration it moves to and the table it found. The
// reverse plan starts from what the forward plan leaves and moves to what it
// found, so each prepared table takes the prior declaration of the table and
// the table the forward plan found with its index changes projected onto it.
// Without it, an owner asked to drop an index the forward plan added finds
// the table without the index and refuses the drop.
//
// A table the reverse plan alters is planned from its own projected capture,
// which the reverse table diff carries, and a table it creates or drops from
// its creation or removal; both replace the prepared entry, so it is left as
// it is.
func reverseTablePreparation(ctx context.Context, forward, reverse *difftypes.SchemaDiff, prior *schemamodel.Database, dialect string, runtime Runtime) error {
	if reverse.TablePreparation == nil {
		return nil
	}
	semantics := forward.EffectiveIdentifierSemantics(dialect)
	planned := make(map[string]bool, len(reverse.TablesModified)+len(reverse.TablesAdded)+len(reverse.TablesRemoved))
	for _, table := range reverse.TablesModified {
		planned[semantics.QualifiedTableIdentityKey(table.TableName)] = true
	}
	for _, table := range reverse.TablesAdded {
		planned[semantics.QualifiedTableIdentityKey(table.Name)] = true
	}
	for _, table := range reverse.TablesRemoved {
		planned[semantics.QualifiedTableIdentityKey(table.Name)] = true
	}
	for i, table := range reverse.TablePreparation.Prepared {
		if !table.Current.HasTable() {
			continue
		}
		name := table.Current.Table.QualifiedName()
		if planned[semantics.QualifiedTableIdentityKey(name)] {
			continue
		}
		indexes, err := projectIndexes(ctx, forward, table.Current, dialect, semantics, runtime)
		if err != nil {
			return err
		}
		current := table.Current.Clone()
		current.Indexes = indexes
		reverse.TablePreparation.Prepared[i].Current = current
		reverse.TablePreparation.Prepared[i].Desired = priorTableDeclaration(prior, name, semantics)
	}
	return nil
}
