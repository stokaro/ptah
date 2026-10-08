package diffpolicy

import "ptah.run/migration/schemadiff/difftypes"

// retainIndexComments removes cleanup tied to a skipped index removal. A
// retained index still owns its comment, including on targets that store it
// separately from the index itself.
func retainIndexComments(diff *difftypes.SchemaDiff, retained []difftypes.IndexRef, dialect string) []difftypes.IndexCommentChange {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	key := func(table, name string) difftypes.IndexRef {
		return difftypes.IndexRef{TableName: semantics.QualifiedTableIdentityKey(table), Name: semantics.IndexIdentityKey(name)}
	}
	keep := make(map[difftypes.IndexRef]bool, len(retained))
	for _, ref := range retained {
		keep[key(ref.TableName, ref.Name)] = true
	}
	var changes []difftypes.IndexCommentChange
	for _, change := range diff.IndexCommentsChanged {
		if change.From == "" && change.Desired == "" && keep[key(change.TableName, change.Name)] {
			continue
		}
		changes = append(changes, change)
	}
	return changes
}
