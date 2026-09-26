package compare

import (
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// ConstraintComments records the comment transitions of the declared table
// constraints the database holds with the same definition
// (stokaro/ptah#3678).
//
// It compares only where caps says the target stores a constraint's comment
// and reads it back. Everywhere else the reader reports none whatever was
// written, so a declared one would be a difference no plan can close.
//
// A constraint is compared when the declaration states it as a constraint. One
// the comparison synthesizes -- the primary key of a primary-key column, a
// column's CHECK or foreign key, a table's checks list -- comes from a form
// with no place for a comment, so reading its empty comment as a request to
// remove the database's would drop a comment nobody could have declared. A
// constraint whose definition changed is dropped and added again, and the
// statement that adds it writes its comment, so it is not compared either.
func ConstraintComments(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	opts *config.CompareOptions,
	caps capability.Capabilities,
	semantics identifier.Semantics,
) {
	if !caps.Has(capability.ConstraintComments) {
		return
	}
	dialect := ""
	if opts != nil {
		dialect = opts.Dialect
	}
	declared := declaredConstraintKeys(desired, semantics)
	genConstraints, dbConstraints := pairedConstraints(desired, database, dialect, semantics)
	var changes []difftypes.ConstraintCommentChange
	for key, genConstraint := range genConstraints {
		dbConstraint, exists := dbConstraints[key]
		if _, stated := declared[key]; !stated || !exists {
			continue
		}
		if constraintDefinitionsChanged(genConstraint, dbConstraint, dialect, semantics, checkExpressionsOf(opts)) {
			continue
		}
		desiredComment := strings.TrimSpace(genConstraint.Comment)
		currentComment := strings.TrimSpace(dbConstraint.Comment)
		if desiredComment == currentComment {
			continue
		}
		changes = append(changes, difftypes.ConstraintCommentChange{
			TableName: dbConstraint.QualifiedTableName(),
			Name:      dbConstraint.Name,
			Current:   currentComment,
			Desired:   desiredComment,
		})
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].TableName != changes[j].TableName {
			return changes[i].TableName < changes[j].TableName
		}
		return changes[i].Name < changes[j].Name
	})
	diff.ConstraintCommentsChanged = changes
}

// declaredConstraintKeys keys the constraints the declaration states as
// constraints.
func declaredConstraintKeys(desired *schemamodel.Database, semantics identifier.Semantics) map[tableMemberKey]struct{} {
	keys := make(map[tableMemberKey]struct{}, len(desired.Constraints))
	for _, constraint := range desired.Constraints {
		_, key := declaredConstraint(constraint, desired.Tables, semantics)
		keys[key] = struct{}{}
	}
	return keys
}
