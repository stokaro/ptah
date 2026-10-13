package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
)

type placedOwnedConstraint struct {
	declaration *ast.OwnedIndexConstraint
	place       int
}

// foldOwnedConstraints uses an owner's semantic key only within one CREATE
// TABLE and one kind. A later written name belongs to the first equal index
// when that index is unnamed. Common UNIQUE keys cannot share this namespace.
func foldOwnedConstraints(node *ast.CreateTableNode, dialect string) []placedOwnedConstraint {
	positions := make(map[*ast.OwnedIndexConstraint]int)
	for i, element := range node.Elements {
		if element.OwnedConstraint != nil {
			positions[element.OwnedConstraint] = 2*i + 1
		}
	}
	type foldIdentity struct {
		kind schemaext.Kind
		key  string
	}
	kept := make(map[foldIdentity]*ast.OwnedIndexConstraint)
	var result []placedOwnedConstraint
	for i, declaration := range node.OwnedConstraints {
		key := foldIdentity{kind: declaration.Value.Kind(), key: declaration.FoldKey}
		if namesConstraintsLikePostgres(dialect) && key.key != "" {
			if first, found := kept[key]; found {
				if first.Name == "" {
					first.Name = declaration.Name
				}
				continue
			}
			kept[key] = declaration
		}
		place, found := positions[declaration]
		if !found {
			place = 2*(len(node.Columns)+len(node.Constraints)+i) + 1
		}
		result = append(result, placedOwnedConstraint{declaration: declaration, place: place})
	}
	return result
}

func (s *createdStatement) ownedKeys() []createdConstraint {
	keys := make([]createdConstraint, 0, len(s.owned))
	for _, owned := range s.owned {
		declaration := owned.declaration
		keys = append(keys, createdConstraint{
			place: owned.place, written: declaration.Name, indexed: true,
			what: fmt.Sprintf("the %s constraint", declaration.Value.Kind()),
			derive: func(taken func(string) bool) string {
				return pgname.Constraint(s.table.Name, declaration.NameParts, declaration.NameSuffix, taken)
			},
			keep: func(name string) { declaration.Name = name },
		})
	}
	return keys
}

func appendOwnedConstraints(database *schemamodel.Database, document *Document, table schemamodel.Table, declarations []placedOwnedConstraint, dialect string) error {
	builder := objectidentity.NewBuilder(identifier.ForDialect(dialect))
	var objects []schemaext.Object
	var declared []objectidentity.ID
	for _, declaration := range declarations {
		value := declaration.declaration
		if value.Name == "" {
			return fmt.Errorf("%w: %s cannot name an owned constraint without an explicit name", schemaext.ErrInvalidValue, dialect)
		}
		// Names supplied by an owner and the naming pass are logical identifiers.
		ref := builder.ConstraintParts(table.Schema, table.Name, value.Name)
		ref.Kind = objectidentity.Kind(value.Value.Kind())
		declared = append(declared, declaredChild(declaringDatabases(database, document.base), ref, dialect))
		objects = append(objects, schemaext.Object{Ref: ref, Value: value.Value})
	}
	captured, err := schemaext.NewObjects(objects...)
	if err != nil {
		return err
	}
	if err := appendTableObjects(database, declaringDatabases(database, document.base), table, captured, dialect); err != nil {
		return err
	}
	for _, ref := range declared {
		delete(document.comments, ref.Key())
	}
	return nil
}

// An ALTER addition gets a fresh index even when another constraint has the
// same definition. It sees names left by each earlier operation of the file.
func appendAddedOwnedConstraint(database *schemamodel.Database, target alterTarget, declaration *ast.OwnedIndexConstraint) error {
	if declaration.Name == "" && namesConstraintsLikePostgres(target.sourcePlatform) {
		constraints := constraintNamesInSchema(target.databases, target.table.Schema)
		relations := pgname.RelationNames(target.databases, target.table.Schema)
		declaration.Name = pgname.Constraint(target.table.Name, declaration.NameParts, declaration.NameSuffix, func(name string) bool {
			return constraints.Taken(name) || relations.Taken(name)
		})
	}
	if target.holdsConstraint(declaration.Name) || relationHeld(target.databases, target.document, target.table.Schema, declaration.Name, target.sourcePlatform) {
		return fmt.Errorf("%w: ALTER TABLE %s ADD CONSTRAINT %s: a constraint or relation holds the name", schemaext.ErrDuplicate, target.written, declaration.Name)
	}
	return appendOwnedConstraints(database, target.document, *target.table, []placedOwnedConstraint{{declaration: declaration}}, target.sourcePlatform)
}
