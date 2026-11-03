package postgres

import (
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

// removedTypeReader is a type the plan removes whose definition can say which
// relations' row types it names, under its common identity.
type removedTypeReader struct {
	id     objectidentity.ID
	reader schemaext.RelationReader
}

// removedTypeReaders are the types the plan removes that answer which
// relations they read: a domain, a composite type or a range type.
func removedTypeReaders(diff *difftypes.SchemaDiff) []removedTypeReader {
	var readers []removedTypeReader
	for _, change := range diff.FeatureChanges {
		if _, says := change.Value.(schemaext.StateRemoval); says && !schemaext.RemovesState(change.Value) {
			continue
		}
		object, found, err := diff.Features.CurrentObjects.Get(change.Subject)
		if err != nil || !found {
			continue
		}
		definer, defines := object.Value.(schemaext.TypeDefiner)
		reader, reads := object.Value.(schemaext.RelationReader)
		if defines && reads {
			readers = append(readers, removedTypeReader{id: definer.DefinedType(object.Ref).ID, reader: reader})
		}
	}
	return readers
}

// cascadeForLaterTypeDrops gives CASCADE, in the exact style, to each table
// drop of steps that a type the plan removes still holds when it runs: the
// type names the table's row type and its owner drops it in a later step, or
// in none. The type owner drops such a type ahead of the tables unless
// something dropped after the tables still uses it, so whether the drop
// relies on CASCADE is read here off the scheduled plan rather than guessed
// before it; [tableDropRelies] leaves these types out for that reason.
func (p *Planner) cascadeForLaterTypeDrops(diff *difftypes.SchemaDiff, steps []plangraph.Step[[]ast.Node]) {
	if !p.exact() || !p.cascades(true) {
		return
	}
	readers := removedTypeReaders(diff)
	if len(readers) == 0 {
		return
	}
	semantics := relationSemantics(diff)
	dropped := make(map[objectidentity.Key]bool)
	for _, step := range steps {
		for _, node := range step.Payload {
			drop, ok := node.(*ast.DropTableNode)
			if !ok || drop.Cascade {
				continue
			}
			for _, name := range drop.TableNames() {
				for _, typ := range readers {
					if !dropped[typ.id.Key()] && schemaext.ReadsRelationIn(typ.reader, name, semantics) {
						drop.SetCascade()
					}
				}
			}
		}
		for _, effect := range step.Effects {
			if effect.Action == plangraph.Drop {
				dropped[effect.Subject.Key()] = true
			}
		}
	}
}
