package planner

import (
	"fmt"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/tableref"
)

// refuseCreatedAndDroppedNamesakes refuses a plan that creates an object and
// drops another of its kind whose name the target may read as the same name.
//
// The comparison pairs two such names as one object and plans a change of it,
// so it never asks for both. A kind whose names it does not pair the way the
// server compares them asks for both, and the plan then loses the object.
// Measured on SQL Server 2025 under a case-insensitive collation:
// `CREATE OR ALTER PROCEDURE [MIXEDCASE]` changes the procedure dbo.MixedCase,
// the `DROP PROCEDURE [dbo].[MixedCase]` after it removes it, and every step
// reports success (stokaro/ptah#6722). The refusal stops that plan before
// anything runs.
//
// Two names are namesakes when their conflict keys agree under the target's
// rules, read as [indexscope.ConflictSemantics] reads them, and they are not
// spelled alike. Two names are spelled alike when they are in one schema and
// their own names have the same bytes: a PostgreSQL overload dropped while
// another is created is that, and so is an object a plan drops to create it
// again, which the planner orders. Without a server to ask, two ASCII names
// that differ only in case may be one name on SQL Server, so an offline plan
// for them is refused too. Indexes and constraints belong to a table, and
// their replacements are planned as a drop before the create; they are not
// checked here.
func refuseCreatedAndDroppedNamesakes(diff *difftypes.SchemaDiff, semantics identifier.Semantics, target string) error {
	if diff == nil {
		return nil
	}
	semantics = indexscope.ConflictSemantics(semantics)
	for _, kind := range namesakeKinds(diff, semantics) {
		dropped := make(map[string][]plannedName, len(kind.dropped))
		for _, name := range kind.dropped {
			dropped[name.key] = append(dropped[name.key], name)
		}
		for _, created := range kind.created {
			for _, removed := range dropped[created.key] {
				if created.spelling == removed.spelling {
					continue
				}
				return fmt.Errorf("%w: the plan creates %s %s and drops %s %s, and %s may read the two names as one, "+
					"so the drop would remove the object the create makes; declare the object as the database spells it, "+
					"or plan against the database so that the server says how it compares the names",
					ptaherr.ErrInvalidSchemaDiff, created.noun, created.name, removed.noun, removed.name, target)
			}
		}
	}
	return nil
}

// plannedName is one object a plan creates or drops: how a message names it,
// the key the target's rules give it, and its spelling with the schema
// written out.
type plannedName struct {
	noun     string
	name     string
	key      string
	spelling string
}

// namesakeKind is the objects of one kind a plan creates and drops.
type namesakeKind struct {
	created []plannedName
	dropped []plannedName
}

// namesakeKinds lists, for each kind of object a schema holds by name, the
// objects of it the diff creates and drops. A function and a procedure are one
// kind, because on SQL Server they share one namespace.
func namesakeKinds(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []namesakeKind {
	schemaScoped := func(noun string, created, dropped []string) namesakeKind {
		return namesakeKind{
			created: schemaScopedNames(noun, created, semantics),
			dropped: schemaScopedNames(noun, dropped, semantics),
		}
	}
	routines := namesakeKind{}
	for _, routine := range diff.FunctionsAdded {
		routines.created = append(routines.created, schemaScopedName(routineNoun(routine), routine.Name, semantics))
	}
	for _, routine := range diff.FunctionsRemoved {
		routines.dropped = append(routines.dropped, schemaScopedName(routineNoun(routine), routine.Name, semantics))
	}
	for _, routine := range diff.ProceduresRemoved {
		routines.dropped = append(routines.dropped, schemaScopedName("procedure", routine.Name, semantics))
	}
	triggers := namesakeKind{}
	for _, trigger := range diff.TriggersAdded {
		triggers.created = append(triggers.created, triggerName(trigger, semantics))
	}
	for _, trigger := range diff.TriggersRemoved {
		triggers.dropped = append(triggers.dropped, triggerName(trigger, semantics))
	}
	schemas := namesakeKind{}
	for _, schema := range diff.SchemasAdded {
		schemas.created = append(schemas.created, schemaName(schema.Name, semantics))
	}
	for _, schema := range diff.SchemasRemoved {
		schemas.dropped = append(schemas.dropped, schemaName(schema, semantics))
	}
	return []namesakeKind{
		schemas,
		schemaScoped("table", diff.TablesAdded.Names(), diff.TablesRemoved.Names()),
		schemaScoped("view", diff.ViewsAdded.Names(), diff.ViewsRemoved.Names()),
		schemaScoped("materialized view", diff.MaterializedViewsAdded.Names(), diff.MaterializedViewsRemoved.Names()),
		schemaScoped("sequence", diff.SequencesAdded.Names(), diff.SequencesRemoved.Names()),
		routines,
		triggers,
	}
}

func routineNoun(routine difftypes.RoutineChange) string {
	if routine.IsProcedure() {
		return "procedure"
	}
	return "function"
}

func schemaScopedNames(noun string, names []string, semantics identifier.Semantics) []plannedName {
	planned := make([]plannedName, 0, len(names))
	for _, name := range names {
		planned = append(planned, schemaScopedName(noun, name, semantics))
	}
	return planned
}

// schemaScopedName is an object named in a schema, such as a table or a
// procedure, whose name may carry the schema.
func schemaScopedName(noun, name string, semantics identifier.Semantics) plannedName {
	return plannedName{noun: noun, name: name, key: semantics.QualifiedTableConflictKey(name), spelling: schemaSpelling(name, semantics)}
}

// triggerName is a trigger, which the comparison names per table.
func triggerName(trigger difftypes.TriggerRef, semantics identifier.Semantics) plannedName {
	return plannedName{
		noun:     "trigger",
		name:     trigger.TriggerName + " on " + trigger.TableName,
		key:      semantics.QualifiedTableConflictKey(trigger.TableName) + "\x00" + semantics.TableConflictKey(trigger.TriggerName),
		spelling: schemaSpelling(trigger.TableName, semantics) + "\x00" + trigger.TriggerName,
	}
}

func schemaName(name string, semantics identifier.Semantics) plannedName {
	return plannedName{noun: "schema", name: name, key: semantics.TableConflictKey(name), spelling: name}
}

// schemaSpelling is name with the schema it is in written out, so that
// `orders`, `dbo.orders` and `[dbo].[orders]` are one spelling on a target
// whose default schema is dbo, and `orders` and `ORDERS` are two.
func schemaSpelling(name string, semantics identifier.Semantics) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	schema := ref.Schema
	if !ref.Qualified {
		schema = semantics.DefaultSchema
	}
	return semantics.TableIdentityKey(schema) + "\x00" + ref.Name
}
