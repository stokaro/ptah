package planner

import (
	"fmt"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/mysqlroutine"
	"ptah.run/internal/offlinenames"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/identifiervalidation"
)

// refuseCreatedAndDroppedNamesakes refuses a plan that creates an object and
// drops another of its kind whose name the target may read as the same name,
// and a plan that creates an object and drops one of another kind that shares
// its namespace under a name the target may read as the same name.
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
// Two names are namesakes when the target's rules, read as
// [indexscope.ConflictSemantics] reads them, may make them one name, as
// [identifiervalidation.Namesake.MayBe] says, and they are not spelled alike.
// Two names are spelled alike when they are in one schema and their own names
// have the same bytes: a PostgreSQL overload dropped while another is created
// is that, and so is an object a plan drops to create it again, which the
// planner orders. A routine name is keyed by the routine rule where the
// target's descriptor states one
// ([ptah.run/core/platform/descriptor.Comparison.RoutineNamesFold]), since
// the server folds it whatever its table rule says.
//
// Under the offline rules of a target whose catalog compares its names (see
// [offlinenames.Unanswered]), no rule says that two names are two: a
// collation with contractions reads `ccs` as `cscs` under Hungarian_CI_AS, and
// `a-b` as `ab` under Maori_100_CI_AS, and nothing but the server says which
// collation it has. Without a connection such a plan therefore never creates
// an object and drops another of its kind unless the two are spelled alike,
// whatever the two names are (stokaro/ptah#6746). That is the cost of
// planning such a target offline, and the refusal says that no connection
// answered. With a connection, the server resolves every name, and only the
// names it reads as one are refused.
//
// Objects of two kinds that share a namespace in a schema
// ([descriptor.Comparison.SharedObjectNamespace]) are refused when their names
// may be one name, spelled alike or not. The planner writes every create
// before every drop of another kind, so the create runs while the other
// object holds the name, and the server refuses it: measured on SQL Server
// 2025, `CREATE OR ALTER PROCEDURE [dbo].[REPORT]` beside the view dbo.Report
// fails with error 2010 (stokaro/ptah#6741). A function and a procedure are
// two kinds here: a plan that replaces the function dbo.x by the procedure
// dbo.x creates the procedure first, and fails the same way. Nothing is lost
// there, so the offline rules apply as they stand: two ASCII names that differ
// beyond case are two names.
//
// Indexes and constraints belong to a table, and their replacements are
// planned as a drop before the create; they are not checked here. Triggers
// are not checked either. A trigger the plan adds is written as CREATE
// TRIGGER, never as a statement that replaces one in place, so a namesake the
// planner leaves in place makes the server refuse the create, and nothing is
// lost. The MySQL-family planner, which plans SQL Server too, creates a
// trigger after the drop of one whose name differs from it only in case, so
// that plan runs.
func refuseCreatedAndDroppedNamesakes(diff *difftypes.SchemaDiff, semantics identifier.Semantics, target string) error {
	if diff == nil {
		return nil
	}
	rules := namesakeRules{target: target, unanswered: offlinenames.Unanswered(semantics)}
	semantics = indexscope.ConflictSemantics(semantics)
	described, _ := platform.Lookup(target)
	kinds := namesakeKinds(diff, semantics, described)
	for _, kind := range kinds {
		if err := rules.refuseNamesakes(kind); err != nil {
			return err
		}
	}
	return refuseNamesakesOfAnotherKind(kinds, target)
}

// namesakeRules is how a plan's namesakes are judged on target. unanswered
// reports that the target's catalog compares its names and no connection
// answered how ([offlinenames.Unanswered]).
type namesakeRules struct {
	target     string
	unanswered bool
}

// refuseNamesakes refuses the first object of kind the plan creates whose
// name may be the name of one it drops; see [refuseCreatedAndDroppedNamesakes].
func (r namesakeRules) refuseNamesakes(kind namesakeKind) error {
	for _, created := range kind.created {
		for _, removed := range kind.dropped {
			if created.spelling != removed.spelling && (r.unanswered || created.MayBe(removed.Namesake)) {
				return r.namesakeError(created, removed)
			}
		}
	}
	return nil
}

// refuseNamesakesOfAnotherKind refuses the first object the plan creates
// whose name may be the name of an object of another kind it drops, where the
// target keeps both kinds in one namespace; see
// [refuseCreatedAndDroppedNamesakes]. The kind of an object is its noun, so
// a function and a procedure are two kinds.
func refuseNamesakesOfAnotherKind(kinds []namesakeKind, target string) error {
	for _, creating := range kinds {
		for _, dropping := range kinds {
			if !creating.shared || !dropping.shared {
				continue
			}
			for _, created := range creating.created {
				for _, removed := range dropping.dropped {
					if created.noun != removed.noun && created.MayBe(removed.Namesake) {
						return fmt.Errorf(
							"%w: the plan creates %s %s and drops %s %s, and %s keeps both kinds in one namespace "+
								"in a schema, where it may read the two names as one, so the create would fail while "+
								"the %s exists; drop the %s in a plan of its own first",
							ptaherr.ErrInvalidSchemaDiff, created.noun, created.name, removed.noun, removed.name, target,
							removed.noun, removed.noun,
						)
					}
				}
			}
		}
	}
	return nil
}

// namesakeError names a created object and a dropped one the target may read
// as one; see [refuseCreatedAndDroppedNamesakes] for when each wording is
// written.
func (r namesakeRules) namesakeError(created, removed plannedName) error {
	pair := fmt.Sprintf("the plan creates %s %s and drops %s %s", created.noun, created.name, removed.noun, removed.name)
	resolved := !created.Unresolved() && !removed.Unresolved()
	switch {
	case r.unanswered && resolved && created.MayBe(removed.Namesake):
		return fmt.Errorf(
			"%w: %s, which differ only in case, so under a case-insensitive collation the drop would remove the object "+
				"the create makes, and without a connection Ptah cannot tell which collation the database uses; "+
				"declare the object as the database spells it",
			ptaherr.ErrInvalidSchemaDiff, pair,
		)
	case r.unanswered:
		return fmt.Errorf(
			"%w: %s, and without a connection Ptah cannot tell whether they are one name under the database collation, "+
				"so the drop may remove the object the create makes; compare with a connection to the database, "+
				"which answers how it compares the names",
			ptaherr.ErrInvalidSchemaDiff, pair,
		)
	case !resolved:
		return fmt.Errorf(
			"%w: %s, and %s was not asked whether they are one name, so the drop may remove the object the create makes",
			ptaherr.ErrInvalidSchemaDiff, pair, r.target,
		)
	}
	return fmt.Errorf(
		"%w: %s, and %s reads them as one name, so the drop would remove the object the create makes; "+
			"declare the object as the database spells it",
		ptaherr.ErrInvalidSchemaDiff, pair, r.target,
	)
}

// plannedName is one object a plan creates or drops: how a message names it,
// how the target's rules compare its name, and its spelling with the schema
// written out.
type plannedName struct {
	identifiervalidation.Namesake

	noun     string
	name     string
	spelling string
}

// namesakeKind is the objects of one kind a plan creates and drops, or its
// functions and procedures together. shared reports that the kind is in the
// namespace the target's descriptor states
// ([descriptor.Comparison.SharedObjectNamespace]).
type namesakeKind struct {
	shared  bool
	created []plannedName
	dropped []plannedName
}

// namesakeKinds lists, for each kind of object a schema holds by name, the
// objects of it the diff creates and drops. Functions and procedures are
// listed together, because a plan can replace a routine of one kind by a
// routine of the other under one name; each keeps its noun, which
// [refuseNamesakesOfAnotherKind] reads as its kind. A routine name is keyed by
// the routine rule of the target's descriptor where it states one.
func namesakeKinds(diff *difftypes.SchemaDiff, semantics identifier.Semantics, described descriptor.Dialect) []namesakeKind {
	routinesFold := described.Comparison.RoutineNamesFold
	namespace := described.Comparison.SharedObjectNamespace
	schemaScoped := func(noun string, shared bool, created, dropped []string) namesakeKind {
		return namesakeKind{
			shared:  shared,
			created: schemaScopedNames(noun, created, semantics),
			dropped: schemaScopedNames(noun, dropped, semantics),
		}
	}
	routine := func(noun, name string) plannedName {
		planned := schemaScopedName(noun, name, semantics)
		if routinesFold && !planned.Unresolved() {
			// The server folds a routine name whatever its table rule says;
			// the comparison folds it as [mysqlroutine.IdentityKey] does.
			planned.NameKey = mysqlroutine.IdentityKey(planned.NameKey)
			planned.Key = planned.SchemaKey + "\x00" + planned.NameKey
		}
		return planned
	}
	routines := namesakeKind{shared: namespace.Routines}
	for _, added := range diff.FunctionsAdded {
		routines.created = append(routines.created, routine(routineNoun(added), added.Name))
	}
	for _, removed := range diff.FunctionsRemoved {
		routines.dropped = append(routines.dropped, routine(routineNoun(removed), removed.Name))
	}
	for _, removed := range diff.ProceduresRemoved {
		routines.dropped = append(routines.dropped, routine("procedure", removed.Name))
	}
	var schemas namesakeKind
	for _, schema := range diff.SchemasAdded {
		schemas.created = append(schemas.created, schemaName(schema.Name, semantics))
	}
	for _, schema := range diff.SchemasRemoved {
		schemas.dropped = append(schemas.dropped, schemaName(schema, semantics))
	}
	return []namesakeKind{
		schemas,
		schemaScoped("table", namespace.Tables, diff.TablesAdded.Names(), diff.TablesRemoved.Names()),
		schemaScoped("view", namespace.Views, diff.ViewsAdded.Names(), diff.ViewsRemoved.Names()),
		schemaScoped("materialized view", namespace.MaterializedViews,
			diff.MaterializedViewsAdded.Names(), diff.MaterializedViewsRemoved.Names()),
		schemaScoped("sequence", namespace.Sequences, diff.SequencesAdded.Names(), diff.SequencesRemoved.Names()),
		routines,
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
	return plannedName{
		Namesake: identifiervalidation.NamesakeOf(name, semantics),
		noun:     noun, name: name, spelling: schemaSpelling(name, semantics),
	}
}

func schemaName(name string, semantics identifier.Semantics) plannedName {
	key := semantics.TableConflictKey(name)
	return plannedName{
		Namesake: identifiervalidation.Namesake{Key: key, NameKey: key},
		noun:     "schema", name: name, spelling: name,
	}
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
