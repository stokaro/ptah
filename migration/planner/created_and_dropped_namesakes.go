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
// have the same bytes, both read under the target's own rules: `ccs`,
// `dbo.ccs` and `[dbo].[ccs]` are spelled alike where the default schema is
// dbo, and `DBO.f` and `f` are not, whatever the folded rules say of them. A
// routine name is keyed by the routine rule where the target's descriptor
// states one ([ptah.run/core/platform/descriptor.Comparison.RoutineNamesFold]),
// since the server folds it whatever its table rule says.
//
// A routine created beside one of its kind spelled alike that the plan drops
// is another overload where the target holds overloads apart
// ([descriptor.Comparison.RoutineOverloads]), as PostgreSQL does, and the plan
// runs. On any other target the two are one routine, and the planner writes
// the create before the drop: on SQL Server `CREATE OR ALTER FUNCTION
// [dbo].[f]` replaces the function and `DROP FUNCTION [dbo].[f]` then removes
// it. The comparison plans a routine whose definition changes as a
// modification ([difftypes.FunctionDiff]), which the planner replaces by DROP
// and CREATE and gives its privileges back, so only a diff built by hand
// carries such a pair, and it is refused. A schema created and dropped under
// one spelling is refused on every target for the same reason: every planner
// creates a schema only where none exists, `CREATE SCHEMA IF NOT EXISTS` or
// its SQL Server guard, and drops it after, and on MySQL that drop is `DROP
// DATABASE`, which takes the tables with it. The comparison keys a schema by
// its identity, so it never asks for both. A pair of another kind spelled
// alike is not judged here: a view or a sequence is created by a statement
// the server refuses while the object exists, and the PostgreSQL planner
// drops a view before it creates it again.
//
// Under the offline rules of a target whose catalog compares its names (see
// [offlinenames.Unanswered]), no rule says that two names are two: a
// collation with contractions reads `ccs` as `cscs` under Hungarian_CI_AS,
// and nothing but the server says which collation it has. Without a
// connection such a plan therefore never creates an object and drops another
// of its kind unless the two are spelled alike, whatever the two names are
// (stokaro/ptah#6746). That is the cost of planning such a target offline,
// and the refusal says that no connection answered. With a connection, the
// server resolves every name, and only the names it reads as one are refused.
//
// Roles are judged where the target compares a role name under its catalog's
// rules ([offlinenames.RolesByCatalog]). A role is created with CREATE ROLE,
// which the server refuses while a namesake exists: measured on SQL Server
// 2025 under a case-insensitive collation, CREATE ROLE [APP] beside the role
// App fails with error 15023. Nothing is lost there, so the refusal only says
// so before anything runs, and the offline rules apply as they stand: two
// ASCII role names that differ beyond case are two names. The comparison never
// drops a role, so only a diff built by hand carries such a pair.
//
// Objects of two kinds that share a namespace in a schema
// ([descriptor.Comparison.SharedObjectNamespace]) are refused when their names
// may be one name, spelled alike or not. The planner writes every create
// before every drop of another kind, so the create runs while the other
// object holds the name, and the server refuses it: measured on SQL Server
// 2025, `CREATE OR ALTER PROCEDURE [dbo].[REPORT]` beside the view dbo.Report
// fails with error 2010 (stokaro/ptah#6741). A function and a procedure are
// two kinds here: a plan that replaces the function dbo.x by the procedure
// dbo.x creates the procedure first, and fails the same way. The plan is not
// reordered to drop first: the drop takes every privilege on the object, which
// only the replacement of a changed routine ([difftypes.FunctionDiff]) gives
// back, and the rollback of a pair of two kinds would create the old object
// while the new one holds the name. The refusal names the two migrations that
// make the change instead. Nothing is lost there, so the offline rules apply
// as they stand: two ASCII names that differ beyond case are two names.
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
	described, _ := platform.Lookup(target)
	rules := namesakeRules{
		target:     target,
		unanswered: offlinenames.Unanswered(semantics),
		overloads:  described.Comparison.RoutineOverloads,
	}
	kinds := namesakeKinds(diff, semantics, described, target)
	for _, kind := range kinds {
		if err := rules.refuseNamesakes(kind); err != nil {
			return err
		}
	}
	return refuseNamesakesOfAnotherKind(kinds, target)
}

// namesakeRules is how a plan's namesakes are judged on target. unanswered
// reports that the target's catalog compares its names and no connection
// answered how ([offlinenames.Unanswered]); overloads that the target holds
// two routines of one kind and one name apart
// ([descriptor.Comparison.RoutineOverloads]).
type namesakeRules struct {
	target     string
	unanswered bool
	overloads  bool
}

// refuseNamesakes refuses the first object of kind the plan creates whose
// name may be the name of one it drops; see [refuseCreatedAndDroppedNamesakes].
func (r namesakeRules) refuseNamesakes(kind namesakeKind) error {
	for _, created := range kind.created {
		for _, removed := range kind.dropped {
			switch {
			case created.spelling == removed.spelling:
				if kind.oneObject {
					return oneSchemaError(created, removed)
				}
				if kind.routines && created.noun == removed.noun && !r.overloads {
					return r.oneRoutineError(created, removed)
				}
			case (r.unanswered && !kind.loss.refused) || created.MayBe(removed.Namesake):
				return r.namesakeError(kind.loss, created, removed)
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
						return anotherKindError(created, removed, target)
					}
				}
			}
		}
	}
	return nil
}

// anotherKindError names a created object and a dropped one of another kind
// that target keeps in one namespace, and the two migrations that make the
// change. Two names spelled alike are one name; any other pair is one the
// target may read as one.
func anotherKindError(created, removed plannedName, target string) error {
	reading := "where it may read the two names as one"
	if created.spelling == removed.spelling {
		reading = "where the two are one name"
	}
	return fmt.Errorf(
		"%w: the plan creates %s %s and drops %s %s, and %s keeps both kinds in one namespace in a schema, "+
			"%s, so the create would fail while the %s exists; make the change in two migrations, "+
			"the first dropping the %s %s and the second creating the %s %s",
		ptaherr.ErrInvalidSchemaDiff, created.noun, created.name, removed.noun, removed.name, target,
		reading, removed.noun, removed.noun, removed.name, created.noun, created.name,
	)
}

// oneRoutineError names a created routine and a dropped one of its kind spelled
// alike on a target that holds one routine of a kind under a name; see
// [refuseCreatedAndDroppedNamesakes].
func (r namesakeRules) oneRoutineError(created, removed plannedName) error {
	return fmt.Errorf(
		"%w: the plan creates %s %s and drops %s %s, which are one routine on %s, since it has no routine "+
			"overloading, and a plan cannot both create it and drop it; a routine whose definition changes is "+
			"a modification, which the planner replaces",
		ptaherr.ErrInvalidSchemaDiff, created.noun, created.name, removed.noun, removed.name, r.target,
	)
}

// oneSchemaError names a schema a plan creates and drops under one spelling;
// see [refuseCreatedAndDroppedNamesakes].
func oneSchemaError(created, removed plannedName) error {
	return fmt.Errorf(
		"%w: the plan creates schema %s and drops schema %s, which are one schema, so the drop would remove the "+
			"schema the create keeps",
		ptaherr.ErrInvalidSchemaDiff, created.name, removed.name,
	)
}

// namesakeError names a created object and a dropped one the target may read
// as one, and what the plan would do to them, as loss words it; see
// [refuseCreatedAndDroppedNamesakes] for when each wording is written.
func (r namesakeRules) namesakeError(loss loss, created, removed plannedName) error {
	pair := fmt.Sprintf("the plan creates %s %s and drops %s %s", created.noun, created.name, removed.noun, removed.name)
	resolved := !created.Unresolved() && !removed.Unresolved()
	switch {
	case r.unanswered && resolved && created.MayBe(removed.Namesake):
		return fmt.Errorf(
			"%w: %s, which differ only in case, so under a case-insensitive collation %s, "+
				"and without a connection Ptah cannot tell which collation the database uses; "+
				"declare the object as the database spells it",
			ptaherr.ErrInvalidSchemaDiff, pair, loss.would,
		)
	case r.unanswered:
		return fmt.Errorf(
			"%w: %s, and without a connection Ptah cannot tell whether they are one name under the database collation, "+
				"so %s; compare with a connection to the database, which answers how it compares the names",
			ptaherr.ErrInvalidSchemaDiff, pair, loss.may,
		)
	case !resolved:
		return fmt.Errorf(
			"%w: %s, and %s was not asked whether they are one name, so %s",
			ptaherr.ErrInvalidSchemaDiff, pair, r.target, loss.may,
		)
	}
	return fmt.Errorf(
		"%w: %s, and %s reads them as one name, so %s; declare the object as the database spells it",
		ptaherr.ErrInvalidSchemaDiff, pair, r.target, loss.would,
	)
}

// loss says what a plan that creates an object and drops a namesake of it does
// on the server: would where the server reads the two names as one, may where
// nothing says whether it does. refused reports that the server refuses the
// create, so nothing is lost and only names that may be one are judged, under
// the offline rules as they stand.
type loss struct {
	would, may string
	refused    bool
}

var (
	// lostObject is a create that runs, which the drop after it undoes.
	lostObject = loss{
		would: "the drop would remove the object the create makes",
		may:   "the drop may remove the object the create makes",
	}
	// refusedRole is CREATE ROLE, which the server refuses while the role the
	// plan drops after it holds the name.
	refusedRole = loss{
		would:   "the create would fail while the role the plan drops exists",
		may:     "the create may fail while the role the plan drops exists",
		refused: true,
	}
)

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
// ([descriptor.Comparison.SharedObjectNamespace]), routines that the kind is
// the routines, oneObject that two of its objects spelled alike are one
// object whatever the target, and loss what a plan does to a pair of its
// namesakes.
type namesakeKind struct {
	shared    bool
	routines  bool
	oneObject bool
	loss      loss
	created   []plannedName
	dropped   []plannedName
}

// namesakeKinds lists, for each kind of object a schema holds by name, and
// for the roles where the target compares a role name under its catalog's
// rules, the objects of it the diff creates and drops. Functions and
// procedures are listed together, because a plan can replace a routine of one
// kind by a routine of the other under one name; each keeps its noun, which
// [refuseNamesakesOfAnotherKind] reads as its kind. A name is compared under
// rules, read as [indexscope.ConflictSemantics] reads them, and spelled under
// rules as they stand. A routine name is keyed by the routine rule of the
// target's descriptor where it states one.
func namesakeKinds(diff *difftypes.SchemaDiff, rules identifier.Semantics, described descriptor.Dialect, target string) []namesakeKind {
	conflict := indexscope.ConflictSemantics(rules)
	routinesFold := described.Comparison.RoutineNamesFold
	namespace := described.Comparison.SharedObjectNamespace
	schemaScoped := func(noun string, shared bool, created, dropped []string) namesakeKind {
		return namesakeKind{
			shared:  shared,
			loss:    lostObject,
			created: schemaScopedNames(noun, created, conflict, rules),
			dropped: schemaScopedNames(noun, dropped, conflict, rules),
		}
	}
	routine := func(noun, name string) plannedName {
		planned := schemaScopedName(noun, name, conflict, rules)
		if routinesFold && !planned.Unresolved() {
			// The server folds a routine name whatever its table rule says;
			// the comparison folds it as [mysqlroutine.IdentityKey] does.
			planned.NameKey = mysqlroutine.IdentityKey(planned.NameKey)
			planned.Key = planned.SchemaKey + "\x00" + planned.NameKey
		}
		return planned
	}
	routines := namesakeKind{shared: namespace.Routines, routines: true, loss: lostObject}
	for _, added := range diff.FunctionsAdded {
		routines.created = append(routines.created, routine(routineNoun(added), added.Name))
	}
	for _, removed := range diff.FunctionsRemoved {
		routines.dropped = append(routines.dropped, routine(routineNoun(removed), removed.Name))
	}
	for _, removed := range diff.ProceduresRemoved {
		routines.dropped = append(routines.dropped, routine("procedure", removed.Name))
	}
	schemas := namesakeKind{oneObject: true, loss: lostObject}
	for _, schema := range diff.SchemasAdded {
		schemas.created = append(schemas.created, unscopedName("schema", schema.Name, conflict))
	}
	for _, schema := range diff.SchemasRemoved {
		schemas.dropped = append(schemas.dropped, unscopedName("schema", schema, conflict))
	}
	roles := namesakeKind{loss: refusedRole}
	if offlinenames.RolesByCatalog(target) {
		for _, role := range diff.RolesAdded.Names() {
			roles.created = append(roles.created, unscopedName("role", role, conflict))
		}
		for _, role := range diff.RolesRemoved.Names() {
			roles.dropped = append(roles.dropped, unscopedName("role", role, conflict))
		}
	}
	return []namesakeKind{
		schemas,
		roles,
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

func schemaScopedNames(noun string, names []string, conflict, rules identifier.Semantics) []plannedName {
	planned := make([]plannedName, 0, len(names))
	for _, name := range names {
		planned = append(planned, schemaScopedName(noun, name, conflict, rules))
	}
	return planned
}

// schemaScopedName is an object named in a schema, such as a table or a
// procedure, whose name may carry the schema: compared under conflict, and
// spelled and named in a message with the schema written out under rules.
func schemaScopedName(noun, name string, conflict, rules identifier.Semantics) plannedName {
	return plannedName{
		Namesake: identifiervalidation.NamesakeOf(name, conflict),
		noun:     noun, name: qualifiedName(name, rules.DefaultSchema), spelling: schemaSpelling(name, rules),
	}
}

// unscopedName is an object that no schema holds, a schema or a role, compared
// under conflict and spelled as written.
func unscopedName(noun, name string, conflict identifier.Semantics) plannedName {
	key := conflict.TableConflictKey(name)
	return plannedName{
		Namesake: identifiervalidation.Namesake{Key: key, NameKey: key},
		noun:     noun, name: name, spelling: name,
	}
}

// qualifiedName is name with the schema it is in written out, as a refusal
// names it, so that both objects of a pair are named the same way: `f` is
// dbo.f where the default schema is dbo.
func qualifiedName(name, defaultSchema string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	if !ref.Qualified {
		ref.Schema = defaultSchema
	}
	return tableref.Canonical(ref.Schema, ref.Name)
}

// schemaSpelling is name with the schema it is in written out, so that
// `orders`, `dbo.orders` and `[dbo].[orders]` are one spelling on a target
// whose default schema is dbo, and `orders` and `ORDERS` are two. The schema
// is keyed by rules as they stand, which a caller does not fold: under the
// offline SQL Server rules `DBO` and `dbo` are two spellings.
func schemaSpelling(name string, rules identifier.Semantics) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	schema := ref.Schema
	if !ref.Qualified {
		schema = rules.DefaultSchema
	}
	return rules.TableIdentityKey(schema) + "\x00" + ref.Name
}
