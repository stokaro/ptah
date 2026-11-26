package planner

import (
	"fmt"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/mysqlroutine"
	"ptah.run/internal/offlinenames"
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
// Two names are namesakes when the target's rules, read as
// [indexscope.ConflictSemantics] reads them, may make them one name and they
// are not spelled alike. Two names are spelled alike when they are in one
// schema and their own names have the same bytes: a PostgreSQL overload
// dropped while another is created is that, and so is an object a plan drops
// to create it again, which the planner orders. A routine name is keyed by
// the routine rule where the target's descriptor states one
// ([ptah.run/core/platform/descriptor.Comparison.RoutineNamesFold]), since
// the server folds it whatever its table rule says.
//
// The rules may make two names one when their keys agree, or when a part of
// one of them has no class under those rules and every other part may agree:
// an accent-insensitive collation reads `café` as `cafe`, and nothing but the
// server says which collation it has. Under the offline rules of a target
// whose catalog compares its names (see [offlinenames.Unanswered]), two ASCII
// names that differ only in case are one name under a case-insensitive
// collation, and a name with a non-ASCII rune has no class, so the refusal is
// marked as one a connection would have answered. That is the cost of
// planning such a target offline: a plan that creates an object and drops
// another of its kind in the same schema is refused when either name carries
// a non-ASCII rune, however unrelated the two names are. With a connection,
// the server resolves every name and no such pair is left.
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
	unanswered := offlinenames.Unanswered(semantics)
	semantics = indexscope.ConflictSemantics(semantics)
	for _, kind := range namesakeKinds(diff, semantics, target) {
		if err := refuseNamesakes(kind, target, unanswered); err != nil {
			return err
		}
	}
	return nil
}

// refuseNamesakes refuses the first object of kind the plan creates whose
// name may be the name of one it drops; see [refuseCreatedAndDroppedNamesakes].
func refuseNamesakes(kind namesakeKind, target string, unanswered bool) error {
	for _, created := range kind.created {
		for _, removed := range kind.dropped {
			if created.spelling != removed.spelling && created.mayBe(removed) {
				return namesakeError(created, removed, target, unanswered)
			}
		}
	}
	return nil
}

// namesakeError names a created object and a dropped one the target may read
// as one; see [refuseCreatedAndDroppedNamesakes] for when each wording is
// written.
func namesakeError(created, removed plannedName, target string, unanswered bool) error {
	pair := fmt.Sprintf("the plan creates %s %s and drops %s %s", created.noun, created.name, removed.noun, removed.name)
	switch {
	case unanswered && (created.unresolved() || removed.unresolved()):
		return offlinenames.Mark(fmt.Errorf(
			"%w: %s, and without a connection Ptah cannot tell whether they are one name under the database collation",
			ptaherr.ErrInvalidSchemaDiff, pair,
		))
	case unanswered:
		return offlinenames.Mark(fmt.Errorf(
			"%w: %s, which differ only in case, so under a case-insensitive collation the drop would remove the object "+
				"the create makes, and without a connection Ptah cannot tell which collation the database uses; "+
				"declare the object as the database spells it",
			ptaherr.ErrInvalidSchemaDiff, pair,
		))
	case created.unresolved() || removed.unresolved():
		return fmt.Errorf(
			"%w: %s, and %s was not asked whether they are one name, so the drop may remove the object the create makes",
			ptaherr.ErrInvalidSchemaDiff, pair, target,
		)
	}
	return fmt.Errorf(
		"%w: %s, and %s reads them as one name, so the drop would remove the object the create makes; "+
			"declare the object as the database spells it",
		ptaherr.ErrInvalidSchemaDiff, pair, target,
	)
}

// plannedName is one object a plan creates or drops: how a message names it,
// the key the target's rules give it, the keys of its schema and of its own
// name, and its spelling with the schema written out. schemaKey is empty for
// a schema, and for a name whose schema the target's rules do not place.
type plannedName struct {
	noun      string
	name      string
	key       string
	schemaKey string
	nameKey   string
	spelling  string
}

// unresolved reports whether the name, or its schema, has no class under the
// rules its key was made with.
func (n plannedName) unresolved() bool {
	return n.schemaKey == unresolvedNameKey || n.nameKey == unresolvedNameKey
}

// mayBe reports whether the target may read n and other as one name: their
// keys agree, or each part of one may be the same part of the other because
// one of the two has no class.
func (n plannedName) mayBe(other plannedName) bool {
	if n.key == other.key {
		return true
	}
	if !n.unresolved() && !other.unresolved() {
		return false
	}
	return partsMayAgree(n.schemaKey, other.schemaKey) && partsMayAgree(n.nameKey, other.nameKey)
}

// partsMayAgree reports whether two parts of a name may be one part: their
// keys agree, either has no class, or either is not placed.
func partsMayAgree(a, b string) bool {
	return a == b || a == "" || b == "" || a == unresolvedNameKey || b == unresolvedNameKey
}

// unresolvedNameKey is the conflict key of a name the target's rules give no
// class.
var unresolvedNameKey = identifier.ComparisonCatalogUnknown.ConflictKey("")

// namesakeKind is the objects of one kind a plan creates and drops.
type namesakeKind struct {
	created []plannedName
	dropped []plannedName
}

// namesakeKinds lists, for each kind of object a schema holds by name, the
// objects of it the diff creates and drops. A function and a procedure are one
// kind, because on SQL Server they share one namespace. A routine name is
// keyed by the routine rule of the target's descriptor where it states one.
func namesakeKinds(diff *difftypes.SchemaDiff, semantics identifier.Semantics, target string) []namesakeKind {
	described, _ := platform.Lookup(target)
	routinesFold := described.Comparison.RoutineNamesFold
	schemaScoped := func(noun string, created, dropped []string) namesakeKind {
		return namesakeKind{
			created: schemaScopedNames(noun, created, semantics),
			dropped: schemaScopedNames(noun, dropped, semantics),
		}
	}
	routine := func(noun, name string) plannedName {
		planned := schemaScopedName(noun, name, semantics)
		if routinesFold && planned.nameKey != unresolvedNameKey {
			// The server folds a routine name whatever its table rule says;
			// the comparison folds it as [mysqlroutine.IdentityKey] does.
			planned.nameKey = mysqlroutine.IdentityKey(planned.nameKey)
			planned.key = planned.schemaKey + "\x00" + planned.nameKey
		}
		return planned
	}
	routines := namesakeKind{}
	for _, added := range diff.FunctionsAdded {
		routines.created = append(routines.created, routine(routineNoun(added), added.Name))
	}
	for _, removed := range diff.FunctionsRemoved {
		routines.dropped = append(routines.dropped, routine(routineNoun(removed), removed.Name))
	}
	for _, removed := range diff.ProceduresRemoved {
		routines.dropped = append(routines.dropped, routine("procedure", removed.Name))
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
	planned := plannedName{
		noun: noun, name: name, key: semantics.QualifiedTableConflictKey(name), spelling: schemaSpelling(name, semantics),
		nameKey: semantics.TableConflictKey(name),
	}
	ref, ok := tableref.Parse(name)
	if !ok {
		return planned
	}
	planned.nameKey = semantics.TableConflictKey(ref.Name)
	schema := ref.Schema
	if !ref.Qualified {
		schema = semantics.DefaultSchema
	}
	if schema != "" {
		planned.schemaKey = semantics.TableConflictKey(schema)
	}
	return planned
}

func schemaName(name string, semantics identifier.Semantics) plannedName {
	key := semantics.TableConflictKey(name)
	return plannedName{noun: "schema", name: name, key: key, nameKey: key, spelling: name}
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
