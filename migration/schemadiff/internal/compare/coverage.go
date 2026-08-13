package compare

import (
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/tableref"
)

// Coverage pairs the limits the two sides of one comparison declared about
// themselves, and answers the only question a comparator has to ask of them:
// is this difference a difference, or is it something one side never looked at?
//
// The two sides gate opposite directions, and that asymmetry is the whole
// point (stokaro/ptah#1276):
//
//   - the DESIRED state's limits gate REMOVALS. An object the database has and
//     a description does not name is a removal only when that description
//     claimed to describe such objects. `ptah-compat schema inspect` omits the
//     block types the pinned Atlas community binary v1.3.0 refuses to read;
//     applying that document back to the database it came from used to plan
//     `DROP EXTENSION`, because a presentation decision had become deletion
//     intent.
//   - the CURRENT state's limits gate ADDITIONS. An object a description names
//     and a read did not report is a creation only when that read looked. A
//     role reader scoped to the inspected schema, or a table read scoped to one
//     schema, reports nothing about what it did not read, and planning
//     `CREATE ROLE` for a role that exists fails the migration.
//
// The additive half is narrower than the removal half, and the difference is
// the point of [Coverage.PlansAddition]: an object the DESIRED state names is
// intent, spelled out by the author, and "the reader did not look" is not a
// reason to discard it. It is only a reason to distrust the conclusion "this
// object is missing". Where the creation the planner would emit carries its own
// IF NOT EXISTS guard that converges every modeled semantic, that conclusion is
// not needed -- the statement is correct whether the object is there or not --
// so the record withholds nothing. PostgreSQL extension placement is the
// counterexample: CREATE EXTENSION IF NOT EXISTS can no-op in the wrong schema,
// so its comparator deliberately passes an unguarded policy. Where creation is
// not convergent, the addition is withheld AND collected in
// [Coverage.UndecidedAdditions], because a plan that quietly drops something the
// author wrote is the defect this package exists to prevent, not a conservative
// version of it.
//
// What was withheld is collected on the Coverage rather than on the SchemaDiff,
// deliberately. Every slice field of that type is a category of change the
// planner renders SQL for, which two guards enforce by reflecting over the
// struct (stokaro/ptah#1284), and a withheld addition is the opposite of a
// change: there is no statement, and a `migrate diff` that wrote a migration
// file holding none would be worse than the silence this replaces. It is a
// diagnostic, and it travels as one.
//
// The zero Coverage plans everything, so a comparison between two descriptions
// that declared no limits behaves exactly as it did before this type existed.
type Coverage struct {
	// Desired is what the desired-state description does not describe.
	Desired coverage.Set
	// Current is what the read of the live database did not look at.
	Current coverage.Set
	// undecided accumulates what the additive gate withheld. It is a pointer
	// because a Coverage is passed to every comparator by value, and it is
	// unexported because only [CoverageOf] may create the accumulator: a zero
	// Coverage records nothing, which is what a caller comparing two fully
	// authoritative descriptions wants.
	undecided *[]coverage.Object
}

// CoverageOf reads the limits both sides declared. Either side may be nil,
// which is a caller with nothing to compare rather than a caller claiming
// nothing is described.
func CoverageOf(generated *goschema.Database, database *types.DBSchema) Coverage {
	cov := Coverage{undecided: &[]coverage.Object{}}
	if generated != nil {
		cov.Desired = generated.NotDescribed
	}
	if database != nil {
		cov.Current = database.NotDescribed
	}
	return cov
}

// UndecidedAdditions returns the objects the desired state declared that this
// comparison could neither confirm present nor safely plan a creation for.
//
// The order is the order the comparators ran in, and within one comparator it
// follows a map iteration, so a caller that prints these must sort them first.
func (c Coverage) UndecidedAdditions() []coverage.Object {
	if c.undecided == nil {
		return nil
	}
	return *c.undecided
}

// PlansRemoval reports whether an object present in the database and absent
// from the desired state is a removal. Pass every spelling of the object's name
// the two sides might carry; the qualified and unqualified forms of one object
// are routinely spelled differently across the boundary, and a missed spelling
// restores the defect.
func (c Coverage) PlansRemoval(kind coverage.Kind, schema string, names ...string) bool {
	return c.Desired.DescribesIn(kind, schema, names...)
}

// PlansAddition reports whether the read's silence about an object is
// authoritative enough to conclude the object is missing.
//
// It answers only that question. Whether the creation is planned is
// [Coverage.keepPlannedAdditions], because a creation that converges every
// modeled semantic does not need the answer.
func (c Coverage) PlansAddition(kind coverage.Kind, schema string, names ...string) bool {
	return c.Current.DescribesIn(kind, schema, names...)
}

// creationGuard reports whether the statement the planner emits for one
// planned addition converges every modeled semantic when the object is already
// there.
//
// It is asked per object rather than per kind because the guard is per object.
// Measured on PostgreSQL 17.10 through `ptah-compat schema diff` against a
// desired document declaring one of each:
//
//	extension "citext" { if_not_exists = true }   placement may remain wrong, false
//	extension "citext" {}                         CREATE EXTENSION "citext";
//	sequence "s2" { if_not_exists = true, ... }   converges existence, true
//	sequence "s1" { ... }                         CREATE SEQUENCE "public"."s1" AS bigint;
//
// so a table keyed by [coverage.Kind] alone would be wrong in both directions.
type creationGuard func(name string) bool

// guardedCreations builds a [creationGuard] from the desired state's own
// IF NOT EXISTS flags. The map must be keyed exactly as the planned-addition
// list spells its entries, since that is what the guard is asked about.
func guardedCreations(guards map[string]bool) creationGuard {
	return func(name string) bool { return guards[name] }
}

// unguardedCreations is the guard for kinds whose creation has no conditional
// form in any dialect Ptah renders: CREATE ROLE, CREATE DOMAIN, CREATE TYPE and
// CREATE TABLE are all plain, so the object already existing fails the
// migration. Measured with the same command as [creationGuard].
func unguardedCreations() creationGuard {
	return func(string) bool { return false }
}

// alwaysGuardedCreations is the guard for kinds the planner itself makes
// repeatable. An RLS policy is emitted as `DROP POLICY IF EXISTS` immediately
// followed by `CREATE POLICY`, measured on PostgreSQL 17.10, so the pair
// succeeds whether or not the policy is there.
func alwaysGuardedCreations() creationGuard {
	return func(string) bool { return true }
}

// keepPlannedAdditions splits a list of planned additions into the ones this
// comparison plans and the ones it withholds.
//
// An addition is planned when the read's silence is authoritative, and ALSO
// when it is not but the caller says creation converges every modeled semantic:
// an object the desired state names is an explicit request, and such a creation
// grants it without needing to know whether the object is already there. Only
// the remainder -- a request the comparison can neither confirm nor safely
// grant -- is withheld, and the caller records it so that no surface reports a
// synced schema while holding it back (stokaro/ptah#1276).
//
// names maps a planned entry to every spelling of it the coverage record might
// use.
func (c Coverage) keepPlannedAdditions(
	kind coverage.Kind,
	planned []string,
	names func(string) (schema string, spellings []string),
	guarded creationGuard,
) (kept, withheld []string) {
	kept = c.keep(planned, func(entry string) bool {
		schema, spellings := names(entry)
		if c.PlansAddition(kind, schema, spellings...) {
			return true
		}
		if guarded(entry) {
			return true
		}
		withheld = append(withheld, entry)
		return false
	})
	return kept, withheld
}

// recordUndecidedAdditions collects what the coverage gate withheld, tagged
// with the kind so a surface can say what it is holding back.
func (c Coverage) recordUndecidedAdditions(kind coverage.Kind, withheld []string) {
	if c.undecided == nil {
		return
	}
	for _, name := range withheld {
		*c.undecided = append(*c.undecided, coverage.Object{Kind: kind, Name: name})
	}
}

// keepPlannedRemovals drops from a list of planned removals every object the
// desired state never claimed to describe.
func (c Coverage) keepPlannedRemovals(
	kind coverage.Kind,
	planned []string,
	names func(string) (schema string, spellings []string),
) []string {
	return c.keep(planned, func(entry string) bool {
		schema, spellings := names(entry)
		return c.PlansRemoval(kind, schema, spellings...)
	})
}

// keep preserves the input's nil-versus-empty shape, because several
// comparators publish an empty non-nil slice as part of their contract and a
// filter that quietly nils it changes an answer it was not asked about.
func (c Coverage) keep(planned []string, keepEntry func(string) bool) []string {
	if planned == nil {
		return nil
	}
	out := make([]string, 0, len(planned))
	for _, entry := range planned {
		if keepEntry(entry) {
			out = append(out, entry)
		}
	}
	return out
}

// qualifiedName is the spelling helper for objects a comparator keys by a
// possibly-qualified name. Both the whole name and its trailing part are
// offered as spellings, and the leading part is reported as the owning schema,
// so an object in a schema nobody read is recognized through the schema record
// even when nothing named the object itself.
//
// Splitting goes through [tableref.Parse] rather than a bare cut on the first
// dot, because a canonical reference quotes a component containing one and a
// naive split would take half an identifier for a schema.
func qualifiedName(name string) (schema string, spellings []string) {
	ref, ok := tableref.Parse(name)
	if !ok || !ref.Qualified {
		return "", []string{name}
	}
	return ref.Schema, []string{name, ref.Name}
}

// tableSchemaOnly reports only the schema a table belongs to. A table is
// covered through its schema rather than by its own name: the schema record is
// what a reader that skipped a whole namespace can produce, and the table names
// inside it are exactly what such a reader does not know.
func tableSchemaOnly(name string) (string, []string) {
	schema, _ := qualifiedName(name)
	return schema, nil
}

// globalName is the spelling helper for objects that have no owning schema:
// extensions are database-scoped and roles are cluster-scoped.
func globalName(name string) (string, []string) {
	return "", []string{name}
}
