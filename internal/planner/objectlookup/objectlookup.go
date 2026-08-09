// Package objectlookup answers one question for every migration planner: is the
// object a SchemaDiff entry names the object a schema declares, and which one.
//
// It is the general form of a mistake this repository has now made six times
// (stokaro/ptah#1276, #1311, #1347, #1351 and the sites this package replaces):
// two collections that describe the same object are matched with `==` or
// [slices.Contains], and the two sides do not spell the name the same way.
//
// # Why the spellings differ
//
// A diff records an object under the name the DESIRED schema spells, which is
// often bare. A schema converted back from an introspected database names every
// object the way the CATALOG reports it -- always qualified on MySQL and
// MariaDB, whose reader reports the database name for every view, on SQL Server,
// whose default schema is `dbo`, and on PostgreSQL for every object outside the
// schema being read. Both directions occur, because a document (HCL, SQL) may
// qualify what a Go annotation leaves bare and the reader may leave bare what a
// document qualifies.
//
// Table comparison in migration/schemadiff has always keyed through identifier
// semantics, so it reports `users` and `public.users` as ONE modified table. A
// planner that then matched that verdict against a declaration with `==` split
// them again, and the object it could not resolve produced no statement:
// silently, in the case of ADD COLUMN.
//
// # The rule
//
// Three tiers, applied in order:
//
//  1. An exact string match wins, so a schema that spells the name the way the
//     diff does is never re-interpreted.
//  2. Otherwise the two are compared as identities under [identifier.Semantics]:
//     an absent schema resolves to the dialect's default schema and each part is
//     folded by the dialect's comparison rule. This is what makes `orders` and
//     `dbo.orders` one object on SQL Server, and `Users` and `users` one table on
//     SQLite.
//  3. Otherwise the two are compared on their unqualified names alone, folded by
//     the same rule -- and ONLY where at least one side leaves the schema
//     unstated. Tier 3 supplies a schema nobody wrote down; it never overrules
//     one that is written down.
//
// A tier that finds two candidates resolves to nothing. Two objects of the same
// name in different schemas name no one object, and choosing between them would
// be a coin toss on which one a migration destroys.
//
// # Why tier 3 is gated on an unstated schema
//
// Tier 3 exists because a bare name carries no schema at all and a static
// default cannot always supply one. On MySQL and MariaDB the schema IS the
// database, so [identifier.ForDialect] leaves DefaultSchema empty and tier 2
// can never join the `mydb.v` a reader reports to the `v` a declaration spells.
// PostgreSQL has the same shape whenever the migration targets a schema other
// than `public`: the reader qualifies what the declaration left bare.
//
// Comparing unqualified names with no gate answers a different and much worse
// question. Measured through [go.5x5.cz/ptah/migration/planner.GenerateSchemaDiffSQLStatements]
// on PostgreSQL 17.10, a schema declaring exactly one `users` table -- in
// `reporting` -- and a diff naming `app.users` with one added column produced
// `ALTER TABLE "app"."users" ADD COLUMN "note" TEXT NOT NULL`, built from
// `reporting.users`'s field list. It exits 0 and information_schema.columns
// afterwards shows the column on `app.users`: a statement that applies cleanly
// to a relation the desired schema never declared. Both sides named a schema
// there, and they named different ones, so tier 3 now declines and the planner
// emits nothing rather than writing to the wrong object. Refusing to resolve
// costs a statement; guessing costs the wrong relation.
package objectlookup

import (
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/tableref"
)

// Find returns the single item that names the same object as name, or nil.
//
// nameOf reads the qualified name off an item. The rule Find applies is the
// three-tier one this package documents.
func Find[T any](items []T, name string, semantics identifier.Semantics, nameOf func(T) string) *T {
	for i := range items {
		if nameOf(items[i]) == name {
			return &items[i]
		}
	}
	if match := unique(items, nameOf, func(candidate string) bool {
		return sameIdentity(name, candidate, semantics)
	}); match != nil {
		return match
	}
	return unique(items, nameOf, func(candidate string) bool {
		return sameUnqualified(name, candidate, semantics)
	})
}

// Same reports whether two names refer to the same object, under the same three
// tiers [Find] applies.
//
// It is the form for the sites that hold one reference on each side rather than
// a collection to search -- a constraint's owning table against the table a
// TableDiff names, for instance. Those two come from different sources and do
// not have to spell the schema the same way, and `==` there answers "different
// object" for one object.
func Same(reference, candidate string, semantics identifier.Semantics) bool {
	return reference == candidate ||
		sameIdentity(reference, candidate, semantics) ||
		sameUnqualified(reference, candidate, semantics)
}

// Contains reports whether names holds the object name refers to, under the same
// rule [Find] applies.
//
// It is the identity-aware replacement for `slices.Contains(diff.Tables…, ref)`,
// which is the shape that produced stokaro/ptah#1351.
func Contains(names []string, name string, semantics identifier.Semantics) bool {
	return Find(names, name, semantics, func(candidate string) string { return candidate }) != nil
}

// View returns the declared view a diff entry names, or nil when the schema
// holds no single view under that name.
func View(views []goschema.View, name string, semantics identifier.Semantics) *goschema.View {
	return Find(views, name, semantics, func(view goschema.View) string { return view.Name })
}

// MaterializedView returns the declared materialized view a diff entry names, on
// the same terms as [View].
func MaterializedView(
	views []goschema.MaterializedView,
	name string,
	semantics identifier.Semantics,
) *goschema.MaterializedView {
	return Find(views, name, semantics, func(view goschema.MaterializedView) string { return view.Name })
}

// Qualified returns the single declared object a diff entry names, for any type
// that reports its own qualified name -- a table, a domain, a composite type, a
// range, a sequence, an enum.
func Qualified[T interface{ QualifiedName() string }](
	items []T,
	name string,
	semantics identifier.Semantics,
) *T {
	return Find(items, name, semantics, func(item T) string { return item.QualifiedName() })
}

// Trigger returns the declared trigger a diff entry names, or nil.
//
// A trigger is identified by its own name plus the table it hangs on, and it is
// the table that carries the schema, so the tiers [Find] applies are applied to
// the table half of the key. The trigger's own name is folded by the same
// comparison rule, because a dialect that folds a table name folds a trigger
// name with it.
func Trigger(
	triggers []goschema.Trigger,
	tableName, triggerName string,
	semantics identifier.Semantics,
) *goschema.Trigger {
	for i := range triggers {
		if triggers[i].Name == triggerName && triggers[i].Table == tableName {
			return &triggers[i]
		}
	}

	wanted := semantics.TableIdentityKey(triggerName)
	named := make([]goschema.Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		if semantics.TableIdentityKey(trigger.Name) == wanted {
			named = append(named, trigger)
		}
	}
	return Find(named, tableName, semantics, func(trigger goschema.Trigger) string { return trigger.Table })
}

// unique returns the only item the tier accepts, or nil when none or more than
// one does.
func unique[T any](items []T, nameOf func(T) string, accepts func(string) bool) *T {
	match := -1
	for i := range items {
		if !accepts(nameOf(items[i])) {
			continue
		}
		if match >= 0 {
			return nil
		}
		match = i
	}
	if match < 0 {
		return nil
	}
	return &items[match]
}

// sameIdentity is tier 2: both names resolved to a schema and folded by the
// dialect's comparison rule.
func sameIdentity(reference, candidate string, semantics identifier.Semantics) bool {
	return semantics.QualifiedTableIdentityKey(reference) ==
		semantics.QualifiedTableIdentityKey(candidate)
}

// sameUnqualified is tier 3: the object halves alone, folded by the same rule,
// and only where at least one side left the schema unstated.
//
// Parsing is delegated to tableref so an object whose own name contains a dot --
// quoted as `"tenant.data"` -- is not mistaken for a qualified one. A name
// tableref cannot parse states nothing this tier can read, so it is declined
// rather than resolved to something else; tier 1 has already accepted it against
// an identical spelling.
func sameUnqualified(reference, candidate string, semantics identifier.Semantics) bool {
	referenceRef, referenceOK := tableref.Parse(reference)
	candidateRef, candidateOK := tableref.Parse(candidate)
	if !referenceOK || !candidateOK {
		return false
	}
	if referenceRef.Qualified && candidateRef.Qualified {
		return false
	}
	return semantics.TableIdentityKey(referenceRef.Name) ==
		semantics.TableIdentityKey(candidateRef.Name)
}
