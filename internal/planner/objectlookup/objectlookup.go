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
// Three tiers, applied in order, and none of them guesses:
//
//  1. An exact string match wins, so a schema that spells the name the way the
//     diff does is never re-interpreted.
//  2. Otherwise the two are compared as identities under [identifier.Semantics]:
//     an absent schema resolves to the dialect's default schema and each part is
//     folded by the dialect's comparison rule. This is what makes `orders` and
//     `dbo.orders` one object on SQL Server, and `Users` and `users` one table on
//     SQLite.
//  3. Otherwise the two are compared on their unqualified names alone, folded by
//     the same rule. This tier is what resolves a name across two DIFFERENT
//     schemas, which is the MySQL and MariaDB case: the reader reports
//     `mydb.v` for a view the declaration calls `v`, and no static default
//     schema can be known for a dialect whose schema IS its database.
//
// A tier that finds two candidates resolves to nothing. Two objects of the same
// name in different schemas name no one object, and choosing between them would
// be a coin toss on which one a migration destroys.
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
	if match := unique(items, name, semantics.QualifiedTableIdentityKey, nameOf); match != nil {
		return match
	}
	return unique(items, name, unqualifiedKey(semantics), nameOf)
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

// unique returns the only item whose key matches name's, or nil when none or
// more than one does.
func unique[T any](items []T, name string, key func(string) string, nameOf func(T) string) *T {
	wanted := key(name)
	match := -1
	for i := range items {
		if key(nameOf(items[i])) != wanted {
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

// unqualifiedKey reads the object half of a possibly-qualified name and folds it
// by the dialect's rule, discarding the schema.
//
// Parsing is delegated to tableref so an object whose own name contains a dot --
// quoted as `"tenant.data"` -- is not mistaken for a qualified one. A name
// tableref cannot parse keeps its spelling rather than resolving to something
// else.
func unqualifiedKey(semantics identifier.Semantics) func(string) string {
	return func(value string) string {
		ref, ok := tableref.Parse(value)
		if !ok {
			return value
		}
		return semantics.TableIdentityKey(ref.Name)
	}
}
