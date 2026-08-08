// Package objectlookup resolves the views, materialized views and triggers a
// SchemaDiff entry names against the schema a migration planner renders from.
package objectlookup

import (
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/tableref"
)

// View returns the declared view a diff entry names, or nil when the schema
// holds no single view under that name.
//
// The two sides do not always spell the name the same way, and comparing the
// strings directly loses whole objects. A diff records a modified view under the
// name the Go schema spells, which is normally unqualified, while a schema
// converted back from an introspected database names every view
// "<schema>.<view>" through dbschema/types.DBView.QualifiedName -- always on
// MySQL and MariaDB, whose reader reports the database name for every view, and
// on PostgreSQL for every view outside the schema being read. The down direction
// plans against exactly that converted schema, so a planner that compared the
// two spellings found nothing, rendered no statement, and produced a rollback
// that said "No rollback operations needed" while the view kept its post-up body
// (issue #1287).
//
// An exact match wins, so a schema that spells the name the same way is never
// re-interpreted. Otherwise the two are compared on their unqualified names, and
// only a single candidate is accepted: two views of the same name in different
// schemas name no one object, and choosing between them would be a guess.
func View(views []goschema.View, name string) *goschema.View {
	return resolve(views, name, func(view goschema.View) string { return view.Name })
}

// MaterializedView returns the declared materialized view a diff entry names,
// on the same terms as [View].
func MaterializedView(views []goschema.MaterializedView, name string) *goschema.MaterializedView {
	return resolve(views, name, func(view goschema.MaterializedView) string { return view.Name })
}

// Trigger returns the declared trigger a diff entry names, or nil.
//
// A trigger is identified by its own name plus the table it hangs on, and it is
// the table that carries the schema, so the same qualified-versus-unqualified
// split [View] describes applies to the table half of the key.
func Trigger(triggers []goschema.Trigger, tableName, triggerName string) *goschema.Trigger {
	for i := range triggers {
		if triggers[i].Table == tableName && triggers[i].Name == triggerName {
			return &triggers[i]
		}
	}

	ref, ok := tableref.Parse(tableName)
	if !ok {
		return nil
	}
	match := -1
	for i := range triggers {
		if triggers[i].Name != triggerName {
			continue
		}
		candidate, ok := tableref.Parse(triggers[i].Table)
		if !ok || candidate.Name != ref.Name {
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
	return &triggers[match]
}

// resolve implements the exact-then-unqualified match [View] documents.
func resolve[T any](items []T, name string, nameOf func(T) string) *T {
	for i := range items {
		if nameOf(items[i]) == name {
			return &items[i]
		}
	}

	ref, ok := tableref.Parse(name)
	if !ok {
		return nil
	}
	match := -1
	for i := range items {
		candidate, ok := tableref.Parse(nameOf(items[i]))
		if !ok || candidate.Name != ref.Name {
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
