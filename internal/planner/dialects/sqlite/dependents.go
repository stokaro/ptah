package sqlite

import (
	"maps"
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// legacyAlterOn and legacyAlterOff bracket the drop and the rename of a
// rebuild whose table a view, or a trigger of another table, names.
//
// From SQLite 3.26 a table rename checks every view and trigger of the schema
// and rewrites the references to the renamed table. When the rebuild renames
// its copy, the old table has just been dropped, so a view that selects from
// it fails the check and the rename with it: `error in view v: no such table:
// main.kinds`, measured on SQLite 3.51 through the driver Ptah applies with.
// With legacy_alter_table on, the rename leaves views and triggers as they are,
// and they name the table again once the copy takes its name. SQLite's ALTER
// TABLE page names this pragma for the procedure a rebuild follows.
//
// The pragma takes effect inside a transaction, unlike foreign_keys, so it
// stays in the plan where it is needed. The plan cannot read the value the
// connection held, so it writes off after the rename; the SQLite writer puts
// back the value it found when the transaction ends (internal/dbschema/sqlite),
// and `migrations up` does after each file, so neither the pair nor a failure
// between them changes how a later rename on the connection treats views.
const (
	legacyAlterOn  = "PRAGMA legacy_alter_table = on;"
	legacyAlterOff = "PRAGMA legacy_alter_table = off;"
)

// dependentsOf reports whether a view, or a trigger of another table, may name
// table, so that its rebuild needs the rename bracket. It refuses where such an
// object reads a column the rebuilt table drops: with the bracket the rebuild
// would succeed and leave the object to fail when it runs.
//
// It reads what the plan can see: every declared view and trigger, the views
// and triggers an exclude selector keeps out of scope, and the views the plan
// removes after the rebuild. A view or trigger the plan modifies or removes
// whose current text the diff does not carry counts as naming the table. An
// object it cannot see gets no bracket, so its rename check still runs and
// fails the rebuild loudly instead.
func dependentsOf(diff *difftypes.SchemaDiff, table schemamodel.Table, dropped map[string]bool, semantics identifier.Semantics) (bool, error) {
	check := dependentCheck{table: table, columns: slices.Sorted(maps.Keys(dropped)), semantics: semantics}
	viewsName, err := check.views(diff)
	if err != nil {
		return false, err
	}
	triggersName, err := check.triggers(diff)
	if err != nil {
		return false, err
	}
	return viewsName || triggersName, nil
}

// dependentCheck holds the rebuilt table and the columns its rebuild drops.
type dependentCheck struct {
	table     schemamodel.Table
	columns   []string
	semantics identifier.Semantics
}

// reads returns the first dropped column read reports, and whether there is one.
func (d dependentCheck) reads(read func(relationReference) bool) (string, bool) {
	for _, column := range d.columns {
		if read(relationReference{table: d.table.Name, column: column, semantics: d.semantics}) {
			return column, true
		}
	}
	return "", false
}

// other reports whether name is a table other than the rebuilt one.
func (d dependentCheck) other(name string) bool {
	return d.semantics.QualifiedTableIdentityKey(name) != d.semantics.QualifiedTableIdentityKey(d.table.Name)
}

// views checks the declared views, those out of scope and those the plan
// removes.
func (d dependentCheck) views(diff *difftypes.SchemaDiff) (bool, error) {
	named := len(diff.ViewsModified) > 0
	for _, view := range slices.Concat(diff.DeclaredViewLikes.Views, outOfScopeViews(diff)) {
		if column, ok := d.reads(func(r relationReference) bool { return r.viewReads(view.Body) }); ok {
			return false, brokenDependent(d.table, "view", view.Name, column)
		}
		named = named || namesTable(view.Body, d.table.Name, d.semantics)
	}
	for _, view := range diff.ViewsRemoved {
		named = named || namesTable(view.Body, d.table.Name, d.semantics)
	}
	return named, nil
}

// triggers checks the declared triggers of other tables, those out of scope,
// and those the plan modifies or removes.
func (d dependentCheck) triggers(diff *difftypes.SchemaDiff) (bool, error) {
	type trigger struct{ name, table, text, when string }
	var seen []trigger
	for _, declared := range diff.DeclaredTriggers {
		seen = append(seen, trigger{declared.Name, declared.Table, declared.Body, declared.When})
	}
	for _, kept := range diff.CurrentOutOfScope.Triggers {
		text := kept.Definition
		if text == "" {
			text = kept.Body
		}
		seen = append(seen, trigger{kept.Name, kept.Table, text, kept.When})
	}
	named := false
	for _, trigger := range seen {
		if !d.other(trigger.table) {
			continue
		}
		if column, ok := d.reads(func(r relationReference) bool { return r.triggerReads(trigger.text, trigger.when) }); ok {
			return false, brokenDependent(d.table, "trigger", trigger.name, column)
		}
		named = named || namesTable(trigger.text+" "+trigger.when, d.table.Name, d.semantics)
	}
	for _, removed := range diff.TriggersRemoved {
		named = named || d.other(removed.TableName)
	}
	for _, modified := range diff.TriggersModified {
		named = named || d.other(modified.TableName)
	}
	return named, nil
}

// outOfScopeViews are the views an exclude selector keeps out of scope, in the
// shape a declared view has: the plan reads their name and query alike.
func outOfScopeViews(diff *difftypes.SchemaDiff) []schemamodel.View {
	views := make([]schemamodel.View, 0, len(diff.CurrentOutOfScope.Views))
	for _, view := range diff.CurrentOutOfScope.Views {
		views = append(views, schemamodel.View{Name: view.Name, Body: view.Body})
	}
	return views
}

func brokenDependent(table schemamodel.Table, kind, name, column string) error {
	return unsupportedFeaturef(
		"rebuilding table %s would break %s %s: it reads column %s, which the rebuilt table drops. "+
			"Keep the column, or change the %s first",
		table.QualifiedName(), kind, name, column, kind)
}
