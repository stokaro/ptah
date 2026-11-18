package schemadiff

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableidentity"
	"ptah.run/internal/tableref"
)

// Case-sensitive catalog names.
//
// A read whose names are the stored spelling (catalog.Database.StoredNames),
// on a target whose identifier semantics state an UnquotedFold, can hold a name
// no unquoted name reaches. Oracle is the one: a declared `orders` is rendered
// bare and stored as ORDERS, while a table someone created as "orders" is
// stored as orders. A stored name that is a plain identifier and not in the
// fold's case was created quoted.
//
// The comparison keeps such a name apart. A declaration that would otherwise
// replace one of these objects is refused first, because a declared
// `orders` beside a stored orders and no ORDERS is a request the plan cannot
// tell from a rename, and the plan it would make drops the object that holds
// the data. Then the read is given the spelling [catalogspelling.Spell]
// writes, `"orders"`, which [identifier.Semantics] keys exactly and the Oracle
// renderer writes quoted (stokaro/ptah#6243).

// refuseCaseOnlyNamesakes refuses a declared table, column, index,
// constraint, view, materialized view, sequence or trigger whose name reaches
// no object of its kind in current while a case-sensitive object there differs
// from it only in case.
//
// Only a name written in another case than the fold's can be such a request:
// a declared `orders` is ORDERS, and the author may have meant orders. A name
// already in the fold's case, such as ORDERS, and a name carrying its quotes,
// such as `"orders"`, each say exactly which object they mean, which is how a
// schema read from a database or from a file Ptah wrote names them, so
// neither is refused.
func refuseCaseOnlyNamesakes(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) error {
	if !current.StoredNames || semantics.UnquotedFold == "" {
		return nil
	}
	held := heldTables(current, target, semantics)
	for _, table := range desired.Tables {
		key := tableidentity.Subject(table.Schema, table.Name, target, semantics).Key()
		match, sensitive := pick(held[key], func(table catalog.Table) string { return table.Name }, target)
		if sensitive != nil && mayMean(table.Name, semantics) {
			return caseOnlyNamesakeError("table", table.Name, sensitive.Name, target, semantics)
		}
		if match == nil {
			continue
		}
		if err := refuseColumnNamesakes(desired, table, *match, target, semantics); err != nil {
			return err
		}
	}
	for _, kind := range namedKinds(desired, current) {
		if err := refuseMemberNamesakes(kind.noun, kind.declared, kind.held, target, semantics); err != nil {
			return err
		}
	}
	return nil
}

// mayMean reports whether declared, which reaches no object, may have meant a
// case-sensitive namesake: whether it is written without its quotes and in
// another case than the one the target folds it to.
func mayMean(declared string, semantics identifier.Semantics) bool {
	return !strings.HasPrefix(declared, `"`) && foldedName(declared, semantics) != declared
}

// namedKind is one kind of object whose names Oracle keeps once per schema
// rather than per table, with the names declared and the names read.
type namedKind struct {
	noun     string
	declared []string
	held     []string
}

func namedKinds(desired *schemamodel.Database, current *catalog.Database) []namedKind {
	return []namedKind{
		{noun: "index", declared: declaredIndexNames(desired), held: heldIndexNames(current)},
		{noun: "constraint", declared: declaredConstraintNames(desired), held: heldConstraintNames(current)},
		{noun: "view", declared: unqualifiedNames(desired.Views, func(view schemamodel.View) string { return view.Name }),
			held: names(current.Views, func(view catalog.View) string { return view.Name })},
		{noun: "materialized view",
			declared: unqualifiedNames(desired.MaterializedViews, func(view schemamodel.MaterializedView) string { return view.Name }),
			held:     names(current.MatViews, func(view catalog.MaterializedView) string { return view.Name })},
		{noun: "sequence", declared: unqualifiedNames(desired.Sequences, func(sequence schemamodel.Sequence) string { return sequence.Name }),
			held: names(current.Sequences, func(sequence catalog.Sequence) string { return sequence.Name })},
		{noun: "trigger", declared: unqualifiedNames(desired.Triggers, func(trigger schemamodel.Trigger) string { return trigger.Name }),
			held: names(current.Triggers, func(trigger catalog.Trigger) string { return trigger.Name })},
	}
}

// names lists the name of every element.
func names[T any](elements []T, name func(T) string) []string {
	listed := make([]string, 0, len(elements))
	for _, element := range elements {
		listed = append(listed, name(element))
	}
	return listed
}

// unqualifiedNames lists the name of every element without the schema a
// declaration may qualify it with.
func unqualifiedNames[T any](elements []T, name func(T) string) []string {
	listed := make([]string, 0, len(elements))
	for _, element := range elements {
		written := name(element)
		if ref, ok := tableref.Parse(written); ok && ref.Qualified && !strings.Contains(written, `"`) {
			written = ref.Name
		}
		listed = append(listed, written)
	}
	return listed
}

// heldTables groups the tables current holds by the identity a declared name
// folds to, so a table and its case-sensitive namesakes share one entry.
func heldTables(current *catalog.Database, target string, semantics identifier.Semantics) map[objectidentity.Key][]catalog.Table {
	held := make(map[objectidentity.Key][]catalog.Table, len(current.Tables))
	for _, table := range current.Tables {
		key := tableidentity.Subject(table.Schema, table.Name, target, semantics).Key()
		held[key] = append(held[key], table)
	}
	return held
}

// pick returns the candidate a declared name reaches, and the case-sensitive
// candidate it would be mistaken for when it reaches none.
func pick[T any](candidates []T, name func(T) string, target string) (match, sensitive *T) {
	for index := range candidates {
		if sqlident.CatalogName(target, name(candidates[index])) == name(candidates[index]) {
			return &candidates[index], nil
		}
		if sensitive == nil {
			sensitive = &candidates[index]
		}
	}
	return nil, sensitive
}

func refuseColumnNamesakes(desired *schemamodel.Database, table schemamodel.Table, held catalog.Table, target string, semantics identifier.Semantics) error {
	columns := make(map[string][]catalog.Column, len(held.Columns))
	for _, column := range held.Columns {
		key := semantics.ColumnIdentityKey(column.Name)
		columns[key] = append(columns[key], column)
	}
	for _, field := range desired.Fields {
		if field.StructName != table.StructName {
			continue
		}
		_, sensitive := pick(columns[semantics.ColumnIdentityKey(field.Name)], func(column catalog.Column) string { return column.Name }, target)
		if sensitive != nil && mayMean(field.Name, semantics) {
			return caseOnlyNamesakeError("column", table.Name+"."+field.Name, held.Name+"."+sqlident.CatalogName(target, sensitive.Name), target, semantics)
		}
	}
	return nil
}

// refuseMemberNamesakes is the check for a kind whose names Oracle keeps once
// per schema rather than per table, so a name is compared with every name of
// that kind the read holds.
func refuseMemberNamesakes(kind string, declared, held []string, target string, semantics identifier.Semantics) error {
	byKey := make(map[string][]string, len(held))
	for _, name := range held {
		key := semantics.IndexIdentityKey(name)
		byKey[key] = append(byKey[key], name)
	}
	for _, name := range declared {
		_, sensitive := pick(byKey[semantics.IndexIdentityKey(name)], func(name string) string { return name }, target)
		if sensitive != nil && mayMean(name, semantics) {
			return caseOnlyNamesakeError(kind, name, *sensitive, target, semantics)
		}
	}
	return nil
}

func declaredIndexNames(desired *schemamodel.Database) []string {
	names := make([]string, 0, len(desired.Indexes))
	for _, index := range desired.Indexes {
		if index.Name != "" {
			names = append(names, index.Name)
		}
	}
	return names
}

func declaredConstraintNames(desired *schemamodel.Database) []string {
	names := make([]string, 0, len(desired.Constraints))
	for _, constraint := range desired.Constraints {
		if constraint.Name != "" {
			names = append(names, constraint.Name)
		}
	}
	return names
}

func heldIndexNames(current *catalog.Database) []string {
	names := make([]string, 0, len(current.Indexes))
	for _, index := range current.Indexes {
		names = append(names, index.Name)
	}
	return names
}

func heldConstraintNames(current *catalog.Database) []string {
	names := make([]string, 0, len(current.Constraints))
	for _, constraint := range current.Constraints {
		names = append(names, constraint.Name)
	}
	return names
}

// caseOnlyNamesakeError names both spellings: the object the declaration
// means, in the case the target folds it to, and the case-sensitive one the
// database holds instead.
func caseOnlyNamesakeError(kind, declared, held, target string, semantics identifier.Semantics) error {
	return &RefusalError{cause: fmt.Errorf(
		"%w: declared %s %s names %s on %s, which the database does not hold; "+
			"the database holds the case-sensitive %s %s, which the declared name does not address. "+
			"Planning would drop %s and create %s, so rename one of them",
		ptaherr.ErrUnsupportedFeature, kind, declared, foldedName(declared, semantics), target,
		kind, sqlident.CatalogName(target, held), sqlident.CatalogName(target, held), foldedName(declared, semantics),
	)}
}

// foldedName is the stored spelling of a declared name: in the case the
// target folds it to.
func foldedName(declared string, semantics identifier.Semantics) string {
	if semantics.UnquotedFold == identifier.FoldLower {
		return strings.ToLower(declared)
	}
	return strings.ToUpper(declared)
}
