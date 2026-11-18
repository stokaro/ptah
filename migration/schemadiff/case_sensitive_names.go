package schemadiff

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableidentity"
)

// Case-sensitive catalog names.
//
// The identifier semantics of a live read state an UnquotedFold
// (identifier.ForCatalog) for a target that records a name written without
// quotes in one case and keeps a quoted name exactly. Oracle is the one: a
// declared `orders` is rendered bare and stored as ORDERS, while a table
// someone created as "orders" is stored as orders. The catalog reports
// both as they are stored, and a name in the catalog that is a plain identifier
// and not in the fold's case was created quoted, so no declared name reaches it.
//
// The comparison keeps such a name apart. A declaration that would otherwise
// replace one of these objects is refused first, because a declared
// `orders` beside a stored orders and no ORDERS is a request the plan cannot
// tell from a rename, and the plan it would make drops the table that holds
// the data. Then the read is given the model name [sqlident.CatalogName]
// returns, `"orders"`, which [identifier.Semantics] keys exactly and the Oracle
// renderer writes quoted (stokaro/ptah#6243).

// refuseCaseOnlyNamesakes refuses a declared table, column, index or constraint
// whose name reaches no object in current while a case-sensitive object there
// differs from it only in case.
func refuseCaseOnlyNamesakes(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) error {
	if semantics.UnquotedFold == "" {
		return nil
	}
	held := heldTables(current, target, semantics)
	for _, table := range desired.Tables {
		key := tableidentity.Subject(table.Schema, table.Name, target, semantics).Key()
		match, sensitive := pick(held[key], func(table catalog.Table) string { return table.Name }, target)
		if sensitive != nil {
			return caseOnlyNamesakeError("table", table.Name, sensitive.Name, target, semantics)
		}
		if match == nil {
			continue
		}
		if err := refuseColumnNamesakes(desired, table, *match, target, semantics); err != nil {
			return err
		}
	}
	if err := refuseMemberNamesakes("index", declaredIndexNames(desired), heldIndexNames(current), target, semantics); err != nil {
		return err
	}
	return refuseMemberNamesakes("constraint", declaredConstraintNames(desired), heldConstraintNames(current), target, semantics)
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
		if sensitive != nil {
			return caseOnlyNamesakeError("column", table.Name+"."+field.Name, held.Name+"."+sqlident.CatalogName(target, sensitive.Name), target, semantics)
		}
	}
	return nil
}

// refuseMemberNamesakes is the check for a kind whose names Oracle keeps once
// per schema rather than per table, an index or a constraint, so a name is
// compared with every name of that kind the read holds.
func refuseMemberNamesakes(kind string, declared, held []string, target string, semantics identifier.Semantics) error {
	byKey := make(map[string][]string, len(held))
	for _, name := range held {
		key := semantics.IndexIdentityKey(name)
		byKey[key] = append(byKey[key], name)
	}
	for _, name := range declared {
		_, sensitive := pick(byKey[semantics.IndexIdentityKey(name)], func(name string) string { return name }, target)
		if sensitive != nil {
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

// spellCaseSensitiveNames returns current with each table, column, index and
// constraint name, and each column a key or an index lists, written as
// [sqlident.CatalogName] writes it. Every slice it changes is a copy, so the
// caller's read is left as it was. Schema names keep their stored spelling:
// the session's default schema is compared against them unquoted.
func spellCaseSensitiveNames(current catalog.Database, target string, semantics identifier.Semantics) catalog.Database {
	if semantics.UnquotedFold == "" {
		return current
	}
	spell := func(name string) string { return sqlident.CatalogName(target, name) }
	spellAll := func(names []string) []string {
		if names == nil {
			return nil
		}
		spelled := make([]string, len(names))
		for index, name := range names {
			spelled[index] = spell(name)
		}
		return spelled
	}
	spellPointer := func(name *string) *string {
		if name == nil {
			return nil
		}
		spelled := spell(*name)
		return &spelled
	}

	current.Tables = slices.Clone(current.Tables)
	for index := range current.Tables {
		table := &current.Tables[index]
		table.Name = spell(table.Name)
		table.Columns = slices.Clone(table.Columns)
		for column := range table.Columns {
			table.Columns[column].Name = spell(table.Columns[column].Name)
		}
	}
	current.Indexes = slices.Clone(current.Indexes)
	for index := range current.Indexes {
		held := &current.Indexes[index]
		held.Name = spell(held.Name)
		held.TableName = spell(held.TableName)
		held.Columns = spellAll(held.Columns)
		held.Parts = slices.Clone(held.Parts)
		for part := range held.Parts {
			if held.Parts[part].Name != "" {
				held.Parts[part].Name = spell(held.Parts[part].Name)
			}
		}
	}
	current.Constraints = slices.Clone(current.Constraints)
	for index := range current.Constraints {
		held := &current.Constraints[index]
		held.Name = spell(held.Name)
		held.TableName = spell(held.TableName)
		if held.ColumnName != "" {
			held.ColumnName = spell(held.ColumnName)
		}
		held.ColumnNames = spellAll(held.ColumnNames)
		held.ForeignTable = spellPointer(held.ForeignTable)
		held.ForeignColumn = spellPointer(held.ForeignColumn)
		held.ForeignColumns = spellAll(held.ForeignColumns)
		held.IncludeColumns = spellAll(held.IncludeColumns)
	}
	return current
}
