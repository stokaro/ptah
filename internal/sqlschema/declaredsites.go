package sqlschema

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// elementSite is where the model keeps one CHECK or foreign key a CREATE TABLE
// body declared: on the field of the column that declares it, or among the
// constraints. Exactly one position is set.
type elementSite struct {
	field      int
	constraint int
}

// siteKind is one kind of element a CREATE TABLE body declares on a column or
// on the table, and where the model keeps its name.
type siteKind struct {
	// onColumn reports whether a column of the body declares one.
	onColumn func(*ast.ColumnNode) bool
	// onTable is the kind as a table-level constraint of the body.
	onTable ast.ConstraintType
	// modeled reports whether a constraint of the model is one.
	modeled func(schemamodel.Constraint) bool
	// fieldName is the name of the one a field holds.
	fieldName func(*schemamodel.Field) *string
}

// checkSites is the kind of a CHECK.
func checkSites() siteKind {
	return siteKind{
		onColumn:  func(column *ast.ColumnNode) bool { return column.Check != "" },
		onTable:   ast.CheckConstraint,
		modeled:   isCheck,
		fieldName: func(field *schemamodel.Field) *string { return &field.CheckName },
	}
}

// foreignKeySites is the kind of a foreign key: a column's `REFERENCES`, or a
// table-level FOREIGN KEY.
func foreignKeySites() siteKind {
	return siteKind{
		onColumn:  func(column *ast.ColumnNode) bool { return column.ForeignKey != nil },
		onTable:   ast.ForeignKeyConstraint,
		modeled:   isForeignKey,
		fieldName: func(field *schemamodel.Field) *string { return &field.ForeignKeyName },
	}
}

// name answers the name the element at site carries, to read or assign.
func (k siteKind) name(database *schemamodel.Database, site elementSite) *string {
	if site.field != noPosition {
		return k.fieldName(&database.Fields[site.field])
	}
	return &database.Constraints[site.constraint].Name
}

// declaredSites lists the elements of kind that one CREATE TABLE declared, in
// the order the statement wrote them: on a column, or on the table.
//
// The server numbers what the author left unnamed in that order, the columns'
// elements among the table's: measured on MySQL 26.7.0 and MariaDB 11.8.9,
// `a INT REFERENCES p(id), b INT, FOREIGN KEY (b) ..., d INT REFERENCES p(id)`
// numbers the keys of a, b and d 1, 2 and 3, and a table-level key written
// before the columns takes 1.
//
// The k-th element of kind among the constraints the table appended is the
// k-th such constraint the body recorded, because every CHECK and every
// foreign key converts and the constraints were appended in the body's order.
// A node that did not record where each column sits -- one a fluent builder
// assembled from assigned slices -- gives no position to a column's element,
// and its elements are listed with the ones on columns first, which is the
// order a table written column by column declares them in.
func declaredSites(
	database *schemamodel.Database, node *ast.CreateTableNode, fieldsStart, constraintsStart int, kind siteKind,
) []elementSite {
	var onTable []int
	for i := constraintsStart; i < len(database.Constraints); i++ {
		if kind.modeled(database.Constraints[i]) {
			onTable = append(onTable, i)
		}
	}
	sites := make([]elementSite, 0, len(node.Columns)+len(onTable))
	if !ordersColumnsAndConstraints(node) {
		for i, column := range node.Columns {
			if kind.onColumn(column) {
				sites = append(sites, elementSite{field: fieldsStart + i, constraint: noPosition})
			}
		}
		for _, position := range onTable {
			sites = append(sites, elementSite{field: noPosition, constraint: position})
		}
		return sites
	}
	next := 0
	for _, element := range node.Elements {
		switch {
		case element.Column != nil && kind.onColumn(element.Column):
			sites = append(sites, elementSite{
				field: fieldsStart + slices.Index(node.Columns, element.Column), constraint: noPosition})
		case element.Constraint != nil && element.Constraint.Type == kind.onTable:
			sites = append(sites, elementSite{field: noPosition, constraint: onTable[next]})
			next++
		}
	}
	return sites
}

// ordersColumnsAndConstraints reports whether the body recorded where every
// column and every constraint sits.
func ordersColumnsAndConstraints(node *ast.CreateTableNode) bool {
	columns := 0
	for _, element := range node.Elements {
		if element.Column != nil {
			columns++
		}
	}
	return columns == len(node.Columns) && ordersEverything(node)
}
