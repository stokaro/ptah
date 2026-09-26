package sqlschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlname"
)

// ErrDuplicateCheckName is the class of a CHECK the server would name after
// another CHECK already holds the name.
//
// Neither engine moves a derived name out of the way. Measured on MySQL 8.4.11
// and 26.7.0, an unnamed CHECK of `f2` beside `CONSTRAINT f2_chk_1 CHECK (...)`,
// written before or after it, in either case, or on another table of the
// database, is `ERROR 3822 (HY000): Duplicate check constraint name`. Measured
// on MariaDB 11.8.9, `a int CHECK (a > 0), CONSTRAINT a CHECK (a < 9)` is
// `ERROR 1826 (HY000): Duplicate CHECK constraint name 'a'`. A model holding
// both would carry two CHECKs under one name, and the document is refused as
// the server refuses it.
var ErrDuplicateCheckName = errors.New("two CHECK constraints claim the same name")

// ErrCheckNameTooLong is the class of a CHECK name MySQL derives past the
// longest identifier it keeps. See [mysqlname.MaxIdentifierChars].
var ErrCheckNameTooLong = errors.New("the derived CHECK name is longer than the engine accepts")

// checkSite is where the model keeps one CHECK: on a field, or among the
// constraints. Exactly one position is set.
type checkSite struct {
	field      int
	constraint int
}

// name answers the name the CHECK at the site carries, to read or assign.
func (s checkSite) name(database *schemamodel.Database) *string {
	if s.field != noPosition {
		return &database.Fields[s.field].CheckName
	}
	return &database.Constraints[s.constraint].Name
}

// declaredChecks lists the CHECKs of the table one CREATE TABLE declared, in
// the order the statement wrote them: on a column, or on the table.
//
// The k-th CHECK among the constraints the table appended is the k-th CHECK
// element the body recorded, because every CHECK converts and the constraints
// were appended in the body's order. A node that did not record where each
// column sits -- one a fluent builder assembled from assigned slices -- gives
// no position to a column's CHECK, and its CHECKs are listed with the ones on
// columns first, which is the order a table written column by column declares
// them in.
func declaredChecks(
	database *schemamodel.Database, node *ast.CreateTableNode, fieldsStart, constraintsStart int,
) []checkSite {
	var tableChecks []int
	for i := constraintsStart; i < len(database.Constraints); i++ {
		if isCheck(database.Constraints[i]) {
			tableChecks = append(tableChecks, i)
		}
	}
	if !ordersColumnsAndConstraints(node) {
		sites := make([]checkSite, 0, len(node.Columns)+len(tableChecks))
		for i, column := range node.Columns {
			if column.Check != "" {
				sites = append(sites, checkSite{field: fieldsStart + i, constraint: noPosition})
			}
		}
		for _, position := range tableChecks {
			sites = append(sites, checkSite{field: noPosition, constraint: position})
		}
		return sites
	}
	sites := make([]checkSite, 0, len(node.Columns)+len(tableChecks))
	next := 0
	for _, element := range node.Elements {
		switch {
		case element.Column != nil && element.Column.Check != "":
			sites = append(sites, checkSite{
				field: fieldsStart + slices.Index(node.Columns, element.Column), constraint: noPosition})
		case element.Constraint != nil && element.Constraint.Type == ast.CheckConstraint:
			sites = append(sites, checkSite{field: noPosition, constraint: tableChecks[next]})
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

// namesColumnChecksAfterColumns reports whether the source dialect names a
// CHECK written on a column after the column: MariaDB does, measured on
// 11.8.9, and keeps it apart from the CHECKs written on the table.
func namesColumnChecksAfterColumns(sourcePlatform string) bool {
	return platform.NormalizeDialect(sourcePlatform) == platform.MariaDB
}

// isCheck reports whether a constraint of the model is a CHECK.
func isCheck(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "CHECK")
}

// nameCreatedMySQLFamilyChecks gives every unnamed CHECK one CREATE TABLE
// declared the name its engine gives it: `<table>_chk_<n>` on MySQL and, on
// MariaDB, the column's name for a CHECK on a column and `CONSTRAINT_<n>` for
// one on the table. See [mysqlname.Check] and [mysqlname.MariaDBCheck].
//
// The name has to be decided on the desired model, for the reason
// [nameCreatedConstraints] gives. Left unnamed, the CHECK never pairs with the
// catalog's: the comparison drops the server's CHECK and adds the same one
// back (stokaro/ptah#3741).
func nameCreatedMySQLFamilyChecks(
	database, base *schemamodel.Database, table schemamodel.Table, checks []checkSite, sourcePlatform string,
) error {
	databases := []*schemamodel.Database{database, base}
	switch platform.NormalizeDialect(sourcePlatform) {
	case platform.MySQL:
		n := uint32(1)
		for _, site := range checks {
			name := site.name(database)
			if *name != "" {
				continue
			}
			derived := mysqlname.Check(table.Name, n)
			n++
			if err := refuseMySQLCheckName(databases, table, derived); err != nil {
				return err
			}
			*name = derived
		}
	case platform.MariaDB:
		for _, site := range checks {
			if site.field != noPosition && database.Fields[site.field].CheckName == "" {
				database.Fields[site.field].CheckName = database.Fields[site.field].Name
			}
		}
		if err := refuseMariaDBColumnCheckName(databases, table); err != nil {
			return err
		}
		for _, site := range checks {
			name := site.name(database)
			if *name == "" {
				*name = mysqlname.MariaDBCheck(mariaDBCheckNameTaken(databases, table))
			}
		}
	}
	return nil
}

// nameAddedMySQLFamilyCheck names an unnamed CHECK an ALTER TABLE adds to the
// table, on the table or on a column it adds, in the statement's sequence: on
// MySQL `<table>_chk_<n>` from the number [alterStatement] read, see
// [mysqlname.NextCheckNumber]; on MariaDB the column's name for a column's
// CHECK and the first `CONSTRAINT_<n>` the statement's names leave free for the
// table's. column is the added column, or nil for a CHECK on the table.
func nameAddedMySQLFamilyCheck(name *string, column *schemamodel.Field, target alterTarget) error {
	if *name != "" || target.table == nil || target.statement == nil {
		return nil
	}
	table, statement := *target.table, target.statement
	switch platform.NormalizeDialect(target.sourcePlatform) {
	case platform.MySQL:
		derived := mysqlname.Check(table.Name, statement.nextCheck)
		statement.nextCheck++
		// First: the CHECK the later drop takes still holds the name in the
		// model, which would read as a duplicate rather than as the order.
		if err := refuseCheckNameDroppedLater(target, derived); err != nil {
			return err
		}
		if err := refuseMySQLCheckName(target.databases, table, derived); err != nil {
			return err
		}
		if slices.ContainsFunc(statement.checkNames, func(held string) bool { return strings.EqualFold(held, derived) }) {
			return duplicateMySQLCheckName(derived, table)
		}
		*name = derived
	case platform.MariaDB:
		if column != nil {
			*name = column.Name
			return nil
		}
		derived := mysqlname.MariaDBCheck(func(candidate string) bool {
			return slices.ContainsFunc(statement.checkNames, func(held string) bool { return strings.EqualFold(held, candidate) })
		})
		if err := refuseCheckNameDroppedLater(target, derived); err != nil {
			return err
		}
		statement.checkNames = append(statement.checkNames, derived)
		*name = derived
	}
	return nil
}

// refuseCheckNameDroppedLater refuses an unnamed CHECK whose derived name an
// operation later in the same statement drops.
//
// The server makes a statement's drops before it names what the statement adds,
// whichever the statement writes first: measured on MySQL 8.4.11 and 26.7.0,
// `ALTER TABLE s5 ADD CHECK (a > 2), DROP CONSTRAINT s5_chk_3` names the added
// CHECK `s5_chk_1`, and on MariaDB 11.8.9 the same statement dropping
// `CONSTRAINT_1` names it `CONSTRAINT_1`. The model applies the operations in
// the order written, so the drop would reach the CHECK just added under the
// dropped name, and the document would lose a CHECK the server keeps.
func refuseCheckNameDroppedLater(target alterTarget, derived string) error {
	if !slices.ContainsFunc(target.statement.laterDrops, func(drop string) bool { return strings.EqualFold(drop, derived) }) {
		return nil
	}
	return fmt.Errorf("%w: ALTER TABLE %s adds an unnamed CHECK the server names %s, and drops %s later in "+
		"the same statement; the server drops first, and the reader applies the operations in the order "+
		"written, so write the DROP first", ErrUnmodeledStatement, target.written, derived, derived)
}

// droppedConstraintNames lists the constraint names the operations drop.
func droppedConstraintNames(operations []ast.AlterOperation, sourcePlatform string) []string {
	var dropped []string
	for _, operation := range operations {
		if drop, ok := operation.(*ast.DropConstraintOperation); ok && drop.ConstraintName != "" {
			dropped = append(dropped, normalizeSQLIdentifier(sourcePlatform, drop.ConstraintName))
		}
	}
	return dropped
}

// remainingCheckNames is held less the CHECKs the statement's operations drop
// by name. The server makes a statement's drops before it names what the
// statement adds, whichever the statement writes first.
func remainingCheckNames(held []string, operations []ast.AlterOperation, sourcePlatform string) []string {
	dropped := droppedConstraintNames(operations, sourcePlatform)
	return slices.DeleteFunc(slices.Clone(held), func(name string) bool {
		return slices.ContainsFunc(dropped, func(drop string) bool { return strings.EqualFold(drop, name) })
	})
}

// addedCheckNames lists the names the statement's operations write for the
// CHECKs they add: a CHECK added under a name, and the column a column's CHECK
// is added on, which MariaDB names the CHECK after.
func addedCheckNames(operations []ast.AlterOperation, sourcePlatform string) []string {
	var names []string
	for _, operation := range operations {
		switch typed := operation.(type) {
		case *ast.AddConstraintOperation:
			if typed.Constraint != nil && typed.Constraint.Type == ast.CheckConstraint && typed.Constraint.Name != "" {
				names = append(names, normalizeSQLIdentifier(sourcePlatform, typed.Constraint.Name))
			}
		case *ast.AddColumnOperation:
			if typed.Column == nil || typed.Column.Check == "" {
				continue
			}
			if name := addedColumnCheckName(typed.Column, sourcePlatform); name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

// addedColumnCheckName is the name a column's CHECK carries when the column is
// added: the one written, or on MariaDB the column's own.
func addedColumnCheckName(column *ast.ColumnNode, sourcePlatform string) string {
	if column.CheckName != "" || !namesColumnChecksAfterColumns(sourcePlatform) {
		return normalizeSQLIdentifier(sourcePlatform, column.CheckName)
	}
	return normalizeSQLIdentifier(sourcePlatform, column.Name)
}

// tableCheckNames lists the names of the CHECKs a table holds, on its columns
// and on the table, in this file and in the earlier ones.
func tableCheckNames(databases []*schemamodel.Database, table schemamodel.Table) []string {
	var names []string
	for _, database := range databases {
		if database == nil {
			continue
		}
		for _, field := range database.Fields {
			if field.StructName == table.StructName && field.Check != "" && field.CheckName != "" {
				names = append(names, field.CheckName)
			}
		}
		for _, constraint := range database.Constraints {
			if isCheck(constraint) && constraint.Table == table.QualifiedName() && constraint.Name != "" {
				names = append(names, constraint.Name)
			}
		}
	}
	return names
}

// mariaDBCheckNameTaken answers, without case, whether a CHECK of the table
// already holds a name. MariaDB keeps CHECK names per table.
func mariaDBCheckNameTaken(databases []*schemamodel.Database, table schemamodel.Table) func(string) bool {
	return func(name string) bool {
		return slices.ContainsFunc(tableCheckNames(databases, table), func(held string) bool {
			return strings.EqualFold(held, name)
		})
	}
}

// refuseMariaDBColumnCheckName refuses a table-level CHECK written under the
// name of a column that carries a CHECK: MariaDB names the column's CHECK after
// the column, and answers ERROR 1826 to the second name.
func refuseMariaDBColumnCheckName(databases []*schemamodel.Database, table schemamodel.Table) error {
	for _, database := range databases {
		if database == nil {
			continue
		}
		for _, field := range database.Fields {
			if field.StructName != table.StructName || field.Check == "" {
				continue
			}
			for _, constraint := range database.Constraints {
				if isCheck(constraint) && constraint.Table == table.QualifiedName() &&
					strings.EqualFold(constraint.Name, field.CheckName) {
					return fmt.Errorf("%w: %s names a CHECK of %s, and so does the CHECK on column %s, "+
						"which MariaDB names after the column; MariaDB answers ERROR 1826",
						ErrDuplicateCheckName, constraint.Name, table.Name, field.Name)
				}
			}
		}
	}
	return nil
}

// refuseMySQLCheckName refuses a name MySQL derives that it would refuse: one
// another CHECK of the same database already holds, compared without case, and
// one longer than the longest identifier MySQL keeps.
func refuseMySQLCheckName(databases []*schemamodel.Database, table schemamodel.Table, name string) error {
	if utf8.RuneCountInString(name) > mysqlname.MaxIdentifierChars {
		return fmt.Errorf("%w: %s, the name MySQL gives an unnamed CHECK of %s, is longer than %d "+
			"characters, and MySQL answers ERROR 1059",
			ErrCheckNameTooLong, name, table.Name, mysqlname.MaxIdentifierChars)
	}
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, table.Schema)
		for _, field := range database.Fields {
			if inSchema[field.StructName] && field.Check != "" && strings.EqualFold(field.CheckName, name) {
				return duplicateMySQLCheckName(field.CheckName, table)
			}
		}
		for _, constraint := range database.Constraints {
			if !isCheck(constraint) || !strings.EqualFold(constraint.Name, name) {
				continue
			}
			if heldSchema, _ := splitQualifiedTable(constraint.Table); heldSchema == table.Schema {
				return duplicateMySQLCheckName(constraint.Name, table)
			}
		}
	}
	return nil
}

func duplicateMySQLCheckName(held string, table schemamodel.Table) error {
	return fmt.Errorf("%w: %s, the name MySQL gives an unnamed CHECK of %s, is held by another "+
		"CHECK, and MySQL answers ERROR 3822", ErrDuplicateCheckName, held, table.Name)
}
