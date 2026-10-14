package builtin

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/autoincrement"
	"ptah.run/internal/schemaprep"
)

// validateDeclaredSerialColumns refuses a serial column the target cannot
// write as its auto-increment form.
//
// MySQL, MariaDB and SQL Server write a serial type as an auto-increment
// integer ([schemaprep.SerialWritesAutoIncrement]), and each server
// refuses some tables of them. A table holds at most one auto-increment
// column on all three: MySQL and MariaDB answer a second with ERROR 1075, and
// SQL Server answers a second IDENTITY with Msg 2744. MySQL and MariaDB also
// refuse an AUTO_INCREMENT column that no key starts with, ERROR 1075 again,
// measured on MySQL 26.7 and MariaDB 12.3 for both
// `CREATE TABLE z (id INT PRIMARY KEY, seq INT AUTO_INCREMENT)` and, on MySQL,
// `PRIMARY KEY (a, seq)`. So such a SERIAL is refused by name before any
// statement exists, rather than failing at apply.
//
// Only a column the serial type makes auto-increment is held to this. A
// column declared auto-increment outright keeps the target's own answer.
func validateDeclaredSerialColumns(dialect string, database *schemamodel.Database) error {
	normalized := platform.NormalizeDialect(dialect)
	if normalized != platform.MySQL && normalized != platform.MariaDB && normalized != platform.SQLServer {
		return nil
	}
	owners := schemamodel.ResolveIndexTableNames(database.Indexes, database.Tables)
	for _, table := range database.Tables {
		var fields []schemamodel.Field
		for _, field := range database.Fields {
			if field.StructName == table.StructName {
				fields = append(fields, field)
			}
		}
		if err := validateTableSerialColumns(normalized, table, fields, database, owners); err != nil {
			return err
		}
	}
	return nil
}

func validateTableSerialColumns(
	dialect string,
	table schemamodel.Table,
	fields []schemamodel.Field,
	database *schemamodel.Database,
	indexOwners []string,
) error {
	var automatic []string
	serial := ""
	for _, field := range fields {
		if !autoincrement.DeclaredOn(field, dialect) {
			continue
		}
		automatic = append(automatic, field.Name)
		if serial == "" && schemaprep.SerialWritesAutoIncrement(field, dialect) && !autoincrement.Declared(field) {
			serial = field.Name
		}
	}
	if serial == "" {
		return nil
	}
	if len(automatic) > 1 {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("table %s declares %d auto-increment columns (%s); a serial type is written as one here, "+
				"and %s allows a table one: declare the others as plain integers",
				table.Name, len(automatic), strings.Join(automatic, ", "), dialect),
		}
	}
	if dialect == platform.SQLServer || keyLeadingColumns(table, fields, database, indexOwners)[strings.ToLower(serial)] {
		return nil
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("column %s.%s is a serial type, which %s writes as AUTO_INCREMENT, and no key of the table "+
			"starts with it; %s refuses an AUTO_INCREMENT column that is not the first column of a key: make it the "+
			"primary key, or declare a unique or plain index that starts with it", table.Name, serial, dialect, dialect),
	}
}

// keyLeadingColumns names, in lower case, the columns some key of table
// starts with: its primary key, a unique column, a PRIMARY KEY or UNIQUE
// constraint, or an index.
func keyLeadingColumns(
	table schemamodel.Table,
	fields []schemamodel.Field,
	database *schemamodel.Database,
	indexOwners []string,
) map[string]bool {
	leading := make(map[string]bool)
	if key := tablePrimaryKeyColumns(table, fields); len(key) > 0 {
		leading[strings.ToLower(key[0])] = true
	}
	for _, field := range fields {
		if field.Unique {
			leading[strings.ToLower(field.Name)] = true
		}
	}
	for _, constraint := range database.Constraints {
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner == nil || owner.QualifiedName() != table.QualifiedName() || len(constraint.Columns) == 0 {
			continue
		}
		if strings.EqualFold(constraint.Type, "PRIMARY KEY") || strings.EqualFold(constraint.Type, "UNIQUE") {
			leading[strings.ToLower(constraint.Columns[0])] = true
		}
	}
	for position, index := range database.Indexes {
		if indexOwners[position] != table.QualifiedName() {
			continue
		}
		if parts := schemaprep.IndexKeyParts(index); len(parts) > 0 {
			leading[strings.ToLower(strings.TrimSpace(parts[0]))] = true
		}
	}
	return leading
}

// tablePrimaryKeyColumns lists a table's primary key columns in key order: the
// parts the table names, or the columns it names, or else its fields marked
// primary, in declaration order.
func tablePrimaryKeyColumns(table schemamodel.Table, fields []schemamodel.Field) []string {
	var columns []string
	for _, part := range table.PrimaryKeyParts {
		if name := strings.TrimSpace(part.Name); name != "" {
			columns = append(columns, name)
		}
	}
	if len(columns) > 0 {
		return columns
	}
	if len(table.PrimaryKey) > 0 {
		return table.PrimaryKey
	}
	for _, field := range fields {
		if field.Primary {
			columns = append(columns, field.Name)
		}
	}
	return columns
}
