package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// columnOwnedUniques answers the database UNIQUE constraints that a column's
// own UNIQUE accounts for: for each column the desired state declares unique,
// one UNIQUE over that column alone, and no other.
//
// The column's lifecycle creates and drops that one, so the comparison leaves
// it out; every other UNIQUE of the table stays in it and is paired by name.
// Three conditions keep the rest in:
//
//   - A UNIQUE over more than the column is a key of its own. Measured on
//     MySQL 8.4.11 and 26.7.0, MariaDB 11.8.9 and 12.3.3 and PostgreSQL 18,
//     `a int UNIQUE, b int, UNIQUE (a, b)` builds two keys. Read as the
//     column's, the second is planned as an ADD against the database the file
//     built, and with the key removed from the file it is never dropped
//     (stokaro/ptah#3764).
//   - A UNIQUE the desired state declares by name is that declaration's. So
//     `a int UNIQUE, CONSTRAINT uq_a UNIQUE (a)` pairs `uq_a` with its
//     declaration and leaves the column the other key.
//   - A column accounts for one key. A second UNIQUE over the column alone,
//     which the file does not declare, is planned for removal, as Atlas CE
//     v1.3.0 plans it: `ALTER TABLE c DROP INDEX a_2` on MySQL and MariaDB,
//     `DROP CONSTRAINT c_a_key1` on PostgreSQL.
//
// Which key a column accounts for, where several qualify, is the one named as
// the server names a column's own UNIQUE: after the column on MySQL and
// MariaDB, `<table>_<column>_key` on PostgreSQL. Failing that, the first by
// name, so the answer does not depend on the catalog's order.
func columnOwnedUniques(
	desired *schemamodel.Database,
	database *catalog.Database,
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	semantics identifier.Semantics,
) map[tableMemberKey]struct{} {
	uniqueColumns := desiredUniqueColumns(desired, semantics)
	candidates := make(map[tableMemberKey][]catalog.Constraint)
	for _, constraint := range database.Constraints {
		columns := constraint.ColumnNamesOrDefault()
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(columns) != 1 {
			continue
		}
		column := newTableMemberKey(constraint.QualifiedTableName(), columns[0], semantics)
		if !uniqueColumns[column] {
			continue
		}
		if _, declared := genConstraints[newCatalogConstraintKey(constraint, semantics)]; declared {
			continue
		}
		candidates[column] = append(candidates[column], constraint)
	}
	owned := make(map[tableMemberKey]struct{}, len(candidates))
	for column, constraints := range candidates {
		owner := slices.MinFunc(constraints, func(a, b catalog.Constraint) int {
			return cmp.Or(
				cmp.Compare(columnUniqueNameRank(a, column.member), columnUniqueNameRank(b, column.member)),
				cmp.Compare(a.Name, b.Name),
			)
		})
		owned[newCatalogConstraintKey(owner, semantics)] = struct{}{}
	}
	return owned
}

// desiredUniqueColumns answers the columns the desired state declares UNIQUE,
// keyed by table and column.
func desiredUniqueColumns(desired *schemamodel.Database, semantics identifier.Semantics) map[tableMemberKey]bool {
	tables := make(map[string]string, len(desired.Tables))
	for _, table := range desired.Tables {
		if _, seen := tables[table.StructName]; !seen {
			tables[table.StructName] = table.QualifiedName()
		}
	}
	columns := make(map[tableMemberKey]bool)
	for _, field := range desired.Fields {
		if !field.Unique {
			continue
		}
		table := field.StructName
		if qualified, ok := tables[field.StructName]; ok {
			table = qualified
		}
		columns[newTableMemberKey(table, field.Name, semantics)] = true
	}
	return columns
}

// columnUniqueNameRank is 0 for a name a server gives a column's own UNIQUE and
// 1 for any other. member is the column as the comparison keys it.
func columnUniqueNameRank(constraint catalog.Constraint, member string) int {
	column := constraint.ColumnNamesOrDefault()[0]
	table := constraint.TableName
	if ref, ok := tableref.Parse(constraint.QualifiedTableName()); ok {
		table = ref.Name
	}
	if strings.EqualFold(constraint.Name, column) || strings.EqualFold(constraint.Name, member) ||
		constraint.Name == table+"_"+column+"_key" {
		return 0
	}
	return 1
}
