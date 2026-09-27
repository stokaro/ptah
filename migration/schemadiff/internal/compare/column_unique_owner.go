package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/tableref"
)

// columnKeys is what the comparison reads about the key each column the desired
// state declares UNIQUE owns in the database.
type columnKeys struct {
	// owned are the database UNIQUE constraints the columns account for. The
	// constraint comparison leaves them out.
	owned map[tableMemberKey]struct{}
	// held are the columns, keyed by table and column, whose own key the
	// database holds.
	held map[tableMemberKey]struct{}
}

// readColumnKeys answers which database UNIQUE constraints the columns the
// desired state declares UNIQUE account for: for each such column, one UNIQUE
// over that column alone, and no other.
//
// The column's lifecycle creates and drops that one, so the constraint
// comparison leaves it out; every other UNIQUE of the table stays in it and is
// paired by name. Three conditions keep the rest in:
//
//   - A UNIQUE over more than the column is a key of its own. Measured on
//     MySQL 8.4.11 and 26.7.0, MariaDB 11.8.9 and 12.3.3 and PostgreSQL 18,
//     `a int UNIQUE, b int, UNIQUE (a, b)` builds two keys. Read as the
//     column's, the second is planned as an ADD against the database the file
//     built, and with the key removed from the file it is never dropped
//     (stokaro/ptah#3764).
//   - A UNIQUE the desired state declares by name, as a constraint or as a
//     unique index, is that declaration's. So `a int UNIQUE, CONSTRAINT uq_a
//     UNIQUE (a)` pairs `uq_a` with its declaration and leaves the column the
//     other key, and a database that holds only `uq_a` does not hold the
//     column's (stokaro/ptah#3784).
//   - A column accounts for one key. A second UNIQUE over the column alone,
//     which the file does not declare, is planned for removal, as Atlas CE
//     v1.3.0 plans it: `ALTER TABLE c DROP INDEX a_2` on MySQL and MariaDB,
//     `DROP CONSTRAINT c_a_key1` on PostgreSQL.
//
// Which key a column accounts for is the one named as the server names a
// column's own UNIQUE, on an engine whose rule is measured; see
// [columnkey.Name]. The name the server tries first gives way to a name the
// desired state holds; see [columnkey.Taken]. A key over the column alone under
// another name is not the column's: it is planned for removal, and the
// column's key is added under the server's name, as Atlas CE v1.3.0 plans it
// (stokaro/ptah#3723). On the other engines it is the key named after the
// column where there is one, and the first by name otherwise, so the answer
// does not depend on the catalog's order.
//
// The constraint comparison reads owned, and the column comparison reads held;
// one function answers both, so the two cannot disagree about which key is the
// column's.
func readColumnKeys(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) columnKeys {
	declared := declaredAndCheckConstraints(desired, database, dialect, semantics)
	declaredIndexes := generatedIndexIdentities(desired, semantics)
	uniqueColumns := desiredUniqueColumns(desired, semantics)
	named := columnkey.Named(dialect)
	candidates := make(map[tableMemberKey][]catalog.Constraint)
	for _, constraint := range database.Constraints {
		columns := constraint.ColumnNamesOrDefault()
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(columns) != 1 {
			continue
		}
		column := newTableMemberKey(constraint.QualifiedTableName(), columns[0], semantics)
		if _, unique := uniqueColumns[column]; !unique {
			continue
		}
		if _, byName := declared[newCatalogConstraintKey(constraint, semantics)]; byName ||
			uniqueConstraintOwnedByDeclaredIndex(constraint, dialect, declaredIndexes, semantics) {
			continue
		}
		candidates[column] = append(candidates[column], constraint)
	}
	keys := columnKeys{
		owned: make(map[tableMemberKey]struct{}, len(candidates)),
		held:  make(map[tableMemberKey]struct{}, len(candidates)),
	}
	for column, constraints := range candidates {
		owner := slices.MinFunc(constraints, func(a, b catalog.Constraint) int {
			return cmp.Or(
				cmp.Compare(columnUniqueNameRank(a, column.member), columnUniqueNameRank(b, column.member)),
				cmp.Compare(a.Name, b.Name),
			)
		})
		if named {
			own := uniqueColumns[column]
			name, _ := ownKeyName(desired, own.table, own.column, dialect)
			found := slices.IndexFunc(constraints, func(constraint catalog.Constraint) bool {
				return columnkey.Same(dialect, constraint.Name, name)
			})
			if found < 0 {
				continue
			}
			owner = constraints[found]
		}
		keys.owned[newCatalogConstraintKey(owner, semantics)] = struct{}{}
		keys.held[column] = struct{}{}
	}
	return keys
}

// ownKeyName answers the name the server of dialect gives the own UNIQUE of
// column on table, and false on an engine whose naming [columnkey.Named] does
// not measure. The name gives way to the names the desired state holds, by
// [columnkey.Taken], the rule the SQL reader follows too.
//
// The comparison pairs the column's key with the database key of this name,
// and a planner adds the key under it; see
// [difftypes.TableDiff.ColumnKeyNames]. One function answers both, so a plan
// adds the key the next comparison reads as the column's.
func ownKeyName(desired *schemamodel.Database, table schemamodel.Table, column, dialect string) (string, bool) {
	taken := columnkey.Taken(dialect, []*schemamodel.Database{desired}, table)
	return columnkey.Name(dialect, table.Name, column, taken)
}

// uniqueColumn is a column the desired state declares UNIQUE, with its table.
type uniqueColumn struct {
	table  schemamodel.Table
	column string
}

// desiredUniqueColumns answers the columns the desired state declares UNIQUE,
// keyed by table and column.
func desiredUniqueColumns(desired *schemamodel.Database, semantics identifier.Semantics) map[tableMemberKey]uniqueColumn {
	tables := make(map[string]schemamodel.Table, len(desired.Tables))
	for _, table := range desired.Tables {
		if _, seen := tables[table.StructName]; !seen {
			tables[table.StructName] = table
		}
	}
	columns := make(map[tableMemberKey]uniqueColumn)
	for _, field := range desired.Fields {
		if !field.Unique {
			continue
		}
		table, ok := tables[field.StructName]
		if !ok {
			table = schemamodel.Table{StructName: field.StructName, Name: field.StructName}
		}
		columns[newTableMemberKey(table.QualifiedName(), field.Name, semantics)] = uniqueColumn{table: table, column: field.Name}
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
