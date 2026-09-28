package sqlschema

import (
	"fmt"
	"strings"
)

// refuseUnsupportedForeignKey refuses an index drop that leaves a foreign key of
// the table with no index to check it against, as MySQL and MariaDB refuse it.
//
// The servers keep an index for every foreign key: one that begins with the
// key's columns, or the one they build when the table declares none. A drop
// that takes the last such index away is refused. Measured on MySQL 8.4.11 and
// MariaDB 11.8.9, `KEY ix (x), CONSTRAINT fk FOREIGN KEY (x) ...` followed by
// `DROP INDEX ix ON c` or `ALTER TABLE c DROP INDEX ix` answers `ERROR 1553:
// Cannot drop index 'ix': needed in a foreign key constraint`, and the same
// drop beside `KEY ix2 (x, id)` succeeds (stokaro/ptah#3913). Read as a drop,
// the file describes a table the server cannot hold, and the plan drops an
// index the server refuses to drop.
//
// The question is the one [alterTarget.buildKeyIndex] asks when a key is
// added: whether the table's primary key, a column's own key, a UNIQUE, an
// index it declares or an index the server built for a key begins with the
// key's columns. It runs after the index is out of the model.
func (t alterTarget) refuseUnsupportedForeignKey(dropped string) error {
	naming, ok := namingFor(t.sourcePlatform)
	if !ok {
		return nil
	}
	covered := t.declaredCoverage("")
	for _, built := range t.keys.onTable(t.qualified) {
		covered = append(covered, ascending(built.columns))
	}
	for _, key := range t.foreignKeys() {
		if covered.covers(key.columns, naming) {
			continue
		}
		return fmt.Errorf(
			"ALTER TABLE %s DROP INDEX %s: %s needs an index that begins with (%s), and no other index "+
				"of the table does; MySQL and MariaDB refuse the drop (ERROR 1553: needed in a foreign key constraint)",
			t.written, dropped, key.describe(), strings.Join(key.columns, ", "))
	}
	return nil
}

// tableForeignKey is one foreign key of a table, declared on the table or on
// a column.
type tableForeignKey struct {
	name    string
	columns []string
}

func (k tableForeignKey) describe() string {
	if k.name == "" {
		return "the foreign key on (" + strings.Join(k.columns, ", ") + ")"
	}
	return "foreign key " + k.name
}

// foreignKeys answers the table's foreign keys, table constraints first, then
// the keys its columns carry.
func (t alterTarget) foreignKeys() []tableForeignKey {
	var keys []tableForeignKey
	for _, database := range t.databases {
		for _, constraint := range database.Constraints {
			if isForeignKey(constraint) && constraint.StructName == t.structName {
				keys = append(keys, tableForeignKey{name: constraint.Name, columns: constraint.Columns})
			}
		}
	}
	for _, database := range t.databases {
		for _, field := range database.Fields {
			if field.StructName == t.structName && field.Foreign != "" {
				keys = append(keys, tableForeignKey{name: field.ForeignKeyName, columns: []string{field.Name}})
			}
		}
	}
	return keys
}
