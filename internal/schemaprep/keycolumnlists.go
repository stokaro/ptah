package schemaprep

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// ErrSecondKeyOverColumns is the refusal of a table that declares two primary
// or unique keys over one ordered column list, on a server that holds one
// ([descriptor.Dialect.SecondKeyRefusal]).
var ErrSecondKeyOverColumns = errors.New("two keys claim the same columns")

// ValidateKeyColumnLists refuses a database in which a table declares two
// primary or unique keys over one ordered column list, where the server of
// dialect holds one key per list. The keys are the table's primary key, each
// column declared unique by itself, and each UNIQUE or PRIMARY KEY
// constraint. The error wraps [ErrSecondKeyOverColumns] and ends with the
// error the server answers. A dialect whose server takes a second key, and a
// nil database, pass.
func ValidateKeyColumnLists(database *schemamodel.Database, dialect string) error {
	described, found := platform.Lookup(dialect)
	if database == nil || !found || described.SecondKeyRefusal == "" {
		return nil
	}
	semantics := identifier.ForDialect(dialect)
	for _, table := range database.Tables {
		held := make(map[string]string)
		for _, key := range declaredKeys(database, table) {
			columns := make([]string, len(key.columns))
			for i, column := range key.columns {
				columns[i] = semantics.ColumnIdentityKey(column)
			}
			list := strings.Join(columns, "\x00")
			if first, taken := held[list]; taken {
				return fmt.Errorf("%w: table %q declares %s and %s, both over (%s): %s",
					ErrSecondKeyOverColumns, table.Name, first, key.label, strings.Join(key.columns, ", "), described.SecondKeyRefusal)
			}
			held[list] = key.label
		}
	}
	return nil
}

// declaredKey is one key a table declares, with what a refusal calls it.
type declaredKey struct {
	label   string
	columns []string
}

// declaredKeys lists the keys table declares, in the order a render creates
// them: the primary key, the columns declared unique, then the constraints. A
// PRIMARY KEY constraint over the primary key's own columns, in their order, is
// that key written twice, which the model reads as one (see
// [FoldedIndexConstraints]), so it is not listed again.
func declaredKeys(database *schemamodel.Database, table schemamodel.Table) []declaredKey {
	var keys []declaredKey
	primary := slices.Clone(table.PrimaryKey)
	var unique []string
	for _, field := range database.Fields {
		if field.StructName != table.StructName {
			continue
		}
		if field.Primary && len(table.PrimaryKey) == 0 {
			primary = append(primary, field.Name)
		}
		if field.Unique {
			unique = append(unique, field.Name)
		}
	}
	if len(primary) > 0 {
		keys = append(keys, declaredKey{label: "the primary key", columns: primary})
	}
	for _, column := range unique {
		keys = append(keys, declaredKey{label: fmt.Sprintf("column %q as unique", column), columns: []string{column}})
	}
	for _, constraint := range database.Constraints {
		primaryKey := strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY")
		if !ConstraintBelongsToTable(constraint, table) ||
			(!primaryKey && !strings.EqualFold(strings.TrimSpace(constraint.Type), "UNIQUE")) ||
			(primaryKey && slices.Equal(constraint.Columns, primary)) {
			continue
		}
		label := strings.ToUpper(constraint.Type)
		if constraint.Name != "" {
			label = fmt.Sprintf("%s %q", label, constraint.Name)
		}
		keys = append(keys, declaredKey{label: label, columns: constraint.Columns})
	}
	return keys
}
