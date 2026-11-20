package keyrelease

import (
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/descriptortable"
)

// secondKeyRefusals holds each descriptor's SecondKeyRefusal.
var secondKeyRefusals = descriptortable.New(func(described descriptor.Dialect) string {
	return described.SecondKeyRefusal
})

// UnderColumnKeys answers, on a server that holds one primary or unique key
// per ordered column list ([descriptor.Dialect.SecondKeyRefusal]), the removed
// keys over a column alone whose column gains its own UNIQUE in the column
// change. Oracle answers `MODIFY (a ... UNIQUE)` with ORA-02261 while a key
// over (a) stands, so the removal runs ahead of the column changes. The
// rollback of a named primary key that replaced the key Oracle named for a
// column has this shape. A removal needs its columns, which the comparison
// and the reversal carry.
func UnderColumnKeys(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil || secondKeyRefusals.Get(dialect) == "" {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	gains := make(map[string]bool)
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			before, after, changed := strings.Cut(column.Changes["unique"], " -> ")
			if changed && strings.TrimSpace(before) == "false" && strings.TrimSpace(after) == "true" {
				gains[semantics.QualifiedTableIdentityKey(table.TableName)+"\x00"+semantics.ColumnIdentityKey(column.ColumnName)] = true
			}
		}
	}
	var released Releases
	for _, info := range diff.ConstraintsRemoved {
		isKey := strings.EqualFold(info.Type, "PRIMARY KEY") || strings.EqualFold(info.Type, "UNIQUE")
		if !isKey || len(info.Columns) != 1 ||
			!gains[semantics.QualifiedTableIdentityKey(info.TableName)+"\x00"+semantics.ColumnIdentityKey(info.Columns[0])] {
			continue
		}
		if info.Identity == (difftypes.ConstraintIdentity{}) {
			info.Identity = constraintscope.Identity(semantics, info.TableName, info.Name)
		}
		released.Constraints = append(released.Constraints, info)
	}
	return released
}
