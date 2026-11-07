package mysql

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/planner/keyrelease"
)

// columnKeyHolders is the removed keys and indexes that hold the name a modified
// column's own UNIQUE takes and that a foreign key still stands on when the plan
// frees the name. Dropped ahead of the column change, as [keyrelease.Find] has a
// planner do, such a key answers Error 1553 where the foreign key has no other index.
// Measured on MySQL 8.4 and 26.10 and MariaDB 12.3, `DROP INDEX pid, MODIFY COLUMN pid
// bigint UNIQUE` takes both at once: the column's key gets the name and the foreign
// key stands on it.
type columnKeyHolders struct {
	semantics identifier.Semantics
	// byColumn is, for each such column, the removals its statement holds.
	byColumn map[string]keyrelease.Releases
	// all is every removal above, for the step that would otherwise drop it first.
	all keyrelease.Releases
}

// columnKeyHoldersOf answers diff's holders of this kind among found, the removals
// [keyrelease.Find] answered, on the MySQL family. A holder an index the plan adds takes
// over from is left out, as its takeover goes ahead of the column change instead; see
// [Planner.releaseKeyNames]. dropped is the foreign keys the plan drops ahead of the
// column changes, which stand on nothing by then.
func (p *Planner) columnKeyHoldersOf(
	diff *difftypes.SchemaDiff,
	found keyrelease.Releases,
	dropped map[constraintHostKey]struct{},
) columnKeyHolders {
	holders := columnKeyHolders{
		semantics: diff.EffectiveIdentifierSemantics(p.targetDialect()),
		byColumn:  make(map[string]keyrelease.Releases),
	}
	if !p.isMySQLFamily() {
		return holders
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			_, uniqueChanged := column.Changes["unique"]
			if !uniqueChanged || !column.Desired.Unique || strings.TrimSpace(column.Desired.UniqueExpr) != "" {
				continue
			}
			taken := found.TakenBy(diff, p.targetDialect(), table, column.ColumnName)
			var held keyrelease.Releases
			for _, info := range taken.Constraints {
				if holders.foreignKeyStandsOn(table, info.Name, dropped) {
					held.Constraints = append(held.Constraints, info)
				}
			}
			for _, ref := range taken.Indexes {
				if _, over := indexTakingOverFrom(diff, ref); !over && holders.foreignKeyStandsOn(table, ref.Name, dropped) {
					held.Indexes = append(held.Indexes, ref)
				}
			}
			if len(held.Constraints)+len(held.Indexes) > 0 {
				holders.byColumn[holders.key(table.TableName, column.ColumnName)] = held
				holders.all = holders.all.With(held)
			}
		}
	}
	return holders
}

func (h columnKeyHolders) key(table, column string) string {
	return h.semantics.QualifiedTableIdentityKey(table) + "\x00" + h.semantics.ColumnIdentityKey(column)
}

// foreignKeyStandsOn reports whether a foreign key of table that is still there when
// the plan frees the name begins with the columns of the key or index called name.
func (h columnKeyHolders) foreignKeyStandsOn(
	table difftypes.TableDiff,
	name string,
	dropped map[constraintHostKey]struct{},
) bool {
	columns := h.columnsOf(table, name)
	if len(columns) == 0 {
		return false
	}
	return slices.ContainsFunc(table.Current.Constraints, func(constraint catalog.Constraint) bool {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			return false
		}
		if _, gone := dropped[constraintscope.Identity(h.semantics, table.TableName, constraint.Name)]; gone {
			return false
		}
		keyColumns := constraint.ColumnNamesOrDefault()
		return len(keyColumns) > 0 && len(keyColumns) <= len(columns) && slices.EqualFunc(keyColumns, columns[:len(keyColumns)], func(a, b string) bool {
			return h.semantics.ColumnIdentityKey(a) == h.semantics.ColumnIdentityKey(b)
		})
	})
}

// columnsOf answers the columns of the index or UNIQUE called name on table, as the
// comparison read the table.
func (h columnKeyHolders) columnsOf(table difftypes.TableDiff, name string) []string {
	same := func(other string) bool {
		return h.semantics.IndexIdentityKey(other) == h.semantics.IndexIdentityKey(name)
	}
	for _, index := range table.Current.Indexes {
		if same(index.Name) {
			return index.Columns
		}
	}
	for _, constraint := range table.Current.Constraints {
		if strings.EqualFold(constraint.Type, "UNIQUE") && same(constraint.Name) {
			return constraint.ColumnNamesOrDefault()
		}
	}
	return nil
}

// droppedWith is the drops the statement that gives column its own UNIQUE holds. A
// plain index is dropped as a unique key's is: DROP INDEX, in a clause. Each counts as
// dropped in taken, so the key, or an addition written later, that also takes over
// from it leaves it out.
func (h columnKeyHolders) droppedWith(table, column string, taken *takeoverLedger) []ast.AlterOperation {
	held := h.byColumn[h.key(table, column)]
	refs := make([]difftypes.IndexRef, 0, len(held.Constraints)+len(held.Indexes))
	for _, info := range held.Constraints {
		refs = append(refs, difftypes.IndexRef{Name: info.Name, TableName: info.TableName})
	}
	return taken.drops(append(refs, held.Indexes...))
}
