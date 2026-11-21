package postgres

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/planrefusal"
)

// refuseGeneratedKindChanges refuses a diff that changes the kind of an
// existing generated column, such as STORED to VIRTUAL. ALTER COLUMN SET
// EXPRESSION keeps the kind a column holds: measured on PostgreSQL 18, the
// statement over a VIRTUAL column declared STORED runs, the column stays
// VIRTUAL, and the next comparison plans the same statement again. No
// statement this planner writes changes the kind in place, so the plan stops
// and names every such column rather than write one that changes nothing. The
// refusal carries the columns, so a plan report names each one with both kinds.
func refuseGeneratedKindChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	var columns []planrefusal.GeneratedKind
	var clauses []string
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			if _, changed := column.Changes["generated"]; !changed {
				continue
			}
			held := strings.ToUpper(strings.TrimSpace(column.PreviousGeneratedKind))
			declared := strings.ToUpper(strings.TrimSpace(column.Desired.GeneratedKind))
			if held == "" || declared == "" || strings.TrimSpace(column.Desired.GeneratedExpression) == "" || held == declared {
				continue
			}
			columns = append(columns, planrefusal.GeneratedKind{
				Table: table.TableName, Column: column.ColumnName, HeldKind: held, DeclaredKind: declared,
			})
			clauses = append(clauses, fmt.Sprintf("generated column %s.%s is %s and the schema declares it %s; "+
				"the %s planner cannot change a generated column's kind in place, "+
				"so drop the column and add it back, or declare it %s",
				table.TableName, column.ColumnName, held, declared, dialect, held))
		}
	}
	if len(columns) == 0 {
		return nil
	}
	return &planrefusal.GeneratedKindError{
		Columns: columns,
		Err:     fmt.Errorf("%w: %s", ptaherr.ErrUnsupportedFeature, strings.Join(clauses, "; ")),
	}
}
