package postgres

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
)

// refuseGeneratedKindChanges refuses a diff that changes the kind of an
// existing generated column, such as STORED to VIRTUAL. ALTER COLUMN SET
// EXPRESSION keeps the kind a column holds: measured on PostgreSQL 18, the
// statement over a VIRTUAL column declared STORED runs, the column stays
// VIRTUAL, and the next comparison plans the same statement again. No
// statement this planner writes changes the kind in place, so the plan stops
// and names the column rather than write one that changes nothing.
func refuseGeneratedKindChanges(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
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
			return fmt.Errorf("%w: generated column %s.%s is %s and the schema declares it %s; "+
				"the %s planner cannot change a generated column's kind in place, "+
				"so drop the column and add it back, or declare it %s",
				ptaherr.ErrUnsupportedFeature, table.TableName, column.ColumnName, held, declared, dialect, held)
		}
	}
	return nil
}
