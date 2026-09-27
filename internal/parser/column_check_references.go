package parser

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/mysqlcheck"
)

// columnCheck is one CHECK a table body wrote on a column, with where it
// started.
type columnCheck struct {
	column     string
	expression string
	position   int
}

// refuseMySQLColumnCheckReferences refuses, for MySQL, a CHECK written on a
// column that names another column of the table, as MySQL refuses it; see
// [mysqlcheck.OtherColumn]. It runs once the body is read, because a CHECK may
// name a column declared after its own.
//
// MariaDB accepts the same CHECK, measured on 11.8.9, and a document read for
// no dialect or another one is left alone.
func refuseMySQLColumnCheckReferences(table *ast.CreateTableNode, checks []columnCheck, dialect string) error {
	if platform.NormalizeDialect(dialect) != platform.MySQL {
		return nil
	}
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	for _, check := range checks {
		if other, found := mysqlcheck.OtherColumn(check.column, check.expression, columns); found {
			return fmt.Errorf("column CHECK at position %d: %w", check.position, mysqlcheck.Refusal(check.column, other))
		}
	}
	return nil
}
