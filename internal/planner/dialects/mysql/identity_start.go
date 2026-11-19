package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/internal/autoincrement"
	"ptah.run/internal/planner/columnchange"
	"ptah.run/internal/rundiag"
)

// startClause is the START WITH clause of an identity option list with a
// number for its start. START WITH LIMIT VALUE does not match: it starts after
// the highest value the column holds, so it never moves the counter back.
var startClause = regexp.MustCompile(`(?i)\bSTART\s+WITH\s+([-+]?\d+)\b`)

// leftOutIdentityStart returns the start the declaration of colDiff's column
// gives its identity where the plan changes the generation of an identity the
// column holds, and empty otherwise.
//
// Oracle restarts the counter at a start given to an existing identity column.
// Measured on Oracle Free 23.26, START WITH 2 on a column holding 1 to 3 makes
// the next insert fail with ORA-00001. So the MODIFY leaves the start out
// ([leaveOutIdentityStart]) and the run warns about it
// ([warnLeftOutIdentityStarts]). Both ask this function rather than a
// predicate each, so the warning names exactly the starts the plan leaves out.
func leftOutIdentityStart(colDiff difftypes.ColumnDiff) string {
	if columnchange.PreviousIdentity(colDiff) == "" || autoincrement.Generation(colDiff.Desired) == "" {
		return ""
	}
	if options := strings.TrimSpace(colDiff.Desired.IdentityOptions); options != "" {
		match := startClause.FindStringSubmatch(options)
		if match == nil {
			return ""
		}
		return match[1]
	}
	return strings.TrimSpace(colDiff.Desired.IdentityStart)
}

// leaveOutIdentityStart takes the start [leftOutIdentityStart] reads out of
// column, the MODIFY of colDiff's column on table: the declared start, and the
// START WITH clause of its option list. It returns the comment that goes above
// the MODIFY, the text of the warning the run raises, so a reviewer of a
// migration file reads it beside the statement; nothing where no start is
// left out.
func leaveOutIdentityStart(table string, colDiff difftypes.ColumnDiff, column *ast.ColumnNode) []ast.Node {
	start := leftOutIdentityStart(colDiff)
	if start == "" {
		return nil
	}
	column.IdentityStart = ""
	if match := startClause.FindStringIndex(column.IdentityOptions); match != nil {
		column.IdentityOptions = strings.Join(strings.Fields(column.IdentityOptions[:match[0]]+" "+column.IdentityOptions[match[1]:]), " ")
	}
	return []ast.Node{ast.NewComment(identityStartWarning(table, colDiff, start))}
}

// identityStartWarning says that the plan leaves out start, the START WITH
// column of table declares, why, and how to set it.
func identityStartWarning(table string, colDiff difftypes.ColumnDiff, start string) string {
	generation := strings.ReplaceAll(autoincrement.Generation(colDiff.Desired), "_", " ")
	return fmt.Sprintf("column %s.%s declares START WITH %s, and the plan leaves it out: "+
		"a start given to an identity column that holds rows restarts its counter there, "+
		"and the next insert fails with ORA-00001 where a row holds the value. "+
		"Declare START WITH LIMIT VALUE, or run ALTER TABLE %s MODIFY (%s GENERATED %s AS IDENTITY (START WITH %s)) "+
		"by hand once %s is known to be free",
		table, colDiff.ColumnName, start, table, colDiff.ColumnName, generation, start, start)
}

// warnLeftOutIdentityStarts raises one warning for each column of diff whose
// declared start the plan leaves out ([leftOutIdentityStart]), and writes it
// now, before the plan a command prints or applies.
func warnLeftOutIdentityStarts(ctx context.Context, diff *difftypes.SchemaDiff) {
	stream := rundiag.Stream(ctx)
	for _, table := range diff.TablesModified {
		for _, colDiff := range table.ColumnsModified {
			start := leftOutIdentityStart(colDiff)
			if start == "" {
				continue
			}
			column := table.TableName + "." + colDiff.ColumnName
			_ = rundiag.Warn(ctx, stream, rundiag.LeftOutIdentityStarts, rundiag.Record{
				Key:   column,
				Line:  identityStartWarning(table.TableName, colDiff, start),
				Attrs: []slog.Attr{slog.String("column", column), slog.String("start", start)},
			})
		}
	}
	rundiag.Settle(ctx, rundiag.LeftOutIdentityStarts)
}
