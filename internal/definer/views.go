package definer

import (
	"fmt"
	"io"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/mysqlviewsecurity"
)

// ViewsHaveSQLSecurity reports whether a view on dialect has a SQL SECURITY
// mode, and so a definer it runs as: the descriptor's
// Comparison.ViewSQLSecurity.
func ViewsHaveSQLSecurity(dialect string) bool {
	return viewSQLSecurity.Get(dialect)
}

// viewSQLSecurity holds each descriptor's Comparison.ViewSQLSecurity.
var viewSQLSecurity = descriptortable.New(func(described descriptor.Dialect) bool {
	return described.Comparison.ViewSQLSecurity
})

// RunsAsDefiner reports whether the view of viewDiff runs with its definer's
// privileges before and after the change on dialect: SQL SECURITY DEFINER on
// both sides, where a mode a side leaves out is the server's DEFINER.
func RunsAsDefiner(viewDiff difftypes.ViewDiff, dialect string) bool {
	return ViewsHaveSQLSecurity(dialect) &&
		mysqlviewsecurity.Effective(viewDiff.PreviousSecurity) == "DEFINER" &&
		mysqlviewsecurity.Effective(viewDiff.Desired.Security) == "DEFINER"
}

// ViewClauseDefiner is the definer the statement replacing the view of
// viewDiff names, or empty for a statement that names none.
//
// A plan is often applied by another account than the one whose read it was
// made from: a migration file planned by one account and applied by a deploy
// account, or a rollback run by someone else. Without a DEFINER clause that
// account becomes the definer, silently. Named, the definer stays, or the
// statement fails with ERROR 1227 for an account without the privilege to
// name another one, which the apply paths ask before the first statement;
// naming oneself needs none, so the account the read ran as applies its own
// plan as before.
//
// A forward replacement names the definer the read recorded when the view
// runs under SQL SECURITY DEFINER before and after it; the planner has
// refused every such view whose definer is another account by then. A view
// held as INVOKER and declared DEFINER, and one declared INVOKER, take no
// clause: the declaration asks for the account that applies it, or does not
// run as its definer. A rollback names the definer its forward plan read
// whenever it writes the view under SQL SECURITY DEFINER, since that is the
// view it restores, the definer included.
//
// It names nothing where no database read answers for the plan (account is
// nil), where the read recorded no definer, and on a target whose views have
// no SQL SECURITY mode. [WarnUnnamedViews] reports the replacements that run
// as their definer and name nothing.
func ViewClauseDefiner(viewDiff difftypes.ViewDiff, account *string, dialect string) string {
	switch {
	case account == nil || !ViewsHaveSQLSecurity(dialect) || viewDiff.PreviousDefiner == "":
		return ""
	case mysqlviewsecurity.Effective(viewDiff.Desired.Security) != "DEFINER":
		return ""
	case !viewDiff.Rollback && mysqlviewsecurity.Effective(viewDiff.PreviousSecurity) != "DEFINER":
		return ""
	default:
		return viewDiff.PreviousDefiner
	}
}

// UnnamedViews names, in the diff's order, the views a plan of diff replaces
// under SQL SECURITY DEFINER before and after the change with a statement
// that names no definer, so the account that applies it becomes the definer.
// A plan made from a replay or from two descriptions has no account to name,
// and the replayed state still says which mode each view runs in. A diff
// that records the connected account names none: the planner names the
// definer of each such view or refuses it.
func UnnamedViews(diff *difftypes.SchemaDiff, dialect string) []string {
	if diff == nil || diff.ConnectedAccount != nil {
		return nil
	}
	var names []string
	for _, viewDiff := range diff.ViewsModified {
		if viewDiff.Desired.Name == "" || !RunsAsDefiner(viewDiff, dialect) {
			continue
		}
		names = append(names, viewDiff.Desired.Name)
	}
	return names
}

// WarnUnnamedViews writes one warning line to w for each view
// [UnnamedViews] names. A nil w writes nothing.
func WarnUnnamedViews(w io.Writer, diff *difftypes.SchemaDiff, dialect string) {
	if w == nil {
		return
	}
	for _, name := range UnnamedViews(diff, dialect) {
		_, _ = fmt.Fprintf(w, "warning: view %s runs with its definer's privileges under SQL SECURITY DEFINER, "+
			"and the migration replaces it without a DEFINER clause, so the account that applies the migration "+
			"becomes its definer. To keep a definer, write DEFINER= into the file, or apply it as that definer\n", name)
	}
}
