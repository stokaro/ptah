package mysql

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/definer"
	"ptah.run/internal/mysqlviewsecurity"
	"ptah.run/internal/planrefusal"
)

// refuseForeignViewDefiner refuses a plan that would replace a view running
// with its definer's privileges and make another account its definer.
//
// CREATE OR REPLACE VIEW without a DEFINER clause makes the account that runs
// it the view's definer. A view under SQL SECURITY DEFINER runs with its
// definer's privileges, so replacing one another account defined changes whose
// privileges it runs with, and no statement fails. The plan is applied through
// the connection the comparison read, so the account that read recorded is the
// one the replacement installs. The comparison's routine rule refuses the same
// change to a routine, and both ask [definer.Replace] whether the two accounts
// are one. A definer or an account the read did not record is refused too:
// reading a missing fact as the same account would allow the change this check
// exists to stop.
//
// A view under SQL SECURITY INVOKER on either side of the change is left
// alone. Held as INVOKER, it runs with its caller's privileges until the
// change, and a declaration that asks for DEFINER asks for the account that
// applies it. Declared as INVOKER, it runs with its caller's privileges after
// the change, whoever its definer is. A mode the declaration leaves out is the
// server's DEFINER, as the statement writes it.
//
// The check decides only where the comparison ran against a database read,
// which sets [difftypes.SchemaDiff.ConnectedAccount]. A diff of two
// descriptions has no connection that applies it, and the comparison's routine
// rule leaves it alone too. A rollback replaces the view its forward change
// wrote, not the one the read saw: that view's definer is the account that
// applied the forward change, which nothing read, so there is nothing to
// compare. Run as the same account, it leaves the definer it finds.
func refuseForeignViewDefiner(diff *difftypes.SchemaDiff) error {
	if diff.ConnectedAccount == nil {
		return nil
	}
	account := *diff.ConnectedAccount
	var refusals []string
	var views []planrefusal.ViewDefiner
	for _, viewDiff := range diff.ViewsModified {
		if viewDiff.Rollback || viewDiff.Desired.Name == "" ||
			mysqlviewsecurity.Effective(viewDiff.PreviousSecurity) != "DEFINER" ||
			mysqlviewsecurity.Effective(viewDiff.Desired.Security) != "DEFINER" {
			continue
		}
		name, held := viewDiff.Desired.Name, viewDiff.PreviousDefiner
		switch definer.Replace(held, account) {
		case definer.Kept:
			continue
		case definer.Unknown:
			refusals = append(refusals, fmt.Sprintf(
				"view %s runs with its definer's privileges under SQL SECURITY DEFINER, and the read did not "+
					"record its definer (%q) or the connected account (%q), so replacing it could change whose "+
					"privileges it runs with",
				name, held, account))
		default:
			refusals = append(refusals, fmt.Sprintf(
				"view %s runs with the privileges of its definer %s under SQL SECURITY DEFINER, and replacing "+
					"it as %s would make %s its definer. Connect as %s to replace it, declare SQL SECURITY "+
					"INVOKER if it should run with its caller's privileges, or leave the view unchanged",
				name, held, account, account, held))
		}
		views = append(views, planrefusal.ViewDefiner{View: name, Definer: held, Account: account})
	}
	if len(refusals) == 0 {
		return nil
	}
	return &planrefusal.ViewDefinerError{
		Views: views,
		Err:   fmt.Errorf("%w: %s", ptaherr.ErrUnsupportedFeature, strings.Join(refusals, "; ")),
	}
}
