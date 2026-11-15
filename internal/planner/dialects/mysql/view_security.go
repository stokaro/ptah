package mysql

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/mysqlviewsecurity"
	"ptah.run/internal/planrefusal"
)

// refuseLostSQLSecurity refuses a plan that would take SQL SECURITY INVOKER
// from a view because the declaration names no mode.
//
// A declaration is authoritative for a view, and one that names no mode states
// the server default, DEFINER: CREATE OR REPLACE VIEW without the clause selects
// it, and the view then runs with its definer's privileges, not its caller's.
// Leaving the mode out of a view held as INVOKER is usually an accident, a view
// altered by hand or a declaration that predates the mode, and no statement
// fails when the view changes hands. So the plan is refused before the server
// changes, whatever else changes on the view, and the error gives both answers:
// declare INVOKER to keep it, or DEFINER to change it. Keeping the mode in
// silence would ignore an author who deleted it on purpose, and would hide the
// difference from `schema drift`, which reads the comparison and not this
// planner. The PostgreSQL planner answers a lost security_invoker the same way.
//
// The check reads the comparison's entry, which carries the held mode only on a
// target whose views have one, so it names no dialect. A rollback restores the
// state before the forward change, so a mode it leaves out is not an omission.
func refuseLostSQLSecurity(diff *difftypes.SchemaDiff) error {
	var refusals []string
	var views []planrefusal.ViewOption
	for _, viewDiff := range diff.ViewsModified {
		if viewDiff.Rollback || viewDiff.Desired.Name == "" ||
			!mysqlviewsecurity.Lost(viewDiff.PreviousSecurity, viewDiff.Desired.Security) {
			continue
		}
		refusals = append(refusals, fmt.Sprintf(
			"view %s holds SQL SECURITY INVOKER and the schema does not state it. "+
				"Without it the view runs with its definer's privileges, not its caller's. "+
				"Declare SQL SECURITY INVOKER to keep it, or SQL SECURITY DEFINER to change it",
			viewDiff.Desired.Name))
		views = append(views, planrefusal.ViewOption{View: viewDiff.Desired.Name, Option: mysqlviewsecurity.Name})
	}
	if len(refusals) == 0 {
		return nil
	}
	return &planrefusal.LostViewSecurityError{
		Options: views,
		Err: fmt.Errorf(`%w: %s. A Go annotation states the mode as security="INVOKER", `+
			`a YAML view as security: INVOKER, and an HCL view as security = INVOKER`,
			ptaherr.ErrUnsupportedFeature, strings.Join(refusals, "; ")),
	}
}
