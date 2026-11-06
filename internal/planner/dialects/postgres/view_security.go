package postgres

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/planrefusal"
	"ptah.run/internal/viewoptions"
)

// refuseViewChanges refuses a plan whose view changes would lose something the
// declaration still holds, or could not be carried out, before the plan writes
// a statement. It runs after the skip policy, which may leave a change out.
func (p *Planner) refuseViewChanges(diff *difftypes.SchemaDiff, rebuild viewRebuild, semantics identifier.Semantics) error {
	if err := refuseLostViewSecurity(diff); err != nil {
		return err
	}
	if err := p.refusePoliciesLostToCascade(diff, rebuild); err != nil {
		return err
	}
	return refuseUnrebuildable(diff, rebuild, semantics)
}

// refuseLostViewSecurity refuses a plan that would take security_invoker or
// security_barrier from a view because the declaration does not state it.
//
// A declaration is authoritative for a view's options, so leaving one out asks
// to remove it. For a security option that request is usually an accident. No
// schema could state these options until Ptah read them, so a database that
// holds them has them undeclared, and the removal widens what the view's
// readers see, with no error on any statement. The plan is refused before the
// server changes, and the error gives both answers: state the option to keep
// it, or state it as false to remove it. Keeping it in silence would ignore an
// author who deleted the option on purpose, and would hide the difference from
// `schema drift`, which reads the comparison and not this planner.
//
// The check reads the comparison's entry and not a statement, so it covers every
// path that loses the option: ALTER VIEW ... RESET, a CREATE OR REPLACE VIEW
// for a changed body, which clears what it does not state, and a drop and
// create. A rollback restores the state before the forward change, so an option
// it leaves out is not an omission. A storage parameter and an option held off
// remove nothing a reader can see, and plan as declared.
func refuseLostViewSecurity(diff *difftypes.SchemaDiff) error {
	var refusals []string
	var options []planrefusal.ViewOption
	for _, viewDiff := range diff.ViewsModified {
		if viewDiff.Rollback || viewDiff.Desired.Name == "" {
			continue
		}
		for _, lost := range viewoptions.LostSecurity(viewDiff.PreviousOptions, viewDiff.Desired.Options) {
			refusals = append(refusals, fmt.Sprintf(
				"view %s holds %s=%s and the schema does not state it. Without it %s. "+
					"Declare %s=true to keep it, or %s=false to remove it",
				viewDiff.Desired.Name, lost.Name, lost.Held, lost.Effect, lost.Name, lost.Name))
			options = append(options, planrefusal.ViewOption{View: viewDiff.Desired.Name, Option: lost.Name})
		}
	}
	if len(refusals) == 0 {
		return nil
	}
	return &planrefusal.LostViewSecurityError{
		Options: options,
		Err: fmt.Errorf("%w: %s. An HCL schema states these options in the view's security block, as invoker and barrier",
			ptaherr.ErrUnsupportedFeature, strings.Join(refusals, "; ")),
	}
}
