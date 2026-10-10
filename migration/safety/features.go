package safety

import (
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

// A diff report can precede planning. Use only consequences its owner has
// already established; missing or invalid metadata requires the strongest
// review. A feature leaving the common model must not disappear from reports.
//
// The access assessment is reported under its own category, so a drift report
// says "this widens access" apart from "this drops an object". An assessment
// the classifier cannot trust is reported as unknown. A change that cannot be
// snapshotted is still counted under its kind when it names a valid one, and
// without a kind only when it does not.
func appendFeatureFindings(findings *[]Finding, changes []schemaext.ChangeRecord) {
	for _, change := range changes {
		snapshot, err := change.Clone()
		if err != nil {
			suffix := ""
			if schemaext.ValidatePayload(change.Value) == nil {
				suffix = ":" + string(change.Value.Kind())
			}
			add(findings, "feature_changes"+suffix, 1, Destructive)
			if _, declares := change.Value.(schemaext.AccessEffectSource); declares {
				add(findings, accessCategory(schemaext.AccessUnknown)+suffix, 1, Destructive)
			}
			continue
		}
		kind := string(snapshot.Value.Kind())
		severity := Destructive
		if source, ok := snapshot.Value.(schemaext.EffectSource); ok {
			severity, _ = classifyFeatureEffect(source.Effect())
		}
		add(findings, "feature_changes:"+kind, 1, severity)
		if source, ok := snapshot.Value.(schemaext.AccessEffectSource); ok {
			// Clone refused an invalid assessment above, so this one is valid.
			access := source.AccessEffect().Access
			add(findings, accessCategory(access)+":"+kind, 1, AccessSeverity(access))
		}
	}
}

// accessCategory names the finding that counts changes with one assessment.
func accessCategory(access schemaext.Access) string {
	switch access {
	case schemaext.AccessWidens:
		return "feature_access_widened"
	case schemaext.AccessNarrows:
		return "feature_access_narrowed"
	case schemaext.AccessUnchanged:
		return "feature_access_unchanged"
	default:
		return "feature_access_unknown"
	}
}

// appendViewFeatureFindings reports the owner changes of a materialized view.
// A view the plan replaces loses its rows whatever its settings' changes would
// have done in place, so each change on it is as destructive as the
// replacement; reported by its own effect, a schedule changed beside the
// view's body read as one that keeps the rows (stokaro/ptah#4278).
func appendViewFeatureFindings(findings *[]Finding, view difftypes.MaterializedViewDiff) {
	if !view.Replaces() {
		appendFeatureFindings(findings, view.FeatureChanges)
		return
	}
	for _, change := range view.FeatureChanges {
		category := "feature_changes"
		if change.Value != nil {
			category += ":" + string(change.Value.Kind())
		}
		add(findings, category, 1, Destructive)
	}
}
