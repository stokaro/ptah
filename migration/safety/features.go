package safety

import "ptah.run/core/schemaext"

// A diff report can precede planning. Use only consequences its owner has
// already established; missing or invalid metadata requires the strongest
// review. A feature leaving the common model must not disappear from reports.
//
// The access assessment is reported under its own category, so a drift report
// says "this widens access" apart from "this drops an object". An assessment
// the classifier cannot trust is reported as unknown.
func appendFeatureFindings(findings *[]Finding, changes []schemaext.ChangeRecord) {
	for _, change := range changes {
		snapshot, err := change.Clone()
		if err != nil {
			add(findings, "feature_changes", 1, Destructive)
			if _, declares := change.Value.(schemaext.AccessEffectSource); declares {
				add(findings, accessCategory(schemaext.AccessUnknown), 1, Destructive)
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
			access := readAccess(source.AccessEffect()).Access
			add(findings, accessCategory(access)+":"+kind, 1, accessSeverity(access))
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
