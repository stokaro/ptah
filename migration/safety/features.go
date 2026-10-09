package safety

import "ptah.run/core/schemaext"

// A diff report can precede planning. Use only consequences its owner has
// already established; missing or invalid metadata requires the strongest
// review. A feature leaving the common model must not disappear from reports.
func appendFeatureFindings(findings *[]Finding, changes []schemaext.ChangeRecord) {
	for _, change := range changes {
		snapshot, err := change.Clone()
		if err != nil {
			add(findings, "feature_changes", 1, Destructive)
			continue
		}
		severity := Destructive
		if source, ok := snapshot.Value.(schemaext.EffectSource); ok {
			severity, _ = classifyFeatureEffect(source.Effect())
		}
		add(findings, "feature_changes:"+string(snapshot.Value.Kind()), 1, severity)
	}
}
