package atlashclrender

import (
	"fmt"
)

// reportFeatureObjects names every feature value the HCL document leaves out.
// It reads identities without interpreting payloads, so an unrecognized provider
// cannot turn export loss into a successful cleanup of the source annotations.
func (r *renderer) reportFeatureObjects() {
	for _, ref := range r.db.FeatureObjects.Refs() {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     fmt.Sprintf("features[%q][%q][%q][%q][%q][%q]", ref.Kind, ref.Catalog.Source, ref.Schema.Source, ref.Parent.Source, ref.Name.Source, ref.Signature),
			Message:  fmt.Sprintf("feature object %s of kind %s is not represented in HCL", ref, ref.Kind),
		})
	}
	for _, facets := range r.db.FacetSlots() {
		for _, kind := range facets.Kinds() {
			r.diagnostics = append(r.diagnostics, Diagnostic{
				Severity: SeverityWarning,
				Path:     "feature." + string(kind),
				Message:  fmt.Sprintf("feature facet %s is not represented in HCL", kind),
			})
		}
	}
}

// reportSecrets names every YDB secret the document leaves out, because HCL
// has no block for one. Like a changefeed's, the loss makes
// `--cleanup-go-annotations` refuse, and reading the document back drops no
// secret: the loader records that HCL cannot express one.
func (r *renderer) reportSecrets() {
	for _, secret := range r.db.Secrets {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     "secret." + secret.QualifiedName(),
			Message:  fmt.Sprintf("secret %s is not represented in HCL", secret.QualifiedName()),
		})
	}
}

// reportExternalObjects names every YDB external data source and external
// table the document leaves out, because HCL has no block for either, for the
// reasons [renderer.reportSecrets] gives.
func (r *renderer) reportExternalObjects() {
	for _, source := range r.db.ExternalDataSources {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     "external_data_source." + source.QualifiedName(),
			Message:  fmt.Sprintf("external data source %s is not represented in HCL", source.QualifiedName()),
		})
	}
	for _, table := range r.db.ExternalTables {
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     "external_table." + table.QualifiedName(),
			Message:  fmt.Sprintf("external table %s is not represented in HCL", table.QualifiedName()),
		})
	}
}
