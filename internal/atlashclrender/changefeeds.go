package atlashclrender

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/ydbsource"
)

// reportFeatureObjects names every feature value the HCL document leaves out.
// It reads identities without interpreting payloads, so an unrecognized provider
// cannot turn export loss into a successful cleanup of the source annotations.
func (r *renderer) reportFeatureObjects() {
	for _, ref := range r.db.FeatureObjects.Refs() {
		if ref.Kind == objectidentity.Kind(ydbcoordination.Kind) {
			continue
		}
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

// reportExternalObjects names every YDB external data source and external
// table the document leaves out, because HCL has no block for either. Like a
// changefeed's, the loss makes `--cleanup-go-annotations` refuse, and reading
// the document back drops no such object: the loader records that HCL cannot
// express one.
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

// coordinationNode holds the validated inputs for one HCL block.
type coordinationNode struct {
	ref  objectidentity.ID
	spec ydbcoordination.Spec
}

// captureCoordinationNodes validates every supported standalone object before
// rendering. A malformed known value cannot silently become an export warning.
func (r *renderer) captureCoordinationNodes() error {
	directives, err := ydbsource.HCLCoordinationDirectives(r.db.FeatureCoverage)
	if err != nil {
		return err
	}
	r.coordinationDirectives = directives
	objects, err := r.db.FeatureObjects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.Ref.Kind != objectidentity.Kind(ydbcoordination.Kind) {
			continue
		}
		node, ok := object.Value.(*ydbcoordination.Desired)
		if !ok {
			return fmt.Errorf("%w: HCL requires desired coordination values", schemaext.ErrInvalidValue)
		}
		if err := ydbcoordination.ValidateRef(object.Ref); err != nil {
			return err
		}
		if err := ydbcoordination.Validate(node.Spec); err != nil {
			return err
		}
		r.coordinationNodes = append(r.coordinationNodes, coordinationNode{ref: object.Ref, spec: node.Spec})
	}
	return nil
}
