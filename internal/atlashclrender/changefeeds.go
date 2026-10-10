package atlashclrender

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/ydbsource"
)

// reportFeatureObjects names every feature value the HCL document leaves out,
// and every secret, topic or external object the source records as not
// described without holding it, which HCL has no directive for. It reads
// identities without interpreting payloads, so an unrecognized provider cannot
// turn export loss into a successful cleanup of the source annotations.
func (r *renderer) reportFeatureObjects() {
	for _, ref := range r.db.FeatureObjects.Refs() {
		if ref.Kind == objectidentity.Kind(ydbcoordination.Kind) || ref.Kind == objectidentity.Kind(tsschema.ContinuousAggregateKind) {
			continue
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     featurePath(ref),
			Message:  fmt.Sprintf("feature object %s of kind %s is not represented in HCL", ref, ref.Kind),
		})
	}
	for _, record := range r.db.FeatureCoverage.SubjectRecords() {
		state := record.Knowledge.State
		if !slices.Contains(unrecordableLimits, record.Kind) || (state != schemaext.Uninspected && state != schemaext.Unrepresentable) {
			continue
		}
		if _, held, err := r.db.FeatureObjects.Get(record.Subject); held || err != nil {
			continue
		}
		r.diagnostics = append(r.diagnostics, Diagnostic{
			Severity: SeverityWarning,
			Path:     featurePath(record.Subject),
			Message: fmt.Sprintf("feature object %s of kind %s is not described (%s), and HCL cannot record that",
				record.Subject, record.Kind, record.Knowledge.Reason),
		})
	}
	// A table's facets name the table, so the reader knows which table's
	// setting the document leaves out; any other slot names the kind alone.
	tables := make(map[*schemaext.Facets]string, len(r.db.Tables))
	for i := range r.db.Tables {
		tables[&r.db.Tables[i].Facets] = "table." + r.db.Tables[i].QualifiedName()
	}
	for _, facets := range r.db.FacetSlots() {
		for _, kind := range facets.Kinds() {
			path, held := tables[facets]
			if held && representsFacet(kind) {
				continue
			}
			if !held {
				path = "feature." + string(kind)
			}
			r.diagnostics = append(r.diagnostics, Diagnostic{
				Severity: SeverityWarning,
				Path:     path,
				Message:  fmt.Sprintf("feature facet %s is not represented in HCL", kind),
			})
		}
	}
}

// unrecordableLimits are the kinds whose source limits HCL has no directive
// for. Changefeeds, streaming queries and pools report their own losses.
var unrecordableLimits = []schemaext.Kind{ydbsecret.Kind, ydbtopic.Kind, ydbexternal.SourceKind, ydbexternal.TableKind}

func featurePath(ref objectidentity.ID) string {
	return fmt.Sprintf("features[%q][%q][%q][%q][%q][%q]", ref.Kind, ref.Catalog.Source, ref.Schema.Source, ref.Parent.Source, ref.Name.Source, ref.Signature)
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
