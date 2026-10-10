package atlashclrender

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/synonym"
	"ptah.run/internal/ydbsource"
)

// reportFeatureObjects names every feature value the HCL document leaves out,
// and every secret, topic, external object, async replication or transfer the
// source records as not described without holding it, which HCL has no
// directive for. It reads
// identities without interpreting payloads, so an unrecognized provider cannot
// turn export loss into a successful cleanup of the source annotations.
func (r *renderer) reportFeatureObjects() {
	for _, ref := range r.db.FeatureObjects.Refs() {
		if ref.Kind == objectidentity.Kind(ydbcoordination.Kind) || ref.Kind == objectidentity.Kind(tsschema.ContinuousAggregateKind) ||
			ref.Kind == objectidentity.Kind(pgpolicy.PolicyKind) || ref.Kind == objectidentity.Kind(mssqlproperty.Kind) ||
			ref.Kind == objectidentity.Kind(synonym.Kind) {
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
	r.reportFacets()
}

// reportFacets reports each facet the document leaves out. A table's facets
// name the table, so the reader knows which table's setting is missing; any
// other slot names the kind alone.
func (r *renderer) reportFacets() {
	tables := make(map[*schemaext.Facets]string, len(r.db.Tables))
	writes := make(map[*schemaext.Facets]func(schemaext.Kind) bool, len(r.db.Tables)+len(r.db.Indexes)+len(r.db.Fields))
	for i := range r.db.Tables {
		tables[&r.db.Tables[i].Facets] = "table." + r.db.Tables[i].QualifiedName()
		writes[&r.db.Tables[i].Facets] = WritesTableFacet
	}
	for i := range r.db.Indexes {
		writes[&r.db.Indexes[i].Facets] = WritesIndexFacet
	}
	for i := range r.db.Fields {
		writes[&r.db.Fields[i].Facets] = WritesColumnFacet
	}
	for _, facets := range r.db.FacetSlots() {
		for _, kind := range facets.Kinds() {
			if written, found := writes[facets]; found && written(kind) {
				continue
			}
			path, held := tables[facets]
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
var unrecordableLimits = []schemaext.Kind{ydbsecret.Kind, ydbtopic.Kind, ydbexternal.SourceKind, ydbexternal.TableKind,
	ydbreplication.ReplicationKind, ydbreplication.TransferKind}

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
