package atlashclrender

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// reportFeatureObjects names every feature value the HCL document leaves out,
// and every secret, topic, external object, async replication or transfer the
// source records as not described without holding it, which HCL has no
// directive for. It reads
// identities without interpreting payloads, so an unrecognized provider cannot
// turn export loss into a successful cleanup of the source annotations.
func (r *renderer) reportFeatureObjects() {
	for _, ref := range r.db.FeatureObjects.Refs() {
		if r.owners.Encodes(schemaext.Kind(ref.Kind)) {
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
		if !r.owners.Unrecorded(record.Kind) || (state != schemaext.Uninspected && state != schemaext.Unrepresentable) {
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

// reportFacets reports each facet the document leaves out: a facet of a
// table, column, index, primary key, constraint, routine, or materialized view
// that no owner of the render writes, and every facet anywhere else. A table's facets name the
// table, so the reader knows which table's setting is missing; any other slot
// names the kind alone.
func (r *renderer) reportFacets() {
	tables := make(map[*schemaext.Facets]string, len(r.db.Tables))
	blocks := make(map[*schemaext.Facets]bool, 2*len(r.db.Tables)+len(r.db.Constraints)+len(r.db.Indexes)+len(r.db.Fields))
	for i := range r.db.Tables {
		tables[&r.db.Tables[i].Facets] = "table." + r.db.Tables[i].QualifiedName()
		blocks[&r.db.Tables[i].Facets], blocks[&r.db.Tables[i].PrimaryKeyFacets] = true, true
	}
	for i := range r.db.Constraints {
		blocks[&r.db.Constraints[i].Facets] = true
	}
	for i := range r.db.Indexes {
		blocks[&r.db.Indexes[i].Facets] = true
	}
	for i := range r.db.Fields {
		blocks[&r.db.Fields[i].Facets] = true
	}
	for i := range r.db.Functions {
		blocks[&r.db.Functions[i].Facets] = true
	}
	for i := range r.db.MaterializedViews {
		blocks[&r.db.MaterializedViews[i].Facets] = true
	}
	for _, facets := range r.db.FacetSlots() {
		for _, kind := range facets.Kinds() {
			if blocks[facets] && r.owners.Encodes(kind) {
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

func featurePath(ref objectidentity.ID) string {
	if !ref.Database.Empty() || !ref.Namespace.IsZero() {
		return fmt.Sprintf("features[%q]", ref.String())
	}
	return fmt.Sprintf("features[%q][%q][%q][%q][%q][%q]", ref.Kind, ref.Catalog.Source, ref.Schema.Source, ref.Parent.Source, ref.Name.Source, ref.Signature)
}
