package modelast

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// validateFeatureLowering accounts for feature state before the first node is
// visited. The current lowering path attaches named children to their CREATE
// TABLE. Other placements need a selected owner lowering service; dropping them
// would leave a successful AST that no downstream renderer can refuse.
func validateFeatureLowering(database schemamodel.Database, dialect string) error {
	for _, facets := range database.FacetSlots() {
		if !facets.IsZero() {
			return fmt.Errorf("%w: no schema-to-AST lowering for feature facet %q", ptaherr.ErrUnsupportedFeature, facets.Kinds()[0])
		}
	}
	if err := validateLoweringCoverage(database.FeatureCoverage); err != nil {
		return err
	}
	if err := schemaprep.ValidateFeatureParents(&database, dialect); err != nil {
		return err
	}
	for _, ref := range database.FeatureObjects.Refs() {
		if ref.Parent.Empty() {
			return fmt.Errorf("%w: no schema-to-AST lowering for standalone feature object %s", ptaherr.ErrUnsupportedFeature, ref)
		}
		if database.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref).State == schemaext.Absent {
			return fmt.Errorf("%w: declared feature object %s is marked absent", schemaext.ErrInvalidValue, ref)
		}
	}
	return nil
}

func validateLoweringCoverage(coverage schemaext.Coverage) error {
	if !coverage.IsZero() && coverage.Representation() != schemaext.Desired {
		return fmt.Errorf("%w: schema-to-AST requires desired feature coverage", schemaext.ErrInvalidValue)
	}
	// Knowledge is not an executable object. Complete and absent claims can
	// accompany declarations without creating DDL. Default requests and known
	// representation loss require owner interpretation before this boundary.
	for _, record := range coverage.KindRecords() {
		if err := validateLoweringKnowledge(record.Model.Kind, record.Knowledge); err != nil {
			return err
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if err := validateLoweringKnowledge(record.Kind, record.Knowledge); err != nil {
			return err
		}
	}
	return nil
}

func validateLoweringKnowledge(kind schemaext.Kind, knowledge schemaext.Knowledge) error {
	if knowledge.State == schemaext.Defaulted || knowledge.State == schemaext.Unrepresentable {
		return fmt.Errorf("%w: feature %q requires owner lowering for %q knowledge", ptaherr.ErrUnsupportedFeature, kind, knowledge.State)
	}
	return nil
}
