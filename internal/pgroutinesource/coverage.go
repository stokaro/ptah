package pgroutinesource

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgroutine"
)

// Sources describe declared functions completely. An unread function cannot be
// written as one whose omitted properties request the server defaults.
func exportCoverage(coverage schemaext.Coverage) error {
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == pgroutine.PropertiesKind && (record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable) {
			return fmt.Errorf("%w: function %s properties cannot be exported without losing their coverage: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.KindRecords() {
		if record.Model.Kind == pgroutine.PropertiesKind && record.Knowledge.Unscoped().State == schemaext.Unrepresentable {
			return fmt.Errorf("%w: function properties cannot be exported without losing their coverage: %s", ptaherr.ErrUnsupportedFeature, record.Knowledge.Unscoped().Reason)
		}
	}
	return nil
}
