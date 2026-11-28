// Package viewcapture identifies settings a read could not capture for a view.
package viewcapture

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// UnreadSettings returns the explicit limits on view in coverage, in the
// coverage's stable order. Replacement and rollback share this recognition:
// either operation can recreate a view without a setting the read did not
// capture. The caller binds coverage and view to the same target identities.
// A kind with no claim about this view adds no limit here.
func UnreadSettings(coverage schemaext.Coverage, view objectidentity.ID) []schemaext.SubjectCoverage {
	var unread []schemaext.SubjectCoverage
	for _, record := range coverage.SubjectRecords() {
		if record.Subject.Key() == view.Key() &&
			(record.Knowledge.State == schemaext.Unrepresentable || record.Knowledge.State == schemaext.Uninspected) {
			unread = append(unread, record)
		}
	}
	return unread
}
