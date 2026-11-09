package providerhost

import (
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// Empty identity database parts name the selected session database. Object
// context uses the wire's absent-part spelling so its validator can check both
// observed owner spellings against the explicit request database. Namespace
// scopes keep their parts: absence there means a wider namespace.
func canonicalizePlanningObjectDatabases(request *protocol.PlanFeaturesRequest) {
	canonicalize := func(subject *protocol.ComparisonIdentity) {
		if subject != nil && subject.Database != nil && proto.Size(subject.Database) == 0 {
			subject.Database = nil
		}
	}
	for _, state := range []*protocol.ComparisonState{request.DesiredObjects, request.CurrentObjects, request.ContextInventory} {
		for _, record := range state.GetRecords() {
			canonicalize(record.Subject)
		}
		for _, covered := range state.GetCoverage().GetSubjects() {
			canonicalize(covered.Subject)
		}
	}
	for _, subject := range request.ChangedSubjects {
		canonicalize(subject)
	}
}
