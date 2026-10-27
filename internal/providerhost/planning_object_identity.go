package providerhost

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
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
	for _, state := range []*protocol.ComparisonState{request.DesiredObjects, request.CurrentObjects} {
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
