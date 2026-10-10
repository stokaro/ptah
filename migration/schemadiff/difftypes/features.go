package difftypes

import "ptah.run/core/schemaext"

// FeatureContext is every named feature object on each side of a
// comparison, changed or not. A planner reads it where a rule about one
// object depends on objects the change set does not name.
//
// DesiredObjects are the objects the target schema declares and
// CurrentObjects the ones the database holds, each in its own representation.
// CurrentCoverage is what the read established about each kind, so a
// planner can tell an object the read recorded and could not describe from
// one the database does not hold.
type FeatureContext struct {
	DesiredObjects  schemaext.Objects
	CurrentObjects  schemaext.Objects
	CurrentCoverage schemaext.Coverage
}
