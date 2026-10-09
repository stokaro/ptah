package ydbworkload

import "ptah.run/core/schemaext"

// Coverage enrolls one workload family's precise model definition. A pool
// receipt cannot establish classifier coverage, or the reverse. Runtime growth
// never enlarges a source's knowledge of workload objects.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(kind, representation, knowledge, subjects)
}

// ownedCoverage builds this package's model registry once; see
// [schemaext.OwnedCoverageSource].
var ownedCoverage = schemaext.OwnedCoverageSource("ptah.run/ydb", Codecs)
