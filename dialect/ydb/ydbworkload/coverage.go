package ydbworkload

import "ptah.run/core/schemaext"

// Coverage enrolls one workload family's precise model definition. A pool
// receipt cannot establish classifier coverage, or the reverse. Runtime growth
// never enlarges a source's knowledge of workload objects.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return schemaext.OwnedCoverage("ptah.run/ydb", Codecs(), kind, representation, knowledge, subjects)
}
