package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withWorkloadKeys declares the keys about YDB's resource pools, backup
// collections and streaming queries. Each names an object only YDB has, and
// SQL Server's Resource Governor has a `CREATE RESOURCE POOL` of its own,
// which creates a server-wide object a probe has no business making.
func withWorkloadKeys(p plan) plan {
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.ResourcePools] = "the key names whether Ptah plans YDB's resource pools and " +
		"classifiers, which only the YDB provider does; this engine's own workload objects, such as SQL " +
		"Server's Resource Governor pools, are another thing and outlive any namespace a probe makes"
	p.undecided[capability.BackupCollections] = "the key names a YDB backup collection, which Ptah models on " +
		"no engine"
	p.undecided[capability.StreamingQueries] = "the key names a YDB streaming query; only the YDB provider renders and reads this object family"
	return p
}
