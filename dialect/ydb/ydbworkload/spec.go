// Package ydbworkload owns YDB resource pool and classifier configuration.
package ydbworkload

// PoolSpec is a YDB resource pool's settings: what `CREATE RESOURCE
// POOL <name> WITH (...)` takes. A resource pool limits the queries that run
// in it, and it belongs to the whole database rather than to a directory.
//
// Each field names the `WITH` option it carries. A nil field declares nothing,
// which YDB keeps as -1, its spelling of "no limit"; zero is a limit, since
// `CONCURRENT_QUERY_LIMIT = 0` runs no query at all. The percentages,
// DatabaseLoadCPUThreshold and ResourceWeight take a fraction; the other two
// are whole numbers. Readers normalize the server's -1 to nil before
// capturing a model. A declaration or model cannot contain a negative limit.
type PoolSpec struct {
	// ConcurrentQueryLimit is `concurrent_query_limit`, the most queries the
	// pool runs at once.
	ConcurrentQueryLimit *int32 `json:"concurrent_query_limit,omitempty"`
	// QueueSize is `queue_size`, the most queries that wait for a slot. YDB
	// takes it only beside a concurrent query limit or a database load
	// threshold.
	QueueSize *int32 `json:"queue_size,omitempty"`
	// DatabaseLoadCPUThreshold is `database_load_cpu_threshold`, the CPU load
	// of the database, in percent, above which the pool queues new queries.
	DatabaseLoadCPUThreshold *float64 `json:"database_load_cpu_threshold,omitempty"`
	// QueryMemoryLimitPercentPerNode is
	// `query_memory_limit_percent_per_node`, the share of a node's memory
	// one query of the pool may take.
	QueryMemoryLimitPercentPerNode *float64 `json:"query_memory_limit_percent_per_node,omitempty"`
	// QueryCPULimitPercentPerNode is `query_cpu_limit_percent_per_node`, the
	// share of a node's CPU one query of the pool may take.
	QueryCPULimitPercentPerNode *float64 `json:"query_cpu_limit_percent_per_node,omitempty"`
	// TotalCPULimitPercentPerNode is `total_cpu_limit_percent_per_node`, the
	// share of a node's CPU every query of the pool together may take.
	TotalCPULimitPercentPerNode *float64 `json:"total_cpu_limit_percent_per_node,omitempty"`
	// ResourceWeight is `resource_weight`, the pool's share of the CPU when
	// pools compete for it.
	ResourceWeight *float64 `json:"resource_weight,omitempty"`
}

// Clone returns an independent copy, so a spec handed to a comparator or a
// planner cannot be changed through the values it shares with the schema it
// came from.
func (s PoolSpec) Clone() PoolSpec {
	return PoolSpec{
		ConcurrentQueryLimit:           clonePointer(s.ConcurrentQueryLimit),
		QueueSize:                      clonePointer(s.QueueSize),
		DatabaseLoadCPUThreshold:       clonePointer(s.DatabaseLoadCPUThreshold),
		QueryMemoryLimitPercentPerNode: clonePointer(s.QueryMemoryLimitPercentPerNode),
		QueryCPULimitPercentPerNode:    clonePointer(s.QueryCPULimitPercentPerNode),
		TotalCPULimitPercentPerNode:    clonePointer(s.TotalCPULimitPercentPerNode),
		ResourceWeight:                 clonePointer(s.ResourceWeight),
	}
}

// clonePointer returns a pointer to a copy of what value points to, or nil.
func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	return new(*value)
}

// ClassifierSpec is a YDB resource pool classifier: what `CREATE
// RESOURCE POOL CLASSIFIER <name> WITH (...)` takes. A classifier sends the
// queries of a user or a group to a resource pool, and of the classifiers
// that match a query, the one with the lowest rank decides.
type ClassifierSpec struct {
	// ResourcePool is `resource_pool`, the pool the classifier sends queries
	// to. YDB does not check that it exists: a query of a classifier that
	// names no pool runs in the pool `default`.
	ResourcePool string `json:"resource_pool"`
	// MemberName is `member_name`, the user or group whose queries the
	// classifier matches, and empty for every query. YDB does not check that
	// it exists either.
	MemberName string `json:"member_name,omitempty"`
	// Rank is `rank`, the classifier's order among the classifiers of the
	// database. No two classifiers share a rank.
	Rank int64 `json:"rank"`
}
