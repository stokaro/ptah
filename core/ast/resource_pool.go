package ast

// ResourcePoolSpec is a YDB resource pool's settings: what `CREATE RESOURCE
// POOL <name> WITH (...)` takes. A resource pool limits the queries that run
// in it, and it belongs to the whole database rather than to a directory.
//
// Each field names the `WITH` option it carries. A nil field declares nothing,
// which YDB keeps as -1, its spelling of "no limit"; zero is a limit, since
// `CONCURRENT_QUERY_LIMIT = 0` runs no query at all. The percentages,
// DatabaseLoadCPUThreshold and ResourceWeight take a fraction; the other two
// are whole numbers. [ptah.run/internal/ydbpool] reads both sides of a
// comparison that way, so a spec naming a setting at -1 and one leaving it
// out describe the same pool.
type ResourcePoolSpec struct {
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
func (s ResourcePoolSpec) Clone() ResourcePoolSpec {
	return ResourcePoolSpec{
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

// ResourcePoolClassifierSpec is a YDB resource pool classifier: what `CREATE
// RESOURCE POOL CLASSIFIER <name> WITH (...)` takes. A classifier sends the
// queries of a user or a group to a resource pool, and of the classifiers
// that match a query, the one with the lowest rank decides.
type ResourcePoolClassifierSpec struct {
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

// CreateResourcePoolNode creates a YDB resource pool: `CREATE RESOURCE POOL
// <name> WITH (...)`. The YDB renderer writes it; every other renderer refuses
// it, because no other engine has a resource pool Ptah models.
type CreateResourcePoolNode struct {
	// Name is the pool's name. A pool belongs to the database, not to a
	// directory, so the name is not a path.
	Name string
	// Spec is the pool's settings.
	Spec ResourcePoolSpec
}

// NewCreateResourcePool creates a CREATE RESOURCE POOL node for the pool name
// carrying spec.
func NewCreateResourcePool(name string, spec ResourcePoolSpec) *CreateResourcePoolNode {
	return &CreateResourcePoolNode{Name: name, Spec: spec.Clone()}
}

// Accept implements the Node interface for CreateResourcePoolNode.
func (n *CreateResourcePoolNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterResourcePoolNode changes a YDB resource pool's settings in place:
// `ALTER RESOURCE POOL <name> SET (...), RESET (...)`. It carries the pool
// before the change as well as after it, because the statement is the
// difference between the two: a setting only Previous names is reset.
type AlterResourcePoolNode struct {
	// Name is the pool's name.
	Name string
	// Spec is the pool as it is to be.
	Spec ResourcePoolSpec
	// Previous is the pool as the database holds it.
	Previous ResourcePoolSpec
}

// NewAlterResourcePool creates an ALTER RESOURCE POOL node that moves the pool
// name from previous to spec.
func NewAlterResourcePool(name string, spec, previous ResourcePoolSpec) *AlterResourcePoolNode {
	return &AlterResourcePoolNode{Name: name, Spec: spec.Clone(), Previous: previous.Clone()}
}

// Accept implements the Node interface for AlterResourcePoolNode.
func (n *AlterResourcePoolNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropResourcePoolNode drops a YDB resource pool: `DROP RESOURCE POOL
// <name>`. A classifier that names the pool stays, and its queries run in the
// pool `default`.
type DropResourcePoolNode struct {
	// Name is the pool's name.
	Name string
}

// NewDropResourcePool creates a DROP RESOURCE POOL node for the pool name.
func NewDropResourcePool(name string) *DropResourcePoolNode {
	return &DropResourcePoolNode{Name: name}
}

// Accept implements the Node interface for DropResourcePoolNode.
func (n *DropResourcePoolNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// CreateResourcePoolClassifierNode creates a YDB resource pool classifier:
// `CREATE RESOURCE POOL CLASSIFIER <name> WITH (...)`. The YDB renderer writes
// it; every other renderer refuses it.
type CreateResourcePoolClassifierNode struct {
	// Name is the classifier's name. A classifier belongs to the database,
	// not to a directory.
	Name string
	// Spec is the classifier's pool, member and rank.
	Spec ResourcePoolClassifierSpec
}

// NewCreateResourcePoolClassifier creates a CREATE RESOURCE POOL CLASSIFIER
// node for the classifier name carrying spec.
func NewCreateResourcePoolClassifier(name string, spec ResourcePoolClassifierSpec) *CreateResourcePoolClassifierNode {
	return &CreateResourcePoolClassifierNode{Name: name, Spec: spec}
}

// Accept implements the Node interface for CreateResourcePoolClassifierNode.
func (n *CreateResourcePoolClassifierNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterResourcePoolClassifierNode changes a YDB resource pool classifier in
// place: `ALTER RESOURCE POOL CLASSIFIER <name> SET (...)`, with `RESET
// (MEMBER_NAME)` when the classifier stops naming a member.
type AlterResourcePoolClassifierNode struct {
	// Name is the classifier's name.
	Name string
	// Spec is the classifier as it is to be.
	Spec ResourcePoolClassifierSpec
	// Previous is the classifier as the database holds it.
	Previous ResourcePoolClassifierSpec
}

// NewAlterResourcePoolClassifier creates an ALTER RESOURCE POOL CLASSIFIER
// node that moves the classifier name from previous to spec.
func NewAlterResourcePoolClassifier(
	name string,
	spec, previous ResourcePoolClassifierSpec,
) *AlterResourcePoolClassifierNode {
	return &AlterResourcePoolClassifierNode{Name: name, Spec: spec, Previous: previous}
}

// Accept implements the Node interface for AlterResourcePoolClassifierNode.
func (n *AlterResourcePoolClassifierNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropResourcePoolClassifierNode drops a YDB resource pool classifier: `DROP
// RESOURCE POOL CLASSIFIER <name>`.
type DropResourcePoolClassifierNode struct {
	// Name is the classifier's name.
	Name string
}

// NewDropResourcePoolClassifier creates a DROP RESOURCE POOL CLASSIFIER node
// for the classifier name.
func NewDropResourcePoolClassifier(name string) *DropResourcePoolClassifierNode {
	return &DropResourcePoolClassifierNode{Name: name}
}

// Accept implements the Node interface for DropResourcePoolClassifierNode.
func (n *DropResourcePoolClassifierNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
