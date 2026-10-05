package ast

// CoordinationNodeSpec is the configuration of a YDB coordination node: the
// settings YDB's coordination service takes when it creates or changes one.
//
// A field left at its zero value declares nothing, and a setting nobody
// declared takes the server's default. YDB stores a node's configuration as it
// was sent, so a node created without a setting reads back without it, and a
// node created with a setting at its default value reads back with it. The
// comparison reads both sides with the defaults filled in, so the two describe
// the same node.
type CoordinationNodeSpec struct {
	// SelfCheckPeriodMillis is how often, in milliseconds, the node checks
	// that it is alive.
	SelfCheckPeriodMillis uint32 `json:"self_check_period_millis,omitempty"`
	// SessionGracePeriodMillis is how long, in milliseconds, a session keeps
	// its semaphores while the node changes its leader.
	SessionGracePeriodMillis uint32 `json:"session_grace_period_millis,omitempty"`
	// ReadConsistencyMode is how a read completes: "strict" only on the
	// current leader, "relaxed" on a stale one too.
	ReadConsistencyMode string `json:"read_consistency_mode,omitempty"`
	// AttachConsistencyMode is how a session attaches, with the same two
	// values.
	AttachConsistencyMode string `json:"attach_consistency_mode,omitempty"`
	// RateLimiterCountersMode is how the node reports the counters of its
	// rate limiter resources: "aggregated" for the resource tree as a whole,
	// "detailed" for every resource.
	RateLimiterCountersMode string `json:"rate_limiter_counters_mode,omitempty"`
}

// IsZero reports whether the spec declares nothing.
func (s CoordinationNodeSpec) IsZero() bool {
	return s == CoordinationNodeSpec{}
}

// CreateCoordinationNodeNode creates a YDB coordination node.
//
// YQL has no statement for a coordination node; YDB creates one through its
// coordination service. The YDB renderer writes Ptah's own statement for it,
// which Ptah's YDB connection runs through that service, so a plan and a
// migration file hold it as text like every other step.
//
// Name is the node's path below the database root, written the way a table's
// name is: `app.locks` names the node locks in the directory app.
type CreateCoordinationNodeNode struct {
	Name string
	Spec CoordinationNodeSpec
}

// Accept implements the Node interface for CreateCoordinationNodeNode.
func (n *CreateCoordinationNodeNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterCoordinationNodeNode changes the configuration of a YDB coordination
// node. Spec names the settings that change; a field left at its zero value
// keeps the node's setting.
type AlterCoordinationNodeNode struct {
	Name string
	Spec CoordinationNodeSpec
}

// Accept implements the Node interface for AlterCoordinationNodeNode.
func (n *AlterCoordinationNodeNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropCoordinationNodeNode drops a YDB coordination node, together with the
// semaphores and the rate limiter resources it holds.
type DropCoordinationNodeNode struct {
	Name string
}

// Accept implements the Node interface for DropCoordinationNodeNode.
func (n *DropCoordinationNodeNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
