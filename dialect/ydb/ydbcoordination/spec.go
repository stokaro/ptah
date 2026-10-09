package ydbcoordination

// Spec is the configuration of a YDB coordination node: the
// settings YDB's coordination service takes when it creates or changes one.
//
// A field left at its zero value declares nothing, and a setting nobody
// declared takes the server's default. YDB stores a node's configuration as it
// was sent, so a node created without a setting reads back without it, and a
// node created with a setting at its default value reads back with it. The
// comparison reads both sides with the defaults filled in, so the two describe
// the same node.
type Spec struct {
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
func (s Spec) IsZero() bool {
	return s == Spec{}
}
