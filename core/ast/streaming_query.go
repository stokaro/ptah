package ast

// StreamingQuerySpec declares a YDB query that continuously processes topic
// messages. Text is the body inside DO BEGIN ... END DO. A nil Run starts the
// query; an empty ResourcePool selects the database's default pool.
type StreamingQuerySpec struct {
	// Text contains the YQL statements executed by the streaming query.
	Text string `json:"text" yaml:"text"`
	// Run selects whether the query runs; nil means true.
	Run *bool `json:"run,omitempty" yaml:"run,omitempty"`
	// ResourcePool names the execution pool, or is empty for default.
	ResourcePool string `json:"resource_pool,omitempty" yaml:"resource_pool,omitempty"`
}

// Clone returns an independent copy, including the optional Run value.
func (s StreamingQuerySpec) Clone() StreamingQuerySpec {
	s.Run = clonePointer(s.Run)
	return s
}
