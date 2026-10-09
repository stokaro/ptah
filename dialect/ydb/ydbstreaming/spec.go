// Package ydbstreaming owns YDB streaming-query configuration and semantics.
// Runtime status, checkpoints, and topic offsets are not schema values.
package ydbstreaming

// Spec captures the body inside DO BEGIN ... END DO and persistent settings.
// A nil Run starts the query; an empty ResourcePool selects the default pool.
type Spec struct {
	Text         string `json:"text" yaml:"text"`
	Run          *bool  `json:"run,omitempty" yaml:"run,omitempty"`
	ResourcePool string `json:"resource_pool,omitempty" yaml:"resource_pool,omitempty"`
}

// Clone returns a snapshot with an independent optional Run setting.
func (s Spec) Clone() Spec {
	if s.Run != nil {
		s.Run = new(*s.Run)
	}
	return s
}

// SameSettings compares captured values without resolving server defaults or
// normalizing query text. Equal compares their effective server behavior.
func (s Spec) SameSettings(other Spec) bool {
	if s.Text != other.Text || s.ResourcePool != other.ResourcePool {
		return false
	}
	if s.Run == nil || other.Run == nil {
		return s.Run == other.Run
	}
	return *s.Run == *other.Run
}
