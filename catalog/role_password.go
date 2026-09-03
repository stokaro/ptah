package catalog

import "fmt"

// RolePasswordState reports what an introspection source can establish about
// whether a role has a password. Its zero value is [RolePasswordUnknown].
//
// The state deliberately carries no password or hash. Some database catalogs
// expose presence only to privileged readers, so unknown is different from an
// observed absence and must remain different in serialized catalog snapshots.
type RolePasswordState string

const (
	// RolePasswordUnknown means the reader could not safely establish whether
	// the role has a password.
	RolePasswordUnknown RolePasswordState = ""
	// RolePasswordAbsent means the reader established that the role has no
	// password.
	RolePasswordAbsent RolePasswordState = "absent"
	// RolePasswordPresent means the reader established that the role has a
	// password without reading or retaining the password value.
	RolePasswordPresent RolePasswordState = "present"
)

// String returns the stable serialized spelling of the state.
func (s RolePasswordState) String() string {
	if s == RolePasswordUnknown {
		return "unknown"
	}
	return string(s)
}

// MarshalText writes the stable string representation used by catalog JSON.
func (s RolePasswordState) MarshalText() ([]byte, error) {
	switch s {
	case RolePasswordUnknown, RolePasswordAbsent, RolePasswordPresent:
		return []byte(s.String()), nil
	default:
		return nil, fmt.Errorf("invalid role password state %q", string(s))
	}
}

// UnmarshalText reads a stable role password state representation.
func (s *RolePasswordState) UnmarshalText(text []byte) error {
	switch string(text) {
	case "unknown":
		*s = RolePasswordUnknown
	case "absent":
		*s = RolePasswordAbsent
	case "present":
		*s = RolePasswordPresent
	default:
		return fmt.Errorf("invalid role password state %q", string(text))
	}
	return nil
}
