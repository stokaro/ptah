package migrator

import "encoding/json"

// MarshalJSON preserves the presence of an exact empty current revision key
// while keeping current_version_key absent for ordinary numeric status. The
// standard omitempty behavior cannot distinguish those states.
func (s MigrationStatus) MarshalJSON() ([]byte, error) {
	type statusJSON MigrationStatus
	encoded := statusJSON(s)
	if !s.CurrentVersionKeySet {
		// The presence bit is authoritative. Discard a stale value rather than
		// serializing it as a current identity and restoring the bit as true.
		encoded.CurrentVersionKey = ""
		return json.Marshal(encoded)
	}
	return json.Marshal(struct {
		statusJSON
		CurrentVersionKey string `json:"current_version_key"`
	}{
		statusJSON:        encoded,
		CurrentVersionKey: s.CurrentVersionKey,
	})
}

// UnmarshalJSON restores the presence of a current revision key, including an
// exact empty identity used by one Flyway repeatable migration.
func (s *MigrationStatus) UnmarshalJSON(data []byte) error {
	type statusJSON MigrationStatus
	decoded := struct {
		statusJSON
		CurrentVersionKey *string `json:"current_version_key"`
	}{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*s = MigrationStatus(decoded.statusJSON)
	if decoded.CurrentVersionKey != nil {
		s.CurrentVersionKey = *decoded.CurrentVersionKey
		s.CurrentVersionKeySet = true
	}
	return nil
}
