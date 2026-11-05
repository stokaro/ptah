package backend

import protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

// RetentionSeconds reads the seconds a retention payload of representation
// holds, as normalization reads them: a value in minutes is converted.
func RetentionSeconds(payload []byte, representation protocol.ValueInput_Representation) (uint64, error) {
	return retentionSeconds(payload, representation)
}
