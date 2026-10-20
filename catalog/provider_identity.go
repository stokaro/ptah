package catalog

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ProviderIdentity binds an explicitly selected provider to trusted executable
// bytes and the negotiated semantics. It carries no address or credentials.
// The zero value denotes an in-process built-in connection.
type ProviderIdentity struct {
	ID               string `json:"id"`
	Version          string `json:"version"`
	SemanticIdentity string `json:"semantic_identity"`
	ArtifactSHA256   string `json:"artifact_sha256"`
	ProtocolRevision uint32 `json:"protocol_revision"`
	// ContractSHA256 hashes the host's versioned canonical projection of the
	// negotiated description and session features, never Protobuf wire bytes.
	ContractSHA256 string `json:"contract_sha256"`
}

// IsZero reports that no executable provider was selected.
func (p ProviderIdentity) IsZero() bool { return p == (ProviderIdentity{}) }

// Validate refuses incomplete identities and malformed artifact or contract
// digests. A checksum binds trusted bytes; it does not authenticate a publisher.
func (p ProviderIdentity) Validate() error {
	for _, value := range []string{p.ID, p.Version, p.SemanticIdentity} {
		if value == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("provider identity requires an ID, version, and semantic identity")
		}
	}
	if p.ProtocolRevision == 0 {
		return fmt.Errorf("provider identity requires a protocol revision")
	}
	for _, value := range []string{p.ArtifactSHA256, p.ContractSHA256} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != 32 || value != strings.ToLower(value) {
			return fmt.Errorf("provider identity requires lowercase SHA-256 digests")
		}
	}
	return nil
}
