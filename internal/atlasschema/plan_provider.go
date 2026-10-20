package atlasschema

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// PlanProviderBinding records the selected executable and the target facts
// that shaped a plan. TargetSHA256 uses the versioned JSON projection below;
// URLs, credentials, and Protobuf bytes never enter that projection.
type PlanProviderBinding struct {
	Identity     catalog.ProviderIdentity `json:"identity"`
	TargetSHA256 string                   `json:"target_sha256"`
}

func planProviderBinding(info catalog.ServerInfo) (*PlanProviderBinding, error) {
	if info.Provider.IsZero() {
		return nil, nil
	}
	if err := info.Provider.Validate(); err != nil {
		return nil, err
	}
	rules := info.IdentifierSemantics.Clone()
	slices.SortFunc(rules.ResolvedNames, func(a, b identifier.ResolvedName) int {
		if order := cmp.Compare(a.Name, b.Name); order != 0 {
			return order
		}
		return cmp.Compare(a.Key, b.Key)
	})
	// Field names and order define projection version 1. Capability JSON uses
	// sorted names; resolved identifier equivalences are sorted above. A future
	// projection change must use a new version rather than reinterpret a digest.
	projection := struct {
		Version        int                     `json:"version"`
		Target         string                  `json:"target"`
		ServerVersion  string                  `json:"server_version"`
		Schema         string                  `json:"schema"`
		WholeServer    bool                    `json:"whole_server"`
		DefaultIntSize int                     `json:"default_int_size"`
		Capabilities   capability.Capabilities `json:"capabilities"`
		Identifiers    identifier.Semantics    `json:"identifiers"`
	}{1, info.Dialect, info.Version, info.Schema, info.WholeServer, info.DefaultIntSize, info.Capabilities, rules}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return nil, fmt.Errorf("encode provider target facts: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return &PlanProviderBinding{Identity: info.Provider, TargetSHA256: hex.EncodeToString(sum[:])}, nil
}

func validatePlanProvider(binding *PlanProviderBinding) error {
	if binding == nil {
		return nil
	}
	if err := binding.Identity.Validate(); err != nil {
		return err
	}
	decoded, err := hex.DecodeString(binding.TargetSHA256)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != binding.TargetSHA256 {
		return fmt.Errorf("plan provider target requires a lowercase SHA-256 digest")
	}
	return nil
}

func verifyPlanProvider(plan *PlanProviderBinding, info catalog.ServerInfo) error {
	if err := validatePlanProvider(plan); err != nil {
		return err
	}
	current, err := planProviderBinding(info)
	if err != nil {
		return err
	}
	if plan == nil && current == nil {
		return nil
	}
	if plan == nil || current == nil || *plan != *current {
		return fmt.Errorf("plan provider binding changed; create and approve a new plan")
	}
	return nil
}
