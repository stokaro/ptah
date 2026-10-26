package atlasschema

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// PlanProviderBinding records the provider release that shaped a plan and the
// target facts it was computed against.
//
// A plan binds to the provider ID, the release version and the major contract
// version. It does not bind to the executable's digest: the same release
// installed again, or built for another platform, applies the plan, and
// another release refuses it. The installation's checksum is verified when the
// provider starts, and the codec definitions a native operation carries are
// verified when the plan is decoded.
//
// TargetSHA256 uses the versioned JSON projection below; URLs, credentials,
// and Protobuf bytes never enter that projection.
type PlanProviderBinding struct {
	ID            string `json:"id"`
	Version       string `json:"version"`
	ContractMajor uint32 `json:"contract_major"`
	TargetSHA256  string `json:"target_sha256"`
}

// planProviderAdvice ends each refusal that names a selected provider. A
// binding cannot be repaired by editing the plan; only planning again records
// the release that applies it.
const planProviderAdvice = "create and approve a new plan with the selected provider"

func planProviderBinding(info catalog.ServerInfo) (*PlanProviderBinding, error) {
	if info.Provider.IsZero() {
		return nil, nil
	}
	if err := info.Provider.Validate(); err != nil {
		return nil, err
	}
	if !info.DatabaseContext.IsZero() {
		if err := info.DatabaseContext.Validate(); err != nil {
			return nil, err
		}
	}
	rules := info.IdentifierSemantics.Clone()
	slices.SortFunc(rules.ResolvedNames, func(a, b identifier.ResolvedName) int {
		if order := cmp.Compare(a.Name, b.Name); order != 0 {
			return order
		}
		return cmp.Compare(a.Key, b.Key)
	})
	// Field names and order define projection version 2. Capability JSON uses
	// sorted names; resolved identifier equivalences are sorted above. A future
	// projection change must use a new version rather than reinterpret a digest.
	projection := struct {
		Version         int                       `json:"version"`
		Target          string                    `json:"target"`
		ServerVersion   string                    `json:"server_version"`
		Schema          string                    `json:"schema"`
		WholeServer     bool                      `json:"whole_server"`
		DefaultIntSize  int                       `json:"default_int_size"`
		Capabilities    capability.Capabilities   `json:"capabilities"`
		Identifiers     identifier.Semantics      `json:"identifiers"`
		DatabaseContext schemaext.DatabaseContext `json:"database_context"`
	}{2, info.Dialect, info.Version, info.Schema, info.WholeServer, info.DefaultIntSize, info.Capabilities, rules, info.DatabaseContext}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return nil, fmt.Errorf("encode provider target facts: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return &PlanProviderBinding{
		ID: info.Provider.ID, Version: info.Provider.Version, ContractMajor: info.Provider.ProtocolRevision,
		TargetSHA256: hex.EncodeToString(sum[:]),
	}, nil
}

func validatePlanProvider(binding *PlanProviderBinding) error {
	if binding == nil {
		return nil
	}
	for _, value := range []string{binding.ID, binding.Version} {
		if value == "" || value != strings.TrimSpace(value) {
			return errors.New("plan provider binding requires a provider ID and a release version")
		}
	}
	if binding.ContractMajor == 0 {
		return errors.New("plan provider binding requires a contract major version")
	}
	decoded, err := hex.DecodeString(binding.TargetSHA256)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != binding.TargetSHA256 {
		return errors.New("plan provider target requires a lowercase SHA-256 digest")
	}
	return nil
}

// verifyPlanProvider refuses a connection served by anything other than the
// provider release the plan names. The refusal names both sides, so the
// operator can see which installation changed.
func verifyPlanProvider(plan *PlanProviderBinding, info catalog.ServerInfo) error {
	if err := validatePlanProvider(plan); err != nil {
		return err
	}
	current, err := planProviderBinding(info)
	if err != nil {
		return err
	}
	switch {
	case plan == nil && current == nil:
		return nil
	case plan == nil:
		return fmt.Errorf("plan was computed without an executable provider, but provider %q version %q serves the connection; %s",
			current.ID, current.Version, planProviderAdvice)
	case current == nil:
		return fmt.Errorf("plan was computed with provider %q version %q, but no executable provider serves the connection; create and approve a new plan",
			plan.ID, plan.Version)
	case plan.ID != current.ID:
		return fmt.Errorf("plan was computed with provider %q, but provider %q serves the connection; %s",
			plan.ID, current.ID, planProviderAdvice)
	case plan.Version != current.Version:
		return fmt.Errorf("plan was computed with provider %q version %q, but the selected provider is version %q; %s",
			plan.ID, plan.Version, current.Version, planProviderAdvice)
	case plan.ContractMajor != current.ContractMajor:
		return fmt.Errorf("plan was computed with provider %q version %q under provider contract major version %d, but the selected provider uses major version %d; %s",
			plan.ID, plan.Version, plan.ContractMajor, current.ContractMajor, planProviderAdvice)
	case plan.TargetSHA256 != current.TargetSHA256:
		return fmt.Errorf("plan provider target facts changed: the database context, server version, schema scope, capabilities or identifier rules differ from planning; %s",
			planProviderAdvice)
	}
	return nil
}
