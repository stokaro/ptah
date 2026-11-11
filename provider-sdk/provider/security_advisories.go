package provider

import (
	"encoding/json"
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

const (
	// VulnerabilityScanFeature says an engine can assess server advisories.
	VulnerabilityScanFeature = "security.vulnerability-scan"
	// SecurityAdvisorySnapshotFeature carries the engine's reviewed, offline
	// server advisory snapshot in Engine.advisory_snapshot_json.
	SecurityAdvisorySnapshotFeature = "security.advisory-snapshot"
)

const maxAdvisorySnapshotBytes = 1 << 20

func validateSecurityAdvisorySnapshot(description *protocol.Description, engine *protocol.Engine) error {
	declared := slices.Contains(engine.GetFeatures(), SecurityAdvisorySnapshotFeature)
	data := engine.GetAdvisorySnapshotJson()
	if !declared {
		if len(data) != 0 {
			return fmt.Errorf("%w: engine %s supplies an advisory snapshot without %s", ErrProtocol, engine.GetId(), SecurityAdvisorySnapshotFeature)
		}
		return nil
	}
	if !slices.Contains(description.GetFeatures(), SecurityAdvisorySnapshotFeature) ||
		!slices.Contains(engine.GetFeatures(), VulnerabilityScanFeature) ||
		len(data) == 0 || len(data) > maxAdvisorySnapshotBytes || !json.Valid(data) {
		return fmt.Errorf("%w: engine %s has an invalid advisory snapshot declaration", ErrProtocol, engine.GetId())
	}
	return nil
}
