package providerhost

import (
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/platform/capability"
)

// ReleaseLine is one release line a provider declares for an engine, with the
// support level in the vocabulary the capability matrix uses.
type ReleaseLine struct {
	Line     string                  `json:"line"`
	Label    string                  `json:"label,omitempty"`
	Support  capability.SupportLevel `json:"support"`
	Image    string                  `json:"image,omitempty"`
	Unprobed string                  `json:"unprobed,omitempty"`
	Note     string                  `json:"note,omitempty"`
}

// ReleaseLines returns the lines the provider declares for one engine, in the
// order it declared them. An engine the provider does not declare has none.
func (h *Host) ReleaseLines(engine string) []ReleaseLine {
	for _, declared := range h.description.GetEngines() {
		if declared.GetId() != engine {
			continue
		}
		lines := make([]ReleaseLine, 0, len(declared.GetReleaseLines()))
		for _, line := range declared.GetReleaseLines() {
			lines = append(lines, ReleaseLine{Line: line.GetLine(), Label: line.GetLabel(), Support: supportLevel(line.GetSupport()),
				Image: line.GetImage(), Unprobed: line.GetUnprobed(), Note: line.GetNote()})
		}
		return lines
	}
	return nil
}

func supportLevel(value protocol.ReleaseLine_Support) capability.SupportLevel {
	switch value {
	case protocol.ReleaseLine_CERTIFIED:
		return capability.Certified
	case protocol.ReleaseLine_LEGACY_TESTED:
		return capability.LegacyTested
	case protocol.ReleaseLine_BEST_EFFORT:
		return capability.BestEffort
	case protocol.ReleaseLine_KNOWN_INCOMPATIBLE:
		return capability.KnownIncompatible
	default:
		return ""
	}
}
