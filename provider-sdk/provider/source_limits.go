package provider

import (
	"slices"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// SourceLimitsFeature enables narrowed coverage from claimed HCL and Go
// ptah:not-described headers. Go declares its header kinds through this feature.
const SourceLimitsFeature = "source.limits"

func validateSourceLimitsEngine(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), SourceLimitsFeature)
	if len(engine.GetGoHeaderKinds()) != 0 && !listed || listed &&
		(!slices.Contains(description.GetFeatures(), SourceLimitsFeature) || !slices.Contains(description.GetFeatures(), DecodeSourceFeature) ||
			len(engine.GetGoHeaderKinds())+len(engine.GetHclSource().GetHeaderKinds()) == 0) {
		return sourceError("source limits require negotiated headers and source decoding")
	}
	for i, kind := range engine.GetGoHeaderKinds() {
		if !validName(kind) || slices.Contains(engine.GetGoHeaderKinds()[:i], kind) {
			return sourceError("a Go header kind must be a unique lower-case name")
		}
	}
	return nil
}

func validateSourceLimits(description *protocol.Description, engine string, document *protocol.SourceDocument) error {
	var kinds []string
	for _, target := range description.GetEngines() {
		if target.GetId() == engine {
			switch document.GetFormat() {
			case protocol.SourceDocument_GO_ANNOTATIONS:
				kinds = target.GetGoHeaderKinds()
			case protocol.SourceDocument_HCL:
				if !slices.Contains(target.GetFeatures(), SourceLimitsFeature) {
					return nil // The negotiated HCL contract has its own header validation.
				}
				kinds = target.GetHclSource().GetHeaderKinds()
			default:
				return nil
			}
		}
	}
	return validateHeaderLimits(kinds, document.GetLimits())
}

func validateHeaderLimits(kinds []string, limits []*protocol.SchemaCoverageLimit) error {
	seen := make(map[[2]string]bool)
	for _, limit := range limits {
		key := [2]string{limit.GetKind(), limit.GetName()}
		if !validName(key[0]) || !slices.Contains(kinds, key[0]) || !utf8.ValidString(key[1]) || seen[key] ||
			(limit.GetReason() != "" && !validName(limit.GetReason())) || (limit.GetProvenance() != "" && !validName(limit.GetProvenance())) {
			return sourceError("a source limit needs a claimed header kind, unique subject and valid evidence tokens")
		}
		seen[key] = true
	}
	return nil
}

// A claimed header authorizes its owner to interpret coverage. The SDK checks
// the vocabulary and inventory, not the semantics of opaque header names.
func sourceLimitsAvailable(description *protocol.Description, engine string, document *protocol.SourceDocument) bool {
	if len(document.GetLimits()) == 0 || !slices.Contains(description.GetFeatures(), SourceLimitsFeature) {
		return false
	}
	for _, target := range description.GetEngines() {
		if target.GetId() == engine {
			return slices.Contains(target.GetFeatures(), SourceLimitsFeature)
		}
	}
	return false
}

func sourceCoverageRequiresLimits(coverage *protocol.ModelCoverage) bool {
	for _, kind := range coverage.GetKinds() {
		if kind.GetKnowledge().GetState() != protocol.ModelKnowledge_COMPLETE || len(kind.GetRegions()) != 0 {
			return true
		}
	}
	for _, subject := range coverage.GetSubjects() {
		switch subject.GetKnowledge().GetState() {
		case protocol.ModelKnowledge_COMPLETE, protocol.ModelKnowledge_ABSENT, protocol.ModelKnowledge_DEFAULTED:
		default:
			return true
		}
	}
	return false
}
