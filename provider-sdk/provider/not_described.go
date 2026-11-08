package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// NotDescribedFeature enables Engine.not_described_kinds in a description and
// Schema.not_described in a read: the kinds of object an engine holds and its
// provider does not model, such as a Spanner change stream, and the read's
// records that it does not describe them. The host adds the engine's kinds to
// the vocabulary its documents may name and keeps a read's records as its
// not-described records, so the header of a document made from the read says
// that the read does not describe them, the host reads that line back, and a
// plan does not take their absence for a removal. It is a two-way feature: the
// provider and each engine whose reads record such kinds list it, and the
// engine declares at least one kind. Contract 25.41.
const NotDescribedFeature = "schema.not-described"

// notDescribedReasons are the reasons a read gives for what it does not
// describe, in the host's spelling: it did not look, the object is outside the
// read's scope, the engine's provider does not model it, or the read could not
// resolve it.
var notDescribedReasons = []string{"not-inspected", "outside-scope", "unsupported", "unresolved"}

// notDescribedProvenances are how a read learns what it does not describe, in
// the host's spelling: it watched the server, it knows the target's kind of
// engine holds such objects, it derived it from another fact, it inferred it,
// or the evidence is unavailable.
var notDescribedProvenances = []string{"observed", "derived-from-target", "derived-from-fact", "inferred", "unavailable"}

func notDescribedError(reason string) error {
	return fmt.Errorf("%w: not-described records: %s", ErrProtocol, reason)
}

// validateNotDescribedFeature refuses an engine that lists the feature without
// the provider's or without a kind, kinds without the feature, and a kind that
// is not a lowercase token or is declared twice.
func validateNotDescribedFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), NotDescribedFeature)
	kinds := engine.GetNotDescribedKinds()
	switch {
	case !listed && len(kinds) != 0:
		return notDescribedError("engine " + engine.GetId() + " declares kinds without listing " + NotDescribedFeature)
	case !listed:
		return nil
	case !slices.Contains(description.GetFeatures(), NotDescribedFeature):
		return notDescribedError("the engine's feature requires the provider's")
	case len(kinds) == 0:
		return notDescribedError("engine " + engine.GetId() + " lists " + NotDescribedFeature + " and declares no kind")
	}
	for i, kind := range kinds {
		if !removalBlockerKind.MatchString(kind) {
			return notDescribedError(fmt.Sprintf("engine %s declares a kind %q that is not a lowercase token", engine.GetId(), kind))
		}
		if slices.Contains(kinds[:i], kind) {
			return notDescribedError(fmt.Sprintf("engine %s declares kind %q twice", engine.GetId(), kind))
		}
	}
	return nil
}

// validateInspectedNotDescribed refuses records in a snapshot whose session did
// not negotiate them, and checks each record of one that did: a kind the engine
// declares, an empty name or one a host can show, a reason and a provenance a
// read gives, and no record of the same kind and name twice.
func validateInspectedNotDescribed(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	records := snapshot.GetNotDescribed()
	if len(records) == 0 {
		return nil
	}
	if !EngineOffers(description, engine.GetId(), NotDescribedFeature) || !slices.Contains(snapshot.GetTargetFeatures(), NotDescribedFeature) {
		return notDescribedError("the read's records were not negotiated")
	}
	seen := make(map[[2]string]bool, len(records))
	for _, record := range records {
		kind := record.GetKind()
		if !slices.Contains(engine.GetNotDescribedKinds(), kind) {
			return notDescribedError(fmt.Sprintf("kind %q is not one engine %s declares", kind, engine.GetId()))
		}
		if record.GetName() != "" && !blockerName(record.GetName()) {
			return notDescribedError(fmt.Sprintf("a %s record has a blank or malformed name", kind))
		}
		if !slices.Contains(notDescribedReasons, record.GetReason()) {
			return notDescribedError(fmt.Sprintf("a %s record gives reason %q, which is none of %v", kind, record.GetReason(), notDescribedReasons))
		}
		if !slices.Contains(notDescribedProvenances, record.GetProvenance()) {
			return notDescribedError(fmt.Sprintf("a %s record gives provenance %q, which is none of %v", kind, record.GetProvenance(), notDescribedProvenances))
		}
		key := [2]string{kind, record.GetName()}
		if seen[key] {
			return notDescribedError(fmt.Sprintf("%s %q is recorded twice", kind, record.GetName()))
		}
		seen[key] = true
	}
	return nil
}
