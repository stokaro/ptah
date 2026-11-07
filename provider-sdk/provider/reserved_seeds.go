package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ReservedSeedsFeature enables the SEEDS purpose of ReservedObject: a read
// reports the ledger the engine's seed service records seed files in as
// storage the engine keeps for itself, as it reports its migration history,
// rather than as a table of the schema. A host leaves the ledger out of every
// comparison, so a plan never drops it, and refuses a declaration that takes
// its name. It is a two-way feature. A provider lists it, and so does each
// engine whose seed service (SeedsFeature) keeps a ledger in the database.
// Contract 25.29.
const ReservedSeedsFeature = "schema.reserved-seeds"

// validateReservedSeedsFeature requires the provider's feature and the
// engine's seed service of an engine that lists the feature.
func validateReservedSeedsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ReservedSeedsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ReservedSeedsFeature) || !EngineOffers(description, engine.GetId(), SeedsFeature) {
		return fmt.Errorf("%w: a reserved seed ledger requires the provider's feature and the engine's seed service", ErrProtocol)
	}
	return nil
}

// validateInspectedSeedReservations refuses a seed ledger reservation in a
// read that did not negotiate ReservedSeedsFeature.
func validateInspectedSeedReservations(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	if EngineOffers(description, engine.GetId(), ReservedSeedsFeature) && slices.Contains(snapshot.GetTargetFeatures(), ReservedSeedsFeature) {
		return nil
	}
	for _, record := range snapshot.GetReservedObjects() {
		if record.GetPurpose() == protocol.ReservedObject_SEEDS {
			return fmt.Errorf("%w: a reserved seed ledger needs %s", ErrProtocol, ReservedSeedsFeature)
		}
	}
	return nil
}
