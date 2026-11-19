package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ReservedRepeatableRecordFeature enables the REPEATABLE purpose of
// ReservedObject: a read reports the record of repeatable migrations
// (RepeatableRecordFeature) as storage the engine keeps for itself, as it
// reports the revision table with HISTORY, rather than as a table of the
// schema. A host leaves the record out of every comparison, so a plan never
// drops it, and refuses a declaration that takes its name. It is a two-way
// feature. A provider lists it, and so does each engine that keeps the record.
// A host that does not list it never receives the purpose. Contract 25.45.
const ReservedRepeatableRecordFeature = "schema.reserved-repeatable-record"

// validateReservedRepeatableRecordFeature requires the provider's feature and
// the engine's record of repeatable migrations of an engine that lists the
// feature.
func validateReservedRepeatableRecordFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), ReservedRepeatableRecordFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ReservedRepeatableRecordFeature) || !EngineOffers(description, engine.GetId(), RepeatableRecordFeature) {
		return fmt.Errorf("%w: a reserved repeatable migration record requires the provider's feature and the engine's record", ErrProtocol)
	}
	return nil
}

// negotiateReservedRepeatableRecord drops the engine's feature where
// negotiation took away the record it reserves, so a host that lists the
// reservation without the record reads neither.
func negotiateReservedRepeatableRecord(engine *protocol.Engine) {
	if !slices.Contains(engine.GetFeatures(), RepeatableRecordFeature) {
		engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == ReservedRepeatableRecordFeature })
	}
}

// validateInspectedRepeatableRecordReservations refuses a reservation of the
// record of repeatable migrations in a read that did not negotiate
// ReservedRepeatableRecordFeature.
func validateInspectedRepeatableRecordReservations(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	if EngineOffers(description, engine.GetId(), ReservedRepeatableRecordFeature) && slices.Contains(snapshot.GetTargetFeatures(), ReservedRepeatableRecordFeature) {
		return nil
	}
	for _, record := range snapshot.GetReservedObjects() {
		if record.GetPurpose() == protocol.ReservedObject_REPEATABLE {
			return fmt.Errorf("%w: a reserved repeatable migration record needs %s", ErrProtocol, ReservedRepeatableRecordFeature)
		}
	}
	return nil
}
