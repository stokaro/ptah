package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// IndexNullsOrderFeature has an engine state the NULLS order of every key
// part of an index on both sides. A read states it on each part of each held
// index, in CapturedIndexPart.nulls, and table preparation states it for each
// key part of each declared index, in PreparedIndex.nulls, filling a part
// that states none with the engine's default for its direction. Both spell it
// FIRST or LAST, so a host compares the two sides exactly, and a declaration
// that changes only a key's NULLS order is a change. It is a two-way feature
// that requires ObservedIndexesFeature and PrepareTablesFeature: the provider
// lists it, and so does each engine that states the order. Contract 25.28.
const IndexNullsOrderFeature = "schema.index-nulls-order"

// nullsOrders are the spellings of a key part's NULLS order.
var nullsOrders = []string{"FIRST", "LAST"}

func validateIndexNullsOrderFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), IndexNullsOrderFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), IndexNullsOrderFeature) || !EngineOffers(description, engine.GetId(), ObservedIndexesFeature) ||
		!EngineOffers(description, engine.GetId(), PrepareTablesFeature) {
		return fmt.Errorf("%w: index NULLS order requires the provider's feature, observed indexes and table preparation", ErrProtocol)
	}
	return nil
}

// validateInspectedIndexNulls checks that a read under a negotiated NULLS
// order states it on every key part of every index. A held index then lists
// each key part in parts, so no part is left to the reader's default. A read
// without the feature is checked as before it.
func validateInspectedIndexNulls(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	if !EngineOffers(description, engine.GetId(), IndexNullsOrderFeature) || !slices.Contains(snapshot.GetTargetFeatures(), IndexNullsOrderFeature) {
		return nil
	}
	for _, table := range snapshot.GetTables() {
		for _, index := range table.GetIndexes() {
			if len(index.GetParts()) == 0 {
				return fmt.Errorf("%w: observed index %q states no key part to give a NULLS order", ErrProtocol, index.GetName())
			}
			for _, part := range index.GetParts() {
				if !slices.Contains(nullsOrders, part.GetNulls()) {
					return fmt.Errorf("%w: observed index %q has a key part with NULLS order %q; it is FIRST or LAST", ErrProtocol, index.GetName(), part.GetNulls())
				}
			}
		}
	}
	return nil
}

// declaredKeyParts is the number of key parts a declared index has: its parts,
// or its fields when it lists no parts.
func declaredKeyParts(index *protocol.DeclaredIndex) int {
	if len(index.GetParts()) != 0 {
		return len(index.GetParts())
	}
	return len(index.GetFields())
}

// validatePreparedIndexNulls checks the NULLS orders one prepared index
// states under a negotiated feature: one of FIRST or LAST for each key part of
// its declaration.
func validatePreparedIndexNulls(declared *protocol.DeclaredIndex, prepared *protocol.PreparedIndex) error {
	if len(prepared.GetNulls()) != declaredKeyParts(declared) {
		return fmt.Errorf("%w: table preparation states %d NULLS orders for the %d key parts of index %q", ErrProtocol,
			len(prepared.GetNulls()), declaredKeyParts(declared), declared.GetName())
	}
	for _, order := range prepared.GetNulls() {
		if !slices.Contains(nullsOrders, order) {
			return fmt.Errorf("%w: table preparation states NULLS order %q for index %q; it is FIRST or LAST", ErrProtocol, order, declared.GetName())
		}
	}
	return nil
}
