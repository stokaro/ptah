package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// IndexMethodsFeature lets table preparation state the kind of each declared
// common index in PreparedTable.indexes, spelled as ObservedIndex.method
// spells the kind of a held index. A host then compares the two spellings
// exactly, and a declaration that changes only an index's kind is a change.
// It is a two-way feature that requires ObservedIndexesFeature and
// PrepareTablesFeature: a provider lists it, and so does each engine whose
// preparation states kinds. Contract 25.14.
const IndexMethodsFeature = "schema.index-methods"

func validateIndexMethodsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), IndexMethodsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), IndexMethodsFeature) || !EngineOffers(description, engine.GetId(), ObservedIndexesFeature) ||
		!EngineOffers(description, engine.GetId(), PrepareTablesFeature) {
		return fmt.Errorf("%w: index methods require observed indexes and table preparation", ErrProtocol)
	}
	return nil
}

// validatePreparedIndexes checks the index kinds a prepared table states.
// Without negotiation a table states none. With it, a table states one for
// each index it declares, in the declared order, so a kind is never missing,
// stated twice or stated for an index the table does not declare.
func validatePreparedIndexes(description *protocol.Description, engine *protocol.Engine, request *protocol.PrepareTablesRequest,
	input *protocol.TablePreparationInput, output *protocol.PreparedTable,
) error {
	prepared := output.GetIndexes()
	if !EngineOffers(description, engine.GetId(), IndexMethodsFeature) || !slices.Contains(request.GetTargetFeatures(), IndexMethodsFeature) {
		if len(prepared) != 0 {
			return fmt.Errorf("%w: prepared index methods need %s", ErrProtocol, IndexMethodsFeature)
		}
		return nil
	}
	declared := input.GetDesired().GetIndexes()
	if len(prepared) != len(declared) {
		return fmt.Errorf("%w: table preparation states %d index methods for %d declared indexes", ErrProtocol, len(prepared), len(declared))
	}
	for i, index := range prepared {
		if index.GetName() != declared[i].GetName() {
			return fmt.Errorf("%w: prepared index method %d names %q, and the declared index there is %q", ErrProtocol, i, index.GetName(), declared[i].GetName())
		}
	}
	return nil
}
