package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
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

// validatePreparedIndexes checks the indexes a prepared table states: their
// kinds under IndexMethodsFeature, their NULLS orders under
// IndexNullsOrderFeature and their predicates under IndexPredicatesFeature.
// With none negotiated a table states no index. With any, a table states one
// entry for each index it declares, in the declared order, so an entry is
// never missing, stated twice or stated for an index the table does not
// declare. An entry states a kind only under index methods, NULLS orders only
// under the NULLS order, and a predicate only under index predicates.
func validatePreparedIndexes(description *protocol.Description, engine *protocol.Engine, request *protocol.PrepareTablesRequest,
	input *protocol.TablePreparationInput, output *protocol.PreparedTable,
) error {
	prepared := output.GetIndexes()
	negotiated := func(feature string) bool {
		return EngineOffers(description, engine.GetId(), feature) && slices.Contains(request.GetTargetFeatures(), feature)
	}
	features := preparedIndexFeatures{methods: negotiated(IndexMethodsFeature), nulls: negotiated(IndexNullsOrderFeature),
		predicates: negotiated(IndexPredicatesFeature)}
	if !features.methods && !features.nulls && !features.predicates {
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
		if !features.methods && index.GetMethod() != "" {
			return fmt.Errorf("%w: prepared index %q states a method, which needs %s", ErrProtocol, index.GetName(), IndexMethodsFeature)
		}
		if err := features.check(declared[i], index); err != nil {
			return err
		}
	}
	return nil
}

// preparedIndexFeatures are the features a table preparation negotiated
// that a prepared index states something under.
type preparedIndexFeatures struct {
	methods, nulls, predicates bool
}

// check checks the NULLS orders and the predicate one prepared index states:
// each only under its negotiated feature, and in the form the feature asks
// for.
func (f preparedIndexFeatures) check(declared *protocol.DeclaredIndex, index *protocol.PreparedIndex) error {
	if !f.nulls && len(index.GetNulls()) != 0 {
		return fmt.Errorf("%w: prepared index NULLS orders need %s", ErrProtocol, IndexNullsOrderFeature)
	}
	if !f.predicates && index.GetCondition() != "" {
		return fmt.Errorf("%w: prepared index predicates need %s", ErrProtocol, IndexPredicatesFeature)
	}
	if f.nulls {
		if err := validatePreparedIndexNulls(declared, index); err != nil {
			return err
		}
	}
	if f.predicates {
		return validatePreparedIndexCondition(declared, index)
	}
	return nil
}
