package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// IndexRenamesFeature enables CommonPlanningStep.renamed_index: a common step
// that renames one index of a table the provider owns in place, where the
// host's comparison paired an index the table holds with one it declares
// under another name and one definition. The owner claims the step with an
// operation that keeps the index, rather than dropping it and building it
// again. The provider and the engine list it, and the engine needs
// ObservedIndexesFeature, through which the host compares the indexes.
const IndexRenamesFeature = "planning.index-renames"

func indexRenameError(reason string) error {
	return fmt.Errorf("%w: index rename: %s", ErrProtocol, reason)
}

func validateIndexRenamesFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), IndexRenamesFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), IndexRenamesFeature) || !EngineOffers(description, engine.GetId(), ObservedIndexesFeature) {
		return indexRenameError("the engine requires the provider's feature and observed indexes")
	}
	return nil
}

// validateIndexRename checks the rename a common step carries. The feature
// is negotiated; the rename is the step's only operand; it names two
// different indexes of a parent the request alters, which holds the index as
// from and declares it as to; and the step reads the parent, drops the index
// subject from and creates the index subject to, in that order and nothing
// else.
func validateIndexRename(description *protocol.Description, request *protocol.PlanFeaturesRequest, step *protocol.CommonPlanningStep) error {
	rename := step.GetRenamedIndex()
	if rename == nil {
		return nil
	}
	if !EngineOffers(description, request.GetEngine(), IndexRenamesFeature) || !slices.Contains(request.GetTargetFeatures(), IndexRenamesFeature) {
		return indexRenameError("the feature was not negotiated")
	}
	if step.GetParent() == nil || step.GetAddedColumn() != nil || step.GetCommonObject() != nil {
		return indexRenameError("the step needs its parent and no other operand")
	}
	from, to := rename.GetFrom(), rename.GetTo()
	if !comparisonReason(from) || !comparisonReason(to) || from == to {
		return indexRenameError("the step names two different indexes")
	}
	if err := validateRenamedParent(request, step.GetParent(), from, to); err != nil {
		return err
	}
	effects := step.GetMetadata().GetEffects()
	if len(effects) != 3 || effects[0].GetAction() != protocol.PlanningEffect_READ || CompareIdentities(effects[0].GetSubject(), step.GetParent()) != 0 ||
		!renamedIndexEffect(effects[1], protocol.PlanningEffect_DROP, step.GetParent(), from) ||
		!renamedIndexEffect(effects[2], protocol.PlanningEffect_CREATE, step.GetParent(), to) {
		return indexRenameError("the step's effects are not read the parent, drop from and create to")
	}
	return nil
}

// validateRenamedParent refuses a rename whose parent the request does not
// alter, or which does not hold the index as from and declare it as to.
func validateRenamedParent(request *protocol.PlanFeaturesRequest, parent *protocol.ComparisonIdentity, from, to string) error {
	at := slices.IndexFunc(request.GetTables(), func(table *protocol.PlanningTable) bool {
		return CompareIdentities(table.GetSubject(), parent) == 0
	})
	if at < 0 || request.GetTables()[at].GetAction() != protocol.PlanningTable_ALTER {
		return indexRenameError("the parent is not a table the request alters")
	}
	table := request.GetTables()[at]
	declared := slices.ContainsFunc(table.GetDesired().GetIndexes(), func(index *protocol.DeclaredIndex) bool { return index.GetName() == to })
	held := slices.ContainsFunc(table.GetCurrent().GetIndexes(), func(index *protocol.ObservedIndex) bool { return index.GetName() == from })
	if !declared || !held {
		return indexRenameError("the parent does not hold the index as from and declare it as to")
	}
	return nil
}

// renamedIndexEffect reports an effect that takes action on the index name
// of table. An index subject names its table as its parent where index names
// are scoped to a table, and names none where they are scoped to the schema.
func renamedIndexEffect(effect *protocol.PlanningEffect, action protocol.PlanningEffect_Action, table *protocol.ComparisonIdentity, name string) bool {
	subject := effect.GetSubject()
	parent := subject.GetParent().GetNormalized()
	return effect.GetAction() == action && subject.GetKind() == "index" && subject.GetName().GetSource() == name &&
		(parent == "" || parent == table.GetName().GetNormalized()) &&
		subject.GetSchema().GetNormalized() == table.GetSchema().GetNormalized() &&
		subject.GetDatabase().GetNormalized() == table.GetDatabase().GetNormalized()
}
