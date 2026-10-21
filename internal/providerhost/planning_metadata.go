package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func planningStepID(id plangraph.StepID) *protocol.StepIdentity {
	return &protocol.StepIdentity{Owner: id.Owner, Name: id.Name}
}
func boundPlanningStepID(id *protocol.StepIdentity) plangraph.StepID {
	return plangraph.StepID{Owner: id.GetOwner(), Name: id.GetName()}
}
func planningEffect(effect schemaext.Effect) (protocol.ChangeSemantics_Impact, error) {
	impact, ok := map[schemaext.Impact]protocol.ChangeSemantics_Impact{
		schemaext.Impact(""):  protocol.ChangeSemantics_UNKNOWN,
		schemaext.Additive:    protocol.ChangeSemantics_ADDITIVE,
		schemaext.Behavioral:  protocol.ChangeSemantics_BEHAVIORAL,
		schemaext.Destructive: protocol.ChangeSemantics_DESTRUCTIVE,
	}[effect.Impact]
	if !ok {
		return 0, fmt.Errorf("%w: unknown planning impact", schemaext.ErrInvalidValue)
	}
	return impact, nil
}
func boundPlanningImpact(impact protocol.ChangeSemantics_Impact, reason string) schemaext.Effect {
	return schemaext.Effect{
		Impact: map[protocol.ChangeSemantics_Impact]schemaext.Impact{
			protocol.ChangeSemantics_UNKNOWN:     schemaext.Impact(""),
			protocol.ChangeSemantics_ADDITIVE:    schemaext.Additive,
			protocol.ChangeSemantics_BEHAVIORAL:  schemaext.Behavioral,
			protocol.ChangeSemantics_DESTRUCTIVE: schemaext.Destructive,
		}[impact],
		Reason: reason,
	}
}
func planningMetadata(step featureplan.CommonStep) (*protocol.PlanningMetadata, error) {
	transaction, ok := map[plangraph.Transaction]protocol.PlanningMetadata_Transaction{
		plangraph.TransactionUnknown:   protocol.PlanningMetadata_UNKNOWN,
		plangraph.TransactionAllowed:   protocol.PlanningMetadata_ALLOWED,
		plangraph.TransactionRequired:  protocol.PlanningMetadata_REQUIRED,
		plangraph.TransactionForbidden: protocol.PlanningMetadata_FORBIDDEN,
	}[step.Transaction]
	if !ok {
		return nil, fmt.Errorf("%w: unknown planning transaction requirement", schemaext.ErrInvalidValue)
	}
	impact, err := planningEffect(step.Impact)
	if err != nil {
		return nil, err
	}
	result := &protocol.PlanningMetadata{Transaction: transaction, Impact: impact, Reason: step.Impact.Reason}
	for _, effect := range step.Effects {
		action, ok := map[plangraph.Action]protocol.PlanningEffect_Action{
			plangraph.Read:   protocol.PlanningEffect_READ,
			plangraph.Create: protocol.PlanningEffect_CREATE,
			plangraph.Alter:  protocol.PlanningEffect_ALTER,
			plangraph.Drop:   protocol.PlanningEffect_DROP,
		}[effect.Action]
		if !ok {
			return nil, fmt.Errorf("%w: unknown planning effect", schemaext.ErrInvalidValue)
		}
		result.Effects = append(result.Effects, &protocol.PlanningEffect{Subject: comparisonIdentity(effect.Subject), Action: action})
	}
	return result, nil
}
func planningParentAction(action featureplan.ParentAction) (protocol.PlanningTable_Action, error) {
	value, ok := map[featureplan.ParentAction]protocol.PlanningTable_Action{
		"":                       protocol.PlanningTable_CONTEXT,
		featureplan.CreateTable:  protocol.PlanningTable_CREATE,
		featureplan.AlterTable:   protocol.PlanningTable_ALTER,
		featureplan.DropTable:    protocol.PlanningTable_DROP,
		featureplan.RebuildTable: protocol.PlanningTable_REBUILD,
	}[action]
	if !ok {
		return 0, fmt.Errorf("%w: unknown planning parent action", schemaext.ErrInvalidValue)
	}
	return value, nil
}
func boundPlanningParentAction(action protocol.PlanningTable_Action) featureplan.ParentAction {
	return map[protocol.PlanningTable_Action]featureplan.ParentAction{
		protocol.PlanningTable_CONTEXT: "",
		protocol.PlanningTable_CREATE:  featureplan.CreateTable,
		protocol.PlanningTable_ALTER:   featureplan.AlterTable,
		protocol.PlanningTable_DROP:    featureplan.DropTable,
		protocol.PlanningTable_REBUILD: featureplan.RebuildTable,
	}[action]
}
func boundPlanningEffects(effects []*protocol.PlanningEffect) []plangraph.Effect {
	result := make([]plangraph.Effect, len(effects))
	actions := map[protocol.PlanningEffect_Action]plangraph.Action{
		protocol.PlanningEffect_READ:   plangraph.Read,
		protocol.PlanningEffect_CREATE: plangraph.Create,
		protocol.PlanningEffect_ALTER:  plangraph.Alter,
		protocol.PlanningEffect_DROP:   plangraph.Drop,
	}
	for i, effect := range effects {
		result[i] = plangraph.Effect{Subject: boundIdentity(effect.GetSubject()), Action: actions[effect.GetAction()]}
	}
	return result
}
func boundPlanningStepIDs(ids []*protocol.StepIdentity) []plangraph.StepID {
	result := make([]plangraph.StepID, len(ids))
	for i, id := range ids {
		result[i] = boundPlanningStepID(id)
	}
	return result
}
