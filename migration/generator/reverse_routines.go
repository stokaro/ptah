package generator

// Reversing routines and triggers. A routine is addressed by its argument list
// as well as its name, which is what a reversal has to carry.

import (
	"strings"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// reverseProceduresRemoved names which of the added routines are procedures, so
// the rollback drops each with the verb its object takes.
//
// reverseRoutinesOfKind splits the additions a forward diff recorded into the
// removals a rollback plans, by the kind each one carries.
//
// The kind used to be looked up in the desired schema, because the forward diff
// recorded an addition by name only. It travels with the change now
// (stokaro/ptah#2315), so a rollback no longer depends on the declaration still
// being there -- and the signature comes with it, which is what makes the DROP
// addressable when the name is overloaded (stokaro/ptah#2296).
//
// A routine whose kind is unset is left with the functions, because a DROP
// FUNCTION that is refused is a louder failure than a DROP PROCEDURE aimed at a
// function (stokaro/ptah#1722).
func reverseRoutinesOfKind(added difftypes.FunctionChanges, isKind func(difftypes.RoutineChange) bool) difftypes.FunctionChanges {
	if len(added) == 0 {
		return nil
	}
	var removals difftypes.FunctionChanges
	for _, routine := range added {
		if !isKind(routine) {
			continue
		}
		// The declaration's parameters are the signature a rollback drops by:
		// the routine being dropped is the one this addition created, so what
		// it was created with is what identifies it.
		routine.Signature = routine.Parameters
		removals = append(removals, routine)
	}
	return removals
}

// reverseFunctionsRemoved is the additions that were plain functions.
func reverseFunctionsRemoved(added difftypes.FunctionChanges) difftypes.FunctionChanges {
	return reverseRoutinesOfKind(added, func(routine difftypes.RoutineChange) bool {
		return !routine.IsProcedure()
	})
}

// reverseProceduresRemoved is the additions that were procedures.
func reverseProceduresRemoved(added difftypes.FunctionChanges) difftypes.FunctionChanges {
	return reverseRoutinesOfKind(added, difftypes.RoutineChange.IsProcedure)
}

// reverseFunctionDiffs reverses function modifications for down migrations
func reverseFunctionDiffs(
	functionDiffs []difftypes.FunctionDiff,
	prior *schemamodel.Database,
) []difftypes.FunctionDiff {
	reversed := make([]difftypes.FunctionDiff, len(functionDiffs))
	for i, functionDiff := range functionDiffs {
		// For function changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range functionDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.FunctionDiff{
			FunctionName: functionDiff.FunctionName,
			Changes:      reversedChanges,
			// The replacement renders from the operand, so reversing the change
			// map without reversing the operand would have the down direction
			// re-apply the body it is undoing (stokaro/ptah#2315).
			Desired: priorFunction(prior, functionDiff.FunctionName),
		}
	}
	return reversed
}

// priorFunction is the function the pre-change database held.
//
// The name is compared exactly, which is the identity the comparison that
// produced the change already used: it pairs a declared routine with a reported
// one by the name the declaration carries.
func priorFunction(prior *schemamodel.Database, name string) schemamodel.Function {
	if prior == nil {
		return schemamodel.Function{}
	}
	for _, function := range prior.Functions {
		if function.Name == name {
			return function
		}
	}
	return schemamodel.Function{}
}

// reverseTriggerDiffs carries modified triggers into the down direction, on the
// same terms as reverseViewDiffs. TableName is part of the trigger's identity
// rather than a changed value, so it is preserved. PostgreSQL 17.10 accepts
// CREATE OR REPLACE TRIGGER even for a timing change, so a trigger needs no
// legality test of its own.
func reverseTriggerDiffs(
	triggerDiffs []difftypes.TriggerDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.TriggerDiff {
	reversed := make([]difftypes.TriggerDiff, len(triggerDiffs))
	for i, triggerDiff := range triggerDiffs {
		reversed[i] = difftypes.TriggerDiff{
			TriggerName: triggerDiff.TriggerName,
			TableName:   triggerDiff.TableName,
			Changes:     reverseChangeMap(triggerDiff.Changes),
			// The replacement renders from the operand, so reversing the change
			// map without reversing the operand would have the down direction
			// re-apply the definition it is undoing.
			Desired: priorTrigger(prior, triggerDiff.TableName, triggerDiff.TriggerName, semantics),
		}
	}
	return reversed
}

// triggerAdditionsFromRemovals turns the forward direction's removals into the
// rollback's additions, giving each the definition the pre-change database held.
//
// A removal carries names only, which is all a DROP needs. The addition it
// becomes renders CREATE TRIGGER, so the operand has to be recovered here or
// the rollback drops a trigger it never puts back.
func triggerAdditionsFromRemovals(
	removals []difftypes.TriggerRef,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.TriggerRef {
	if len(removals) == 0 {
		return nil
	}
	additions := make([]difftypes.TriggerRef, len(removals))
	for i, ref := range removals {
		additions[i] = difftypes.TriggerRef{
			TriggerName: ref.TriggerName,
			TableName:   ref.TableName,
			Desired:     priorTrigger(prior, ref.TableName, ref.TriggerName, semantics),
		}
	}
	return additions
}

// triggerRemovalsFromAdditions turns the forward direction's additions into the
// rollback's removals, dropping the operand each one carried.
//
// A DROP is written from the two names. Carrying the declaration into a removal
// would leave the entry holding a definition nothing reads, which reads to the
// next person as though something did.
func triggerRemovalsFromAdditions(additions []difftypes.TriggerRef) []difftypes.TriggerRef {
	if len(additions) == 0 {
		return nil
	}
	removals := make([]difftypes.TriggerRef, len(additions))
	for i, ref := range additions {
		removals[i] = difftypes.TriggerRef{TriggerName: ref.TriggerName, TableName: ref.TableName}
	}
	return removals
}

// priorTrigger is the trigger the pre-change database held, resolved on the
// terms [objectlookup.Trigger] applies: the table half carries the schema, so
// the identity tiers are applied to it and the trigger's own name is folded by
// the same comparison rule.
func priorTrigger(
	prior *schemamodel.Database,
	tableName, triggerName string,
	semantics identifier.Semantics,
) schemamodel.Trigger {
	if prior == nil {
		return schemamodel.Trigger{}
	}
	if trigger := objectlookup.Trigger(prior.Triggers, tableName, triggerName, semantics); trigger != nil {
		return *trigger
	}
	return schemamodel.Trigger{}
}
