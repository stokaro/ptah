package generator

// Reversing routines and triggers. A routine is addressed by its argument list
// as well as its name, which is what a reversal has to carry.

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectlookup"
)

// reverseProceduresRemoved names which of the added routines are procedures, so
// the rollback drops each with the verb its object takes.
//
// reverseRoutinesOfKind splits the additions a forward diff recorded into the
// removals a rollback plans, by the kind each one carries.
//
// The kind travels with the change (stokaro/ptah#2315) rather than being looked
// up in the desired schema, so a rollback does not depend on the declaration
// still being there -- and the signature comes with it, which is what makes the
// DROP addressable when the name is overloaded (stokaro/ptah#2296).
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
		// it was created with is what identifies it. A declaration that takes
		// no arguments identifies the routine with an empty list, which is not
		// the same statement as dropping it by name.
		routine.Signature = new(routine.Parameters)
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

// reverseFunctionDiffs reverses function modifications for down migrations.
//
// current is the pre-change database the rollback restores. It is the catalog
// rather than its conversion because only the catalog carries the identity that
// tells two overloads of one name apart; see [priorFunction].
func reverseFunctionDiffs(
	functionDiffs []difftypes.FunctionDiff,
	current *catalog.Database,
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
			Desired: priorFunction(
				current, prior, functionDiff.FunctionName, functionDiff.CurrentSignature,
			),
			CurrentSignature: forwardRoutineSignature(functionDiff),
		}
	}
	return reversed
}

// forwardRoutineSignature is the argument list that addresses the routine the
// forward direction leaves in the database, which is the routine a rollback
// drops when it has to rebuild it (stokaro/ptah#3288).
//
// When the forward change left the arguments alone, that routine is addressed
// exactly as the one it replaced, so the identity the comparison read from the
// catalog still holds. When the arguments changed, the routine is the
// declaration, and its parameters are the list -- the same answer the reversal
// of an addition gives in [reverseRoutinesOfKind].
func forwardRoutineSignature(functionDiff difftypes.FunctionDiff) *string {
	if _, changed := functionDiff.Changes["parameters"]; changed {
		return new(functionDiff.Desired.Parameters)
	}
	return functionDiff.CurrentSignature
}

// sameDropIdentity reports whether two drop identities address the same
// routine.
//
// Nil is not an empty list. A change that recorded no identity matches a
// catalog record that carries none either, rather than matching the routine
// that takes no arguments: those are the two answers
// [catalog.Function.DropIdentity] keeps apart, and a lookup that folded them
// would restore the wrong overload.
func sameDropIdentity(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// routineAdditionsFromRemovals turns the forward direction's removals into the
// rollback's additions, each with the declaration the pre-change database
// held: the routine read and converted with every property it has, its
// settings, STRICT, LEAKPROOF, PARALLEL and SECURITY, which the removal's own
// operand does not carry. A SECURITY DEFINER function that came back without
// its pinned search_path would run with the caller's. [refuseUnrestorableRoutines]
// refuses a removal the pre-change database does not hold before the rollback
// is planned, so every removal here finds its routine.
func routineAdditionsFromRemovals(removals difftypes.FunctionChanges, current *catalog.Database, prior *schemamodel.Database) difftypes.FunctionChanges {
	additions := slices.Clone(removals)
	for i, removal := range additions {
		if held := priorFunction(current, prior, removal.Name, removal.Signature); held.Name != "" {
			additions[i].Function = held
		}
	}
	return additions
}

// refuseUnrestorableRoutines refuses a rollback that would create a removed
// routine again without the declaration the pre-change database held: the
// read holds no routine of that name and signature to restore it from. A
// rollback built from the removal's own operand would create the routine
// without its settings, properties and privileges, and say nothing about it.
func refuseUnrestorableRoutines(diff *difftypes.SchemaDiff, current *catalog.Database, prior *schemamodel.Database) error {
	for _, removal := range append(slices.Clone(diff.FunctionsRemoved), diff.ProceduresRemoved...) {
		if priorFunction(current, prior, removal.Name, removal.Signature).Name == "" {
			signature := ""
			if removal.Signature != nil {
				signature = *removal.Signature
			}
			return fmt.Errorf("%w: cannot plan the rollback of removing %s(%s): the database read holds no such routine to restore",
				ptaherr.ErrInvalidSchemaDiff, removal.Name, signature)
		}
	}
	return nil
}

// routineGrantsFromRemovals is the privileges the rollback gives back to the
// routines it creates again, as the pre-change database held them.
//
// A routine PostgreSQL creates holds its default ACL: its owner and PUBLIC
// may execute it. The read reports that as an implicit PUBLIC EXECUTE, and an
// ACL that revoked it reports no PUBLIC row at all. So a routine whose read
// has no PUBLIC EXECUTE gets that privilege revoked, and every privilege the
// read reports for another role, grant option included, is granted again. A
// role's privilege granted by itself is the owner's own, which the new routine
// holds already. Without this a SECURITY DEFINER function revoked from PUBLIC
// came back executable by everyone.
func routineGrantsFromRemovals(removals difftypes.FunctionChanges, current *catalog.Database) (granted, revoked []difftypes.GrantRef) {
	if current == nil {
		return nil, nil
	}
	for _, removal := range removals {
		function, ok := heldFunction(current, removal.Name, removal.Signature)
		if !ok {
			continue
		}
		objectType := "FUNCTION"
		if function.Kind == schemamodel.FunctionKindProcedure {
			objectType = "PROCEDURE"
		}
		arguments := function.Signature()
		// A routine privilege names no column, so the column is the read's,
		// which is empty.
		target := func(role, privilege, column string, withOption bool) difftypes.GrantRef {
			return difftypes.GrantRef{Role: role, Privilege: privilege, ObjectType: objectType,
				ObjectName: function.QualifiedName(), Arguments: arguments, Column: column, WithOption: withOption}
		}
		publicExecutes := false
		for _, grant := range current.Grants {
			if !grantOnRoutine(grant, function, objectType) {
				continue
			}
			role := strings.TrimSpace(grant.Role)
			privilege := strings.ToUpper(strings.TrimSpace(grant.Privilege))
			if strings.EqualFold(role, "PUBLIC") && privilege == "EXECUTE" {
				publicExecutes = true
				if grant.Implicit || !grant.WithOption {
					continue
				}
			}
			if grant.Implicit || strings.EqualFold(role, strings.TrimSpace(grant.GrantedBy)) {
				continue
			}
			granted = append(granted, target(role, privilege, strings.TrimSpace(grant.Column), grant.WithOption))
		}
		if !publicExecutes {
			revoked = append(revoked, target("PUBLIC", "EXECUTE", "", false))
		}
	}
	return granted, revoked
}

// grantOnRoutine reports whether grant, read from the database, is about
// function, a routine of objectType.
func grantOnRoutine(grant catalog.Grant, function catalog.Function, objectType string) bool {
	return strings.EqualFold(strings.TrimSpace(grant.ObjectType), objectType) &&
		grant.QualifiedTarget() == function.QualifiedName() &&
		strings.TrimSpace(grant.Arguments) == strings.TrimSpace(function.Signature())
}

// heldFunction is the routine the pre-change database held under name and
// signature, as the read reported it.
func heldFunction(current *catalog.Database, name string, signature *string) (catalog.Function, bool) {
	for _, function := range current.Functions {
		if function.QualifiedName() == name && sameDropIdentity(function.DropIdentity(), signature) {
			return function, true
		}
	}
	return catalog.Function{}, false
}

// priorFunction is the function the pre-change database held, as a declaration
// the planner can render.
//
// The name is compared exactly, which is the identity the comparison that
// produced the change already used: it pairs a declared routine with a reported
// one by the name the declaration carries.
//
// The signature is compared too, because a name does not select an overload.
// Matching by name alone returned the first routine of that name, so rolling
// back a change to `f(n integer)` restored `f()` instead: a replacement then
// rewrote the wrong routine, and a rebuild dropped `f(n integer)` and never
// created it again (stokaro/ptah#3288). The comparison records the signature
// from this same catalog record, so the two agree by construction.
//
// The conversion keeps the read's routines in order, so the declaration at the
// read routine's position is the one converted from it. Where the two lists
// differ in length, the declaration is found by the name, kind and argument
// list it was converted with instead.
func priorFunction(current *catalog.Database, prior *schemamodel.Database, name string, signature *string) schemamodel.Function {
	if current == nil || prior == nil {
		return schemamodel.Function{}
	}
	for i, function := range current.Functions {
		if function.QualifiedName() != name || !sameDropIdentity(function.DropIdentity(), signature) {
			continue
		}
		if len(current.Functions) == len(prior.Functions) {
			// Keep the catalog signature for overload selection and use the
			// declaration already converted with this operation's selected
			// runtime, including its feature facets.
			return prior.Functions[i]
		}
		for _, declared := range prior.Functions {
			if declared.Name == function.QualifiedName() && declared.Kind == function.Kind && declared.Parameters == function.Parameters {
				return declared
			}
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
// A DROP is written from the two names and from whether the trigger runs a
// separately declared function, which is the one fact of the declaration the
// removal keeps. Carrying the whole declaration would leave the entry holding a
// definition nothing reads, which reads to the next person as though something
// did.
func triggerRemovalsFromAdditions(additions []difftypes.TriggerRef) []difftypes.TriggerRef {
	if len(additions) == 0 {
		return nil
	}
	removals := make([]difftypes.TriggerRef, len(additions))
	for i, ref := range additions {
		removals[i] = difftypes.TriggerRef{
			TriggerName:     ref.TriggerName,
			TableName:       ref.TableName,
			ExecuteFunction: ref.Desired.ExecuteFunction,
		}
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
