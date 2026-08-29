package generator

// Reversing the catalog-level families: extensions, sequences, synonyms,
// extended properties, row-level-security policies and roles.

import (
	"strings"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func reverseExtensionDiffs(diffs []difftypes.ExtensionDiff) []difftypes.ExtensionDiff {
	if diffs == nil {
		return nil
	}
	reversed := make([]difftypes.ExtensionDiff, len(diffs))
	for i, diff := range diffs {
		reversed[i] = difftypes.ExtensionDiff{
			Name:       diff.Name,
			FromSchema: diff.ToSchema,
			ToSchema:   diff.FromSchema,
			// The version pair is swapped for the reason the schema pair is:
			// the rollback moves the extension back to where it was. Dropping
			// it made a version-only change reverse to a diff describing no
			// change at all, so the down file said "No rollback operations
			// needed" and the extension stayed upgraded (stokaro/ptah#2418).
			FromVersion: diff.ToVersion,
			ToVersion:   diff.FromVersion,
			// Carried, not swapped: whether the extension may be moved is a
			// property of the extension, not a direction. Dropping it made
			// every reversed diff claim false, and the planner refuses a move
			// of a non-relocatable extension -- so a legal move reversed into
			// a FAILING down migration.
			Relocatable: diff.Relocatable,
		}
	}
	return reversed
}

// reverseSequenceDiffs reverses sequence modifications for down migrations.
func reverseSequenceDiffs(
	sequenceDiffs []difftypes.SequenceDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.SequenceDiff {
	reversed := make([]difftypes.SequenceDiff, len(sequenceDiffs))
	for i, sequenceDiff := range sequenceDiffs {
		reversedChanges := make(map[string]string)
		for key, change := range sequenceDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.SequenceDiff{
			SequenceName: sequenceDiff.SequenceName,
			Changes:      reversedChanges,
			Desired:      priorSequence(prior, sequenceDiff.SequenceName, semantics),
		}
	}
	return reversed
}

// reverseViewDiffs carries modified views into the down direction.
//
// The entry is carried across rather than swapped with anything: the planner
// renders a modified view from the schema it is given (the pre-change database
// schema, in the down direction), so the entry itself is what selects the prior
// definition.
//
// PreviousBody is different in kind: it names the body the view HAS when the
// statement runs, not a change. When the rollback runs, the database holds what
// the up migration wrote, which is the generated schema's body -- so that is
// what the reversed entry must carry. Getting this wrong is not cosmetic: the
// PostgreSQL planner reads it to decide whether CREATE OR REPLACE VIEW is legal
// for the rollback, and PostgreSQL refuses the replace for every column-list
// change except a trailing append.
//
// A nil schema (the deprecated reverseSchemaDiff entry point) leaves it empty,
// which planners read as "not known" and answer with drop-and-recreate. That is
// the safe direction: it always applies.
//
// Rollback is set for the same reason and is the other half of it. Where a
// planner can neither prove the replace legal nor prove it refused, the answer
// it should give differs by direction, and this is the only place that knows
// which direction is being built.
// reverseSynonymDiffs swaps each retarget so the down direction restores the
// target the database had before the up migration ran.
// reverseExtendedPropertyDiffs swaps the two values of every modified extended
// property, so the down direction restores what the database held.
func reverseExtendedPropertyDiffs(diffs []difftypes.ExtendedPropertyDiff) []difftypes.ExtendedPropertyDiff {
	if len(diffs) == 0 {
		return nil
	}
	reversed := make([]difftypes.ExtendedPropertyDiff, 0, len(diffs))
	for _, diff := range diffs {
		restored := diff
		restored.Value = diff.OldValue
		restored.OldValue = diff.Value
		reversed = append(reversed, restored)
	}
	return reversed
}

func reverseSynonymDiffs(
	diffs []difftypes.SynonymDiff,
	prior *schemamodel.Database,
) []difftypes.SynonymDiff {
	if len(diffs) == 0 {
		return nil
	}
	reversed := make([]difftypes.SynonymDiff, 0, len(diffs))
	for _, diff := range diffs {
		reversed = append(reversed, difftypes.SynonymDiff{
			SynonymName: diff.SynonymName,
			OldTarget:   diff.NewTarget,
			NewTarget:   diff.OldTarget,
			Desired:     priorSynonym(prior, diff.SynonymName),
		})
	}
	return reversed
}

// priorSequence is the sequence the pre-change database held, resolved on the
// three identity tiers, because the diff spells a name the declaration produced
// and a read reports the schema the server puts the sequence under.
func priorSequence(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.Sequence {
	if prior == nil {
		return schemamodel.Sequence{}
	}
	if sequence := objectlookup.Qualified(prior.Sequences, name, semantics); sequence != nil {
		return *sequence
	}
	return schemamodel.Sequence{}
}

// priorSynonym is the synonym the pre-change database held.
//
// The qualified name is the key on both sides, which is the one the comparison
// that produced the change already used: it maps declared synonyms by
// QualifiedName and pairs them with the reported ones under the same key.
func priorSynonym(prior *schemamodel.Database, name string) schemamodel.Synonym {
	if prior == nil {
		return schemamodel.Synonym{}
	}
	for _, synonym := range prior.Synonyms {
		if synonym.QualifiedName() == name {
			return synonym
		}
	}
	return schemamodel.Synonym{}
}

// reverseRLSPolicyDiffs reverses RLS policy modifications for down migrations
func reverseRLSPolicyDiffs(
	policyDiffs []difftypes.RLSPolicyDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.RLSPolicyDiff {
	reversed := make([]difftypes.RLSPolicyDiff, len(policyDiffs))
	for i, policyDiff := range policyDiffs {
		// For policy changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range policyDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		policy, tableSchema := priorRLSPolicy(prior, policyDiff.PolicyName, policyDiff.TableName, semantics)
		reversed[i] = difftypes.RLSPolicyDiff{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
			Changes:    reversedChanges,
			// CREATE POLICY renders from the operand, so reversing the change
			// map without reversing the operand would have the down direction
			// re-apply the predicate it is undoing.
			Desired:     policy,
			TableSchema: tableSchema,
		}
	}
	return reversed
}

// rlsAdditionsFromRemovals turns the forward direction's removals into the
// rollback's additions, giving each the declaration the pre-change database
// held.
//
// A removal carries two names, which is all a DROP needs. The addition it
// becomes renders CREATE POLICY, so the operand has to be recovered here or the
// rollback drops an access-control operation and puts nothing back.
func rlsAdditionsFromRemovals(
	removals []difftypes.RLSPolicyRef,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.RLSPolicyRef {
	if len(removals) == 0 {
		return nil
	}
	additions := make([]difftypes.RLSPolicyRef, len(removals))
	for i, ref := range removals {
		policy, tableSchema := priorRLSPolicy(prior, ref.PolicyName, ref.TableName, semantics)
		additions[i] = difftypes.RLSPolicyRef{
			PolicyName:  ref.PolicyName,
			TableName:   ref.TableName,
			Desired:     policy,
			TableSchema: tableSchema,
		}
	}
	return additions
}

// rlsRemovalsFromAdditions turns the forward direction's additions into the
// rollback's removals, dropping the operand each one carried.
//
// `DROP POLICY name ON table` is written from the two names. Carrying the
// declaration into a removal would leave the entry holding a policy nothing
// reads, which tells the next reader that something does.
func rlsRemovalsFromAdditions(additions []difftypes.RLSPolicyRef) []difftypes.RLSPolicyRef {
	if len(additions) == 0 {
		return nil
	}
	removals := make([]difftypes.RLSPolicyRef, len(additions))
	for i, ref := range additions {
		removals[i] = difftypes.RLSPolicyRef{PolicyName: ref.PolicyName, TableName: ref.TableName}
	}
	return removals
}

// priorRLSPolicy is the policy the pre-change database held, and the schema its
// table is declared under there.
//
// The two are resolved together because they are one answer: SQL Server
// addresses a policy by its table's schema, so a policy recovered without it
// would be rendered under a name the target cannot bind.
//
// The table half goes through the identity tiers, because the declaration and a
// database read do not have to spell it the same way -- which is the whole
// reason a modification's operand could not be found on the down direction
// before (stokaro/ptah#1311).
func priorRLSPolicy(
	prior *schemamodel.Database,
	policyName, tableName string,
	semantics identifier.Semantics,
) (schemamodel.RLSPolicy, string) {
	if prior == nil {
		return schemamodel.RLSPolicy{}, ""
	}
	wanted := semantics.QualifiedTableIdentityKey(tableName)
	for _, policy := range prior.RLSPolicies {
		if policy.Name != policyName {
			continue
		}
		if semantics.QualifiedTableIdentityKey(policy.Table) != wanted {
			continue
		}
		return policy, priorTableSchema(prior, policy.Table)
	}
	return schemamodel.RLSPolicy{}, ""
}

// reverseRoleDiffs reverses role modifications for down migrations
func reverseRoleDiffs(roleDiffs []difftypes.RoleDiff, prior *schemamodel.Database) []difftypes.RoleDiff {
	reversed := make([]difftypes.RoleDiff, len(roleDiffs))
	for i, roleDiff := range roleDiffs {
		// For role changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range roleDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.RoleDiff{
			RoleName: roleDiff.RoleName,
			Changes:  reversedChanges,
			// A password entry does not reverse: what it changed from is
			// unreadable, so the reversed change still says one is required.
			// The operand is what decides whether anything is written, and the
			// pre-change database holds no password -- which is the honest
			// answer, and the reason this rewrite is not optional. Carrying the
			// declaration's role through would have the down direction set the
			// NEW password (stokaro/ptah#2315).
			Desired: priorRole(prior, roleDiff.RoleName),
		}
	}
	return reversed
}

// priorRole is the role the pre-change database held.
//
// A role name is compared exactly, which is the identity the comparison that
// produced the change already used: a role lives outside any schema and is not
// resolved against a search path, so there is no qualified spelling of one and
// nothing for identifier semantics to fold.
func priorRole(prior *schemamodel.Database, name string) schemamodel.Role {
	if prior == nil {
		return schemamodel.Role{}
	}
	for _, role := range prior.Roles {
		if role.Name == name {
			return role
		}
	}
	return schemamodel.Role{}
}
