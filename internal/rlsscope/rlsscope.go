// Package rlsscope validates the row-level security policy references a
// migration plan carries, before any of them is rendered.
package rlsscope

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Validate refuses a diff whose row-level security entries cannot be planned.
//
// It reads the diff and nothing else. Every policy an entry renders travels
// with that entry (stokaro/ptah#2315), so there is no target schema to index
// and no reference left to resolve; what remains is the two ways a diff can be
// unplannable, and both are answered here rather than by emitting no statement
// for the entry. A plan that silently drops an access-control operation reports
// success while leaving the database unprotected -- the failure
// stokaro/ptah#1311 was reviewed for. The public planning contract already
// promises that an invalid schema diff is rejected with
// [ptaherr.ErrInvalidSchemaDiff].
//
// The two:
//
//   - a reference missing either half of its identity, in any of the three
//     lists. A policy name alone does not identify a policy: the name is scoped
//     to the owning table, so `tenant_isolation` is a different policy on every
//     table that declares it (stokaro/ptah#1276).
//   - two declarations that resolved to ONE identity. The comparison records
//     the pair because it cannot report it any other way: keying declarations
//     by identity is what pairs them with the database, so a collision is
//     already a single entry by the time the diff exists (stokaro/ptah#2440).
//
// An added or modified entry carrying no declaration is NOT refused here. That
// is the planner's own check, because what it means differs per dialect: a
// target without row-level security renders a diagnostic from the name alone
// and was never going to read a declaration.
func Validate(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := validateRefs("added", diff.RLSPoliciesAdded); err != nil {
		return err
	}
	if err := validateRefs("removed", diff.RLSPoliciesRemoved); err != nil {
		return err
	}
	if err := validateRefs("modified", modifiedRefs(diff)); err != nil {
		return err
	}
	for _, conflict := range diff.RLSPolicyIdentityConflicts {
		return conflictError(dialect, conflict.First, conflict.Second)
	}
	return nil
}

// modifiedRefs projects the modified policy diffs onto the same reference shape
// the added and removed lists carry, so one validation path covers all three.
func modifiedRefs(diff *difftypes.SchemaDiff) []difftypes.RLSPolicyRef {
	refs := make([]difftypes.RLSPolicyRef, 0, len(diff.RLSPoliciesModified))
	for _, policyDiff := range diff.RLSPoliciesModified {
		refs = append(refs, difftypes.RLSPolicyRef{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
		})
	}
	return refs
}

func validateRefs(operation string, refs []difftypes.RLSPolicyRef) error {
	for position, ref := range refs {
		if err := validateRef(operation, position, ref); err != nil {
			return err
		}
	}
	return nil
}

func validateRef(operation string, position int, ref difftypes.RLSPolicyRef) error {
	if strings.TrimSpace(ref.PolicyName) == "" || strings.TrimSpace(ref.TableName) == "" {
		return fmt.Errorf(
			"%w: %s RLS policy reference at position %d requires a policy name and owning table",
			ptaherr.ErrInvalidSchemaDiff,
			operation,
			position,
		)
	}
	return nil
}

func conflictError(dialect string, previous, policy schemamodel.RLSPolicy) error {
	return fmt.Errorf(
		"%w: target RLS policies %s on %s and %s on %s share one identity in %s",
		ptaherr.ErrInvalidSchemaDiff,
		previous.Name,
		previous.Table,
		policy.Name,
		policy.Table,
		platform.NormalizeDialect(dialect),
	)
}
