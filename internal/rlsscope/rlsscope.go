// Package rlsscope resolves row-level security policy references against the
// schema a migration plan is built from, using the identifier semantics the
// diff was produced with.
package rlsscope

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// identity is what makes two policy references the same policy: the table that
// owns it, under the target's table semantics, and the policy's own name.
//
// It is a struct rather than a joined string because both components are
// database identifiers and either may contain a dot when quoted, so no
// separator encoding is injective: table `a` with policy `"b.c"` and table
// `a.b` with policy `c` would share one joined key and one of two distinct
// policies would be dropped (stokaro/ptah#1276).
//
// The policy name is compared as written, which is what
// `compare.RLSPoliciesWithSemantics` keys on. A resolver that normalized the
// name differently from the comparator that produced the reference would miss
// exactly the references the comparator meant to hand it.
type identity struct {
	table  string
	policy string
}

// Resolver holds the target row-level security policies one migration plan may
// need, indexed by the identity the diff refers to them with.
type Resolver struct {
	semantics identifier.Semantics
	policies  map[identity]goschema.RLSPolicy
}

// Resolve returns the target policy identified by ref.
//
// A reference that resolves to nothing is an error rather than a skip. The
// planner's only alternative is to emit no statement for it, and a plan that
// silently drops an access-control operation reports success while leaving the
// database unprotected -- the failure stokaro/ptah#1311 was reviewed for. The
// public planning contract already promises that an invalid schema diff is
// rejected with [ptaherr.ErrInvalidSchemaDiff].
func (r *Resolver) Resolve(ref types.RLSPolicyRef) (goschema.RLSPolicy, error) {
	if r == nil {
		return goschema.RLSPolicy{}, fmt.Errorf(
			"%w: no validated target RLS policies are available",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	policy, ok := r.policies[identityKey(r.semantics, ref)]
	if !ok {
		return goschema.RLSPolicy{}, fmt.Errorf(
			"%w: target RLS policy %s on table %s was not part of the validated plan",
			ptaherr.ErrInvalidSchemaDiff,
			ref.PolicyName,
			ref.TableName,
		)
	}
	return policy, nil
}

// NewResolver validates every policy identity needed to plan diff and indexes
// the target schema, using conservative offline rules for dialect.
func NewResolver(
	dialect string,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) (*Resolver, error) {
	return NewResolverWithSemantics(dialect, identifier.ForDialect(dialect), diff, generated)
}

// NewResolverWithSemantics validates and indexes target policies using explicit
// live or offline identifier semantics.
//
// Validation covers every reference in the diff, in all three categories:
//
//   - a reference missing either half of its identity is refused outright,
//     because a policy name alone does not identify a policy;
//   - every ADDED and every MODIFIED reference must resolve to a declared
//     policy, since both emit a CREATE POLICY built from that declaration;
//   - a REMOVED reference needs no declaration -- `DROP POLICY name ON table`
//     is built from the reference itself -- so it is checked for shape only.
//
// The target schema is refused when two declarations collapse onto one
// identity, since the plan would then depend on which one the map happened to
// keep.
func NewResolverWithSemantics(
	dialect string,
	semantics identifier.Semantics,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) (*Resolver, error) {
	if diff == nil {
		return nil, fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := validateRefs("added", diff.RLSPoliciesAdded); err != nil {
		return nil, err
	}
	if err := validateRefs("removed", diff.RLSPoliciesRemoved); err != nil {
		return nil, err
	}
	if err := validateRefs("modified", modifiedRefs(diff)); err != nil {
		return nil, err
	}
	resolver, err := newTargetResolver(dialect, semantics, generated)
	if err != nil {
		return nil, err
	}
	if err := resolver.requireResolvable("added", diff.RLSPoliciesAdded); err != nil {
		return nil, err
	}
	return resolver, resolver.requireResolvable("modified", modifiedRefs(diff))
}

// modifiedRefs projects the modified policy diffs onto the same reference shape
// the added and removed lists carry, so one validation path covers all three.
func modifiedRefs(diff *types.SchemaDiff) []types.RLSPolicyRef {
	refs := make([]types.RLSPolicyRef, 0, len(diff.RLSPoliciesModified))
	for _, policyDiff := range diff.RLSPoliciesModified {
		refs = append(refs, types.RLSPolicyRef{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
		})
	}
	return refs
}

func newTargetResolver(
	dialect string,
	semantics identifier.Semantics,
	generated *goschema.Database,
) (*Resolver, error) {
	resolver := &Resolver{
		semantics: semantics,
		policies:  make(map[identity]goschema.RLSPolicy),
	}
	if generated == nil {
		return resolver, nil
	}
	for position, policy := range generated.RLSPolicies {
		ref := types.RLSPolicyRef{PolicyName: policy.Name, TableName: policy.Table}
		if err := validateRef("target", position, ref); err != nil {
			return nil, err
		}
		key := identityKey(semantics, ref)
		if previous, conflict := resolver.policies[key]; conflict {
			return nil, conflictError(dialect, previous, policy)
		}
		resolver.policies[key] = policy
	}
	return resolver, nil
}

func (r *Resolver) requireResolvable(operation string, refs []types.RLSPolicyRef) error {
	for position, ref := range refs {
		if _, exists := r.policies[identityKey(r.semantics, ref)]; exists {
			continue
		}
		return fmt.Errorf(
			"%w: %s RLS policy %s on table %s at position %d is missing from the target schema",
			ptaherr.ErrInvalidSchemaDiff,
			operation,
			ref.PolicyName,
			ref.TableName,
			position,
		)
	}
	return nil
}

// identityKey returns the comparison identity for ref under semantics. It is
// intended only for map keys; callers must keep the original reference when
// rendering SQL so the supplied identifier spelling is preserved.
//
// The table goes through the target's qualified-table rules, which is what
// makes `orders` and `public.orders` one table -- the difference between the
// desired spelling a comparator reports and the introspected spelling a
// rollback plans against (stokaro/ptah#1311).
func identityKey(semantics identifier.Semantics, ref types.RLSPolicyRef) identity {
	return identity{
		table:  semantics.QualifiedTableIdentityKey(ref.TableName),
		policy: ref.PolicyName,
	}
}

func validateRefs(operation string, refs []types.RLSPolicyRef) error {
	for position, ref := range refs {
		if err := validateRef(operation, position, ref); err != nil {
			return err
		}
	}
	return nil
}

func validateRef(operation string, position int, ref types.RLSPolicyRef) error {
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

func conflictError(dialect string, previous, policy goschema.RLSPolicy) error {
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
