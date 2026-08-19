// Package indexscope compares index names using database namespace semantics.
package indexscope

import (
	"fmt"
	"iter"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// Resolver contains the validated target indexes needed by one migration plan.
// Index lookup is constant time and uses the canonical table-qualified identity.
type Resolver struct {
	semantics identifier.Semantics
	indexes   map[types.IndexRef]goschema.Index
}

// Resolve returns the target index identified by ref.
func (r *Resolver) Resolve(ref types.IndexRef) (goschema.Index, error) {
	if r == nil {
		return goschema.Index{}, fmt.Errorf(
			"%w: no validated target indexes are available",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	index, ok := r.indexes[IdentityKeyWithSemantics(r.semantics, ref)]
	if !ok {
		return goschema.Index{}, fmt.Errorf(
			"%w: target index %s.%s was not part of the validated plan",
			ptaherr.ErrInvalidSchemaDiff,
			ref.TableName,
			ref.Name,
		)
	}
	return index, nil
}

func validateDiff(dialect string, semantics identifier.Semantics, diff *types.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	if err := validateRefs(dialect, semantics, "added", diff.IndexesAdded); err != nil {
		return err
	}
	return validateRefs(dialect, semantics, "removed", diff.IndexesRemoved)
}

// NewResolver validates every index identity needed to plan diff and indexes
// the target schema for constant-time addition lookup. Removals are
// self-contained because their owning table is part of IndexRef.
func NewResolver(
	dialect string,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) (*Resolver, error) {
	return NewResolverWithSemantics(dialect, identifier.ForDialect(dialect), diff, generated)
}

// NewResolverWithSemantics validates and indexes target indexes using explicit
// live or offline identifier semantics.
func NewResolverWithSemantics(
	dialect string,
	semantics identifier.Semantics,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) (*Resolver, error) {
	if diff == nil {
		return nil, fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := validateDiff(dialect, semantics, diff); err != nil {
		return nil, err
	}
	resolver, err := newTargetResolver(dialect, semantics, generated)
	if err != nil {
		return nil, err
	}
	if len(diff.IndexesAdded) == 0 {
		return resolver, nil
	}
	for index, ref := range diff.IndexesAdded {
		if _, exists := resolver.indexes[IdentityKeyWithSemantics(semantics, ref)]; !exists {
			return nil, fmt.Errorf(
				"%w: added index %s.%s at position %d is missing or ambiguous in the target schema",
				ptaherr.ErrInvalidSchemaDiff,
				ref.TableName,
				ref.Name,
				index,
			)
		}
	}
	return resolver, nil
}

func newTargetResolver(
	dialect string,
	semantics identifier.Semantics,
	generated *goschema.Database,
) (*Resolver, error) {
	resolver := &Resolver{
		semantics: semantics,
		indexes:   make(map[types.IndexRef]goschema.Index),
	}
	if generated == nil {
		return resolver, nil
	}
	tracker := NewConflictSetWithSemantics(semantics, nil)
	// Materialized views are relations an index can belong to, not just
	// tables: PostgreSQL accepts CREATE INDEX on one, and a UNIQUE index on
	// one is what REFRESH MATERIALIZED VIEW CONCURRENTLY requires. Resolving
	// against tables alone left the owner empty, and the refusal that
	// followed named a position in a slice rather than the index or the view
	// (stokaro/ptah#1725).
	tableNames := goschema.ResolveIndexOwners(generated.Indexes, generated.Tables, generated.MaterializedViews)
	for position, index := range generated.Indexes {
		ref := types.IndexRef{
			Name:      index.Name,
			TableName: tableNames[position],
		}
		if err := validateRef("target", position, ref); err != nil {
			return nil, err
		}
		if err := validateResolvedRef(semantics, "target", position, ref); err != nil {
			return nil, err
		}
		if previous, conflict := tracker.firstMatch(ref); conflict {
			return nil, conflictError(dialect, "target", previous, ref)
		}
		tracker.add(ref)
		resolver.indexes[IdentityKeyWithSemantics(semantics, ref)] = index
	}
	return resolver, nil
}

// IdentityKey returns the dialect-aware comparison identity for ref. It is
// intended only for map and set keys; callers must keep the original ref when
// rendering SQL so the declared identifier spelling is preserved.
func IdentityKey(dialect string, ref types.IndexRef) types.IndexRef {
	return IdentityKeyWithSemantics(identifier.ForDialect(dialect), ref)
}

// IdentityKeyWithSemantics returns the confirmed comparison identity for ref.
// Original references must still be retained for SQL rendering.
func IdentityKeyWithSemantics(
	semantics identifier.Semantics,
	ref types.IndexRef,
) types.IndexRef {
	ref.Name = semantics.IndexIdentityKey(ref.Name)
	ref.TableName = semantics.QualifiedTableIdentityKey(ref.TableName)
	return ref
}

// ConflictSet indexes table-qualified references using the target dialect's
// index namespace. It supports constant-time exact conflict checks.
type ConflictSet struct {
	semantics identifier.Semantics
	matches   map[namespaceKey][]types.IndexRef
}

// NewConflictSet builds a dialect-aware conflict index for refs.
func NewConflictSet(dialect string, refs []types.IndexRef) *ConflictSet {
	return NewConflictSetWithSemantics(identifier.ForDialect(dialect), refs)
}

// NewConflictSetWithSemantics builds an index of confirmed and potential
// identifier collisions under semantics.
func NewConflictSetWithSemantics(
	semantics identifier.Semantics,
	refs []types.IndexRef,
) *ConflictSet {
	set := &ConflictSet{
		semantics: semantics,
		matches:   make(map[namespaceKey][]types.IndexRef, len(refs)),
	}
	for _, ref := range refs {
		set.add(ref)
	}
	return set
}

// Contains reports whether ref conflicts with an indexed reference.
func (s *ConflictSet) Contains(ref types.IndexRef) bool {
	if s == nil {
		return false
	}
	key := conflictKey(s.semantics, ref)
	return len(s.matches[key]) > 0
}

// Matches returns conflicting references. Validated diff references retain
// their original input order. The sequence is allocation-free.
func (s *ConflictSet) Matches(ref types.IndexRef) iter.Seq[types.IndexRef] {
	return func(yield func(types.IndexRef) bool) {
		if s == nil {
			return
		}
		key := conflictKey(s.semantics, ref)
		yieldRefs(s.matches[key], yield)
	}
}

func (s *ConflictSet) add(ref types.IndexRef) {
	key := conflictKey(s.semantics, ref)
	s.matches[key] = append(s.matches[key], ref)
}

func (s *ConflictSet) firstMatch(ref types.IndexRef) (types.IndexRef, bool) {
	for match := range s.Matches(ref) {
		return match, true
	}
	return types.IndexRef{}, false
}

func yieldRefs(refs []types.IndexRef, yield func(types.IndexRef) bool) bool {
	for _, ref := range refs {
		if !yield(ref) {
			return false
		}
	}
	return true
}

func conflictKey(semantics identifier.Semantics, ref types.IndexRef) namespaceKey {
	namespace := semantics.QualifiedTableConflictKey(ref.TableName)
	if semantics.IndexNamespace == identifier.IndexNamespaceSchema {
		table, ok := tableref.Parse(namespace)
		if ok && table.Qualified {
			namespace = table.Schema
		}
	}
	return namespaceKey{
		namespace: namespace,
		name:      semantics.IndexConflictKey(ref.Name),
	}
}

func validateRefs(
	dialect string,
	semantics identifier.Semantics,
	operation string,
	refs []types.IndexRef,
) error {
	tracker := NewConflictSetWithSemantics(semantics, nil)
	for index, ref := range refs {
		if err := validateRef(operation, index, ref); err != nil {
			return err
		}
		if err := validateResolvedRef(semantics, operation, index, ref); err != nil {
			return err
		}
		if previous, conflict := tracker.firstMatch(ref); conflict {
			return conflictError(dialect, operation, previous, ref)
		}
		tracker.add(ref)
	}
	return nil
}

func validateRef(operation string, position int, ref types.IndexRef) error {
	if strings.TrimSpace(ref.Name) == "" || strings.TrimSpace(ref.TableName) == "" {
		return fmt.Errorf(
			"%w: %s index reference at position %d requires a name and owning table",
			ptaherr.ErrInvalidSchemaDiff,
			operation,
			position,
		)
	}
	return nil
}

func validateResolvedRef(
	semantics identifier.Semantics,
	operation string,
	position int,
	ref types.IndexRef,
) error {
	if semantics.IndexNames != identifier.ComparisonCatalogResolved {
		return nil
	}
	if semantics.Resolves(ref.Name) &&
		semantics.ResolvesQualifiedTable(ref.TableName) {
		return nil
	}
	return fmt.Errorf(
		"%w: %s index reference %s.%s at position %d is not covered by catalog identifier semantics",
		ptaherr.ErrInvalidSchemaDiff,
		operation,
		ref.TableName,
		ref.Name,
		position,
	)
}

func conflictError(dialect, operation string, previous, ref types.IndexRef) error {
	return fmt.Errorf(
		"%w: %s indexes %s.%s and %s.%s conflict in the %s namespace",
		ptaherr.ErrInvalidSchemaDiff,
		operation,
		previous.TableName,
		previous.Name,
		ref.TableName,
		ref.Name,
		platform.NormalizeDialect(dialect),
	)
}

type namespaceKey struct {
	namespace string
	name      string
}
