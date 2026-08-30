// Package indexscope compares index names using database namespace semantics.
package indexscope

import (
	"fmt"
	"iter"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// Resolver contains the validated target indexes needed by one migration plan.
// Index lookup is constant time and uses the canonical table-qualified identity.
func validateDiff(dialect string, semantics identifier.Semantics, diff *difftypes.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	if err := validateRefs(dialect, semantics, "added", diff.IndexAdditions()); err != nil {
		return err
	}
	if err := validateAdditionsAreDescribed(diff.IndexesAdded); err != nil {
		return err
	}
	return validateRefs(dialect, semantics, "removed", diff.IndexesRemoved)
}

// ValidateDiff refuses an index reference a plan could not act on: an empty
// name or table, or a pair two dialect-folded spellings collapse onto.
//
// It no longer resolves anything. An addition carries its own definition
// (stokaro/ptah#2315), so what is left here is the identity check that used to
// travel with the lookup.
func ValidateDiff(dialect string, diff *difftypes.SchemaDiff) error {
	return ValidateDiffWithSemantics(dialect, identifier.ForDialect(dialect), diff)
}

// ValidateDiffWithSemantics is [ValidateDiff] under explicit live or offline
// identifier semantics.
func ValidateDiffWithSemantics(
	dialect string,
	semantics identifier.Semantics,
	diff *difftypes.SchemaDiff,
) error {
	if diff == nil {
		return fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	return validateDiff(dialect, semantics, diff)
}

// ValidateDeclared refuses a declaration whose indexes collide.
//
// Two indexes collide when the target's identifier rules fold their (name,
// table) pairs onto one, which is a defect in the document rather than in any
// plan -- so it is asked of the declaration, by whoever holds one.
//
// Materialized views are relations an index can belong to, not just tables:
// PostgreSQL accepts CREATE INDEX on one, and a UNIQUE index on one is what
// REFRESH MATERIALIZED VIEW CONCURRENTLY requires. Resolving against tables
// alone left the owner empty, and the refusal that followed named a position in
// a slice rather than the index or the view (stokaro/ptah#1725) --
// [difftypes.IndexDeclarationsOf] is where that resolution happens now.
func ValidateDeclared(
	dialect string,
	semantics identifier.Semantics,
	declared difftypes.IndexChanges,
) error {
	if len(declared) == 0 {
		return nil
	}
	tracker := NewConflictSetWithSemantics(semantics, nil)
	for position, declaration := range declared {
		ref := difftypes.IndexRef{
			Name:      declaration.Index.Name,
			TableName: declaration.TableName,
		}
		if err := validateRef("target", position, ref); err != nil {
			return err
		}
		if err := validateResolvedRef(semantics, "target", position, ref); err != nil {
			return err
		}
		if previous, conflict := tracker.firstMatch(ref); conflict {
			return conflictError(dialect, "target", previous, ref)
		}
		tracker.add(ref)
	}
	return nil
}

// IdentityKey returns the dialect-aware comparison identity for ref. It is
// intended only for map and set keys; callers must keep the original ref when
// rendering SQL so the declared identifier spelling is preserved.
func IdentityKey(dialect string, ref difftypes.IndexRef) difftypes.IndexRef {
	return IdentityKeyWithSemantics(identifier.ForDialect(dialect), ref)
}

// IdentityKeyWithSemantics returns the confirmed comparison identity for ref.
// Original references must still be retained for SQL rendering.
func IdentityKeyWithSemantics(
	semantics identifier.Semantics,
	ref difftypes.IndexRef,
) difftypes.IndexRef {
	ref.Name = semantics.IndexIdentityKey(ref.Name)
	ref.TableName = semantics.QualifiedTableIdentityKey(ref.TableName)
	return ref
}

// ConflictSet indexes table-qualified references using the target dialect's
// index namespace. It supports constant-time exact conflict checks.
type ConflictSet struct {
	semantics identifier.Semantics
	matches   map[namespaceKey][]difftypes.IndexRef
}

// NewConflictSet builds a dialect-aware conflict index for refs.
func NewConflictSet(dialect string, refs []difftypes.IndexRef) *ConflictSet {
	return NewConflictSetWithSemantics(identifier.ForDialect(dialect), refs)
}

// NewConflictSetWithSemantics builds an index of confirmed and potential
// identifier collisions under semantics.
func NewConflictSetWithSemantics(
	semantics identifier.Semantics,
	refs []difftypes.IndexRef,
) *ConflictSet {
	set := &ConflictSet{
		semantics: semantics,
		matches:   make(map[namespaceKey][]difftypes.IndexRef, len(refs)),
	}
	for _, ref := range refs {
		set.add(ref)
	}
	return set
}

// Contains reports whether ref conflicts with an indexed reference.
func (s *ConflictSet) Contains(ref difftypes.IndexRef) bool {
	if s == nil {
		return false
	}
	key := conflictKey(s.semantics, ref)
	return len(s.matches[key]) > 0
}

// Matches returns conflicting references. Validated diff references retain
// their original input order. The sequence is allocation-free.
func (s *ConflictSet) Matches(ref difftypes.IndexRef) iter.Seq[difftypes.IndexRef] {
	return func(yield func(difftypes.IndexRef) bool) {
		if s == nil {
			return
		}
		key := conflictKey(s.semantics, ref)
		yieldRefs(s.matches[key], yield)
	}
}

func (s *ConflictSet) add(ref difftypes.IndexRef) {
	key := conflictKey(s.semantics, ref)
	s.matches[key] = append(s.matches[key], ref)
}

func (s *ConflictSet) firstMatch(ref difftypes.IndexRef) (difftypes.IndexRef, bool) {
	for match := range s.Matches(ref) {
		return match, true
	}
	return difftypes.IndexRef{}, false
}

func yieldRefs(refs []difftypes.IndexRef, yield func(difftypes.IndexRef) bool) bool {
	for _, ref := range refs {
		if !yield(ref) {
			return false
		}
	}
	return true
}

func conflictKey(semantics identifier.Semantics, ref difftypes.IndexRef) namespaceKey {
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
	refs []difftypes.IndexRef,
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

func validateRef(operation string, position int, ref difftypes.IndexRef) error {
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
	ref difftypes.IndexRef,
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

func conflictError(dialect, operation string, previous, ref difftypes.IndexRef) error {
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

// validateAdditionsAreDescribed refuses an addition that names an index without
// saying what it is.
//
// The resolver this replaced answered the same refusal by failing to find the
// name in the declaration it was handed. An addition carries its declaration
// now, so the question is asked of the addition -- and the answer has to stay a
// refusal: a CREATE INDEX with neither a column list nor an expression is not
// SQL, and emitting one would turn a diff nobody could plan into a migration
// that fails on the server instead (stokaro/ptah#2315).
//
// Fields and Parts are both consulted because an expression index carries its
// elements in Parts alone.
func validateAdditionsAreDescribed(changes difftypes.IndexChanges) error {
	for _, change := range changes {
		if len(change.Index.Fields) > 0 || len(change.Index.Parts) > 0 {
			continue
		}
		return fmt.Errorf(
			"%w: added index %s.%s is not described by the diff; "+
				"an addition has to carry the index it creates",
			ptaherr.ErrInvalidSchemaDiff,
			change.TableName,
			change.Index.Name,
		)
	}
	return nil
}
