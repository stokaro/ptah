// Package indexscope compares index names using database namespace semantics.
package indexscope

import (
	"fmt"
	"iter"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
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
	if err := validateRefs(dialect, semantics, "removed", diff.IndexesRemoved); err != nil {
		return err
	}
	renamedFrom := make([]difftypes.IndexRef, 0, len(diff.IndexesRenamed))
	renamedTo := make([]difftypes.IndexRef, 0, len(diff.IndexesRenamed))
	for _, rename := range diff.IndexesRenamed {
		renamedFrom = append(renamedFrom, difftypes.IndexRef{Name: rename.From, TableName: rename.TableName})
		renamedTo = append(renamedTo, difftypes.IndexRef{Name: rename.To, TableName: rename.TableName})
	}
	if err := validateRefs(dialect, semantics, "renamed", renamedFrom); err != nil {
		return err
	}
	if err := validateRefs(dialect, semantics, "rename target", renamedTo); err != nil {
		return err
	}
	repartitioned := make([]difftypes.IndexRef, 0, len(diff.IndexPartitioningChanged))
	for _, change := range diff.IndexPartitioningChanged {
		repartitioned = append(repartitioned, difftypes.IndexRef{Name: change.Name, TableName: change.TableName})
	}
	if err := validateRefs(dialect, semantics, "repartitioned", repartitioned); err != nil {
		return err
	}
	commented := make([]difftypes.IndexRef, 0, len(diff.IndexCommentsChanged))
	for _, change := range diff.IndexCommentsChanged {
		commented = append(commented, difftypes.IndexRef{Name: change.Name, TableName: change.TableName})
	}
	return validateRefs(dialect, semantics, "commented", commented)
}

// ValidateDiff refuses an index reference a plan could not act on: an empty
// name or table, or a pair two dialect-folded spellings collapse onto.
//
// It resolves nothing. An addition carries its own definition
// (stokaro/ptah#2315), so what is left here is the identity check alone.
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
	// namespaces holds every reference by namespace, and unresolved holds the
	// ones whose equivalence class the comparison cannot compute. A name in
	// the second set collides with everything in its namespace rather than
	// with the other members of its own bucket, which is what an unknown
	// folding rule actually means (stokaro/ptah#2768).
	namespaces map[string][]difftypes.IndexRef
	unresolved map[string][]difftypes.IndexRef
	// unresolvedTables holds the references whose table cannot be told apart
	// from another offline, and all holds every reference. An index on such a
	// table may share a namespace with an index on any table, so it is compared
	// against all of them; see [ConflictSemantics].
	unresolvedTables []difftypes.IndexRef
	all              []difftypes.IndexRef
	// splitTables records that table names are placed by [ConflictSemantics]
	// rather than by the target's own comparison, which happens only when that
	// comparison knows nothing about the target's collation.
	splitTables bool
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
		semantics:   ConflictSemantics(semantics),
		splitTables: semantics.TableNames == identifier.ComparisonCatalogUnknown,
		matches:     make(map[namespaceKey][]difftypes.IndexRef, len(refs)),
		namespaces:  make(map[string][]difftypes.IndexRef, len(refs)),
		unresolved:  make(map[string][]difftypes.IndexRef),
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
	for range s.Matches(ref) {
		return true
	}
	return false
}

// Matches returns conflicting references. Validated diff references retain
// their original input order. The sequence is allocation-free.
func (s *ConflictSet) Matches(ref difftypes.IndexRef) iter.Seq[difftypes.IndexRef] {
	return func(yield func(difftypes.IndexRef) bool) {
		if s == nil {
			return
		}
		key := conflictKey(s.semantics, ref)
		// A table that cannot be told apart from another may be any of them,
		// so its index is compared against every recorded one whose name may
		// collide with it.
		if s.splitTables && !tableResolved(key) {
			yieldNameMatches(s.semantics, s.all, ref, yield)
			return
		}
		// A name whose equivalence class is unknown may be any name in its
		// namespace, so it is compared against all of them rather than against
		// the bucket it happens to hash into.
		if s.unresolvedName(ref) {
			if !yieldRefs(s.namespaces[key.namespace], yield) {
				return
			}
			yieldNameMatches(s.semantics, s.unresolvedTables, ref, yield)
			return
		}
		if !yieldRefs(s.matches[key], yield) {
			return
		}
		// The reverse direction, which is the one a per-value rule misses: an
		// ASCII name collides with an already-recorded unresolved name in the
		// same namespace. Measured, MySQL refuses `İ` beside ASCII `i`.
		if !yieldRefs(s.unresolved[key.namespace], yield) {
			return
		}
		// The same reverse direction for tables: an index on a known table may
		// share a namespace with one on a table recorded as unresolved.
		yieldNameMatches(s.semantics, s.unresolvedTables, ref, yield)
	}
}

func (s *ConflictSet) add(ref difftypes.IndexRef) {
	key := conflictKey(s.semantics, ref)
	s.all = append(s.all, ref)
	if s.splitTables && !tableResolved(key) {
		s.unresolvedTables = append(s.unresolvedTables, ref)
		return
	}
	s.matches[key] = append(s.matches[key], ref)
	s.namespaces[key.namespace] = append(s.namespaces[key.namespace], ref)
	if s.unresolvedName(ref) {
		s.unresolved[key.namespace] = append(s.unresolved[key.namespace], ref)
	}
}

// yieldNameMatches yields the references in refs whose index name may collide
// with ref's: the same conflict key, or a name either side cannot resolve.
func yieldNameMatches(
	semantics identifier.Semantics,
	refs []difftypes.IndexRef,
	ref difftypes.IndexRef,
	yield func(difftypes.IndexRef) bool,
) bool {
	name := semantics.IndexConflictKey(ref.Name)
	unresolved := semantics.IndexConflictUnresolved(ref.Name)
	for _, other := range refs {
		if !unresolved && !semantics.IndexConflictUnresolved(other.Name) &&
			semantics.IndexConflictKey(other.Name) != name {
			continue
		}
		if !yield(other) {
			return false
		}
	}
	return true
}

// unresolvedName reports whether this reference's name has an equivalence
// class the target's comparison cannot compute offline.
func (s *ConflictSet) unresolvedName(ref difftypes.IndexRef) bool {
	return s.semantics.IndexConflictUnresolved(ref.Name)
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
		message := fmt.Sprintf(
			"%s: %s index reference at position %d requires a name and owning table",
			ptaherr.ErrInvalidSchemaDiff,
			operation,
			position,
		)
		return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
			Code: schemavalidation.InvalidSchema, Kind: "index", Object: ref.Name, Message: message,
		}}}).Err("")
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

// unresolvedConflictKey is the key a comparison gives a name it cannot place;
// see [identifier.Comparison.ConflictKey].
var unresolvedConflictKey = identifier.ComparisonCatalogUnknown.ConflictKey("")

// ConflictSemantics returns the semantics a conflict check compares table,
// column and index names with. A comparison that knows nothing about the
// target's collation places every name in one class, so every name conflicts
// with every other: an offline SQL Server plan adding orders_user_ix on orders
// and users_created_ix on users is refused as a namespace conflict
// (stokaro/ptah#4111), and so is every table with two columns
// (stokaro/ptah#4122).
//
// Two ASCII names that differ after ASCII case folding are different under
// every collation SQL Server offers, so they are kept apart. A name with a
// non-ASCII character still has no class: an accent-insensitive collation makes
// `résumé` the same index as `resume`, and `ördérs` the same table as `orders`,
// so such a name is compared against every other.
//
// The index conflict set here and the table and column check in
// migration/internal/identifiervalidation both call it. One rule for both is
// what keeps an offline plan from accepting a table the comparison before it
// refuses, or the other way round.
func ConflictSemantics(semantics identifier.Semantics) identifier.Semantics {
	if semantics.TableNames == identifier.ComparisonCatalogUnknown {
		semantics.TableNames = identifier.ComparisonASCIIFoldedNonASCIIUnknown
	}
	if semantics.ColumnNames == identifier.ComparisonCatalogUnknown {
		semantics.ColumnNames = identifier.ComparisonASCIIFoldedNonASCIIUnknown
	}
	if semantics.IndexNames == identifier.ComparisonCatalogUnknown {
		semantics.IndexNames = identifier.ComparisonASCIIFoldedNonASCIIUnknown
	}
	return semantics
}

// tableResolved reports whether key's table part names one class of tables.
func tableResolved(key namespaceKey) bool {
	return !strings.Contains(key.namespace, unresolvedConflictKey)
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
		message := fmt.Sprintf(
			"%s: added index %s.%s is not described by the diff; "+
				"an addition has to carry the index it creates",
			ptaherr.ErrInvalidSchemaDiff,
			change.TableName,
			change.Index.Name,
		)
		return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
			Code: schemavalidation.InvalidSchema, Kind: "index", Object: change.Index.Name, Message: message,
		}}}).Err("")
	}
	return nil
}
