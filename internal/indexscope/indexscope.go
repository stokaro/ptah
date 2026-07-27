// Package indexscope compares index names using database namespace semantics.
package indexscope

import (
	"fmt"
	"iter"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// Resolver contains the validated target indexes needed by one migration plan.
// Index lookup is constant time and uses the canonical table-qualified identity.
type Resolver struct {
	dialect string
	indexes map[types.IndexRef]goschema.Index
}

// Resolve returns the target index identified by ref.
func (r *Resolver) Resolve(ref types.IndexRef) (goschema.Index, error) {
	if r == nil {
		return goschema.Index{}, fmt.Errorf(
			"%w: no validated target indexes are available",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	index, ok := r.indexes[IdentityKey(r.dialect, ref)]
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

func validateDiff(dialect string, diff *types.SchemaDiff) error {
	if diff == nil {
		return nil
	}
	if err := validateRefs(dialect, "added", diff.IndexesAdded); err != nil {
		return err
	}
	return validateRefs(dialect, "removed", diff.IndexesRemoved)
}

// NewResolver validates every index identity needed to plan diff and indexes
// the target schema for constant-time addition lookup. Removals are
// self-contained because their owning table is part of IndexRef.
func NewResolver(
	dialect string,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) (*Resolver, error) {
	if diff == nil {
		return nil, fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := validateDiff(dialect, diff); err != nil {
		return nil, err
	}
	resolver, err := newTargetResolver(dialect, generated)
	if err != nil {
		return nil, err
	}
	if len(diff.IndexesAdded) == 0 {
		return resolver, nil
	}
	for index, ref := range diff.IndexesAdded {
		if _, exists := resolver.indexes[IdentityKey(dialect, ref)]; !exists {
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

func newTargetResolver(dialect string, generated *goschema.Database) (*Resolver, error) {
	resolver := &Resolver{
		dialect: dialect,
		indexes: make(map[types.IndexRef]goschema.Index),
	}
	if generated == nil {
		return resolver, nil
	}
	tracker := NewConflictSet(dialect, nil)
	tableNames := goschema.ResolveIndexTableNames(generated.Indexes, generated.Tables)
	for position, index := range generated.Indexes {
		ref := types.IndexRef{
			Name:      index.Name,
			TableName: tableNames[position],
		}
		if err := validateRef("target", position, ref); err != nil {
			return nil, err
		}
		if previous, conflict := tracker.firstMatch(ref); conflict {
			return nil, conflictError(dialect, "target", previous, ref)
		}
		tracker.add(ref)
		resolver.indexes[IdentityKey(dialect, ref)] = index
	}
	return resolver, nil
}

func schemaScopedName(ref types.IndexRef, defaultSchema string) string {
	schema := defaultSchema
	if tableSchema, _, qualified := strings.Cut(ref.TableName, "."); qualified {
		schema = tableSchema
	}
	return schema
}

// IdentityKey returns the dialect-aware comparison identity for ref. It is
// intended only for map and set keys; callers must keep the original ref when
// rendering SQL so the declared identifier spelling is preserved.
func IdentityKey(dialect string, ref types.IndexRef) types.IndexRef {
	return scopeForDialect(dialect).identityKey(ref)
}

// ConflictSet indexes table-qualified references using the target dialect's
// index namespace. It supports constant-time exact conflict checks.
type ConflictSet struct {
	scope dialectScope
	exact map[namespaceKey][]types.IndexRef
}

// NewConflictSet builds a dialect-aware conflict index for refs.
func NewConflictSet(dialect string, refs []types.IndexRef) *ConflictSet {
	scope := scopeForDialect(dialect)
	set := &ConflictSet{
		scope: scope,
		exact: make(map[namespaceKey][]types.IndexRef, len(refs)),
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
	key := s.scope.key(ref)
	return len(s.exact[key]) > 0
}

// Matches returns conflicting references. Validated diff references retain
// their original input order. The sequence is allocation-free.
func (s *ConflictSet) Matches(ref types.IndexRef) iter.Seq[types.IndexRef] {
	return func(yield func(types.IndexRef) bool) {
		if s == nil {
			return
		}
		key := s.scope.key(ref)
		yieldRefs(s.exact[key], yield)
	}
}

func (s *ConflictSet) add(ref types.IndexRef) {
	key := s.scope.key(ref)
	s.exact[key] = append(s.exact[key], ref)
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

type dialectScope struct {
	defaultSchema string
	schemaScoped  bool
	nameFolding   identifierFolding
	tableFolding  identifierFolding
}

type identifierFolding uint8

const (
	foldingExact identifierFolding = iota
	foldingASCII
	foldingUnicodeLower
)

func scopeForDialect(dialect string) dialectScope {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.YugabyteDB, platform.Spanner:
		return dialectScope{
			defaultSchema: "public",
			schemaScoped:  true,
		}
	case platform.SQLite:
		// SQLite object identity is ASCII case-insensitive. Non-ASCII
		// identifiers that differ only by Unicode case remain distinct.
		return dialectScope{
			defaultSchema: "main",
			schemaScoped:  true,
			nameFolding:   foldingASCII,
			tableFolding:  foldingASCII,
		}
	case platform.MySQL:
		// Index names are ASCII case-insensitive, while table-name case
		// semantics depend on lower_case_table_names and the host filesystem.
		return dialectScope{nameFolding: foldingASCII}
	case platform.MariaDB:
		// MariaDB applies Unicode lowercase equivalence to index names while
		// table-name case remains server and filesystem dependent.
		return dialectScope{nameFolding: foldingUnicodeLower}
	default:
		return dialectScope{}
	}
}

func (s dialectScope) identityKey(ref types.IndexRef) types.IndexRef {
	ref.Name = foldIdentifier(ref.Name, s.nameFolding)
	ref.TableName = foldIdentifier(ref.TableName, s.tableFolding)
	return ref
}

func foldIdentifier(value string, folding identifierFolding) string {
	switch folding {
	case foldingASCII:
		return foldASCII(value)
	case foldingUnicodeLower:
		return strings.ToLower(value)
	default:
		return value
	}
}

func foldASCII(value string) string {
	for index := range len(value) {
		if value[index] < 'A' || value[index] > 'Z' {
			continue
		}
		folded := []byte(value)
		for position := index; position < len(folded); position++ {
			if folded[position] >= 'A' && folded[position] <= 'Z' {
				folded[position] += 'a' - 'A'
			}
		}
		return string(folded)
	}
	return value
}

func (s dialectScope) key(ref types.IndexRef) namespaceKey {
	ref = s.identityKey(ref)
	namespace := ref.TableName
	if s.schemaScoped {
		namespace = schemaScopedName(ref, s.defaultSchema)
	}
	return namespaceKey{namespace: namespace, name: ref.Name}
}

func validateRefs(dialect, operation string, refs []types.IndexRef) error {
	tracker := NewConflictSet(dialect, nil)
	for index, ref := range refs {
		if err := validateRef(operation, index, ref); err != nil {
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
