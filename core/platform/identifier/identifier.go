// Package identifier describes database identifier comparison semantics.
package identifier

import (
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/tableref"
)

// Comparison describes how a database compares identifiers.
type Comparison string

const (
	// ComparisonExact preserves identifier spelling during comparison.
	ComparisonExact Comparison = "exact"
	// ComparisonASCIIInsensitive folds ASCII letters before comparison.
	ComparisonASCIIInsensitive Comparison = "ascii_case_insensitive"
	// ComparisonUnicodeInsensitive folds Unicode letters before comparison.
	ComparisonUnicodeInsensitive Comparison = "unicode_case_insensitive"
	// ComparisonCatalogUnknown preserves spelling for identity while treating
	// every distinct unresolved name as a potential catalog conflict.
	ComparisonCatalogUnknown Comparison = "catalog_unknown"
	// ComparisonCatalogResolved uses equivalence keys resolved by the target
	// database under its catalog collation.
	ComparisonCatalogResolved Comparison = "catalog_resolved"
)

// IndexNamespace describes where index names must be unique.
type IndexNamespace string

const (
	// IndexNamespaceTable scopes index names to their owning table.
	IndexNamespaceTable IndexNamespace = "table"
	// IndexNamespaceSchema scopes index names to their owning schema.
	IndexNamespaceSchema IndexNamespace = "schema"
)

// Semantics contains the identifier rules needed by schema comparison and
// migration planning. CatalogCollation is diagnostic metadata; the comparison
// fields are the authoritative rules.
type Semantics struct {
	DefaultSchema    string         `json:"default_schema,omitempty"`
	IndexNamespace   IndexNamespace `json:"index_namespace,omitempty"`
	IndexNames       Comparison     `json:"index_names,omitempty"`
	TableNames       Comparison     `json:"table_names,omitempty"`
	ColumnNames      Comparison     `json:"column_names,omitempty"`
	CatalogCollation string         `json:"catalog_collation,omitempty"`
	ResolvedNames    []ResolvedName `json:"resolved_names,omitempty"`
}

// ResolvedName maps one raw identifier to a deterministic catalog-equivalence
// key. Names with the same key compare equal in the target database.
type ResolvedName struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// ForDialect returns conservative offline identifier semantics for dialect.
func ForDialect(dialect string) Semantics {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.YugabyteDB, platform.Spanner:
		return Semantics{
			DefaultSchema:  "public",
			IndexNamespace: IndexNamespaceSchema,
			IndexNames:     ComparisonExact,
			TableNames:     ComparisonExact,
			ColumnNames:    ComparisonExact,
		}
	case platform.SQLite:
		return Semantics{
			DefaultSchema:  "main",
			IndexNamespace: IndexNamespaceSchema,
			IndexNames:     ComparisonASCIIInsensitive,
			TableNames:     ComparisonASCIIInsensitive,
			ColumnNames:    ComparisonASCIIInsensitive,
		}
	case platform.MySQL:
		return Semantics{
			IndexNamespace: IndexNamespaceTable,
			IndexNames:     ComparisonASCIIInsensitive,
			TableNames:     ComparisonExact,
			ColumnNames:    ComparisonExact,
		}
	case platform.MariaDB:
		return Semantics{
			IndexNamespace: IndexNamespaceTable,
			IndexNames:     ComparisonUnicodeInsensitive,
			TableNames:     ComparisonExact,
			ColumnNames:    ComparisonExact,
		}
	case platform.SQLServer:
		return Semantics{
			DefaultSchema:  "dbo",
			IndexNamespace: IndexNamespaceTable,
			IndexNames:     ComparisonCatalogUnknown,
			TableNames:     ComparisonCatalogUnknown,
			ColumnNames:    ComparisonCatalogUnknown,
		}
	default:
		return Semantics{
			IndexNamespace: IndexNamespaceTable,
			IndexNames:     ComparisonExact,
			TableNames:     ComparisonExact,
			ColumnNames:    ComparisonExact,
		}
	}
}

// ForSQLServerCatalog returns SQL Server catalog identifier semantics.
// Call WithResolvedNames before comparing or planning names against a live
// catalog. Unresolved names deliberately share one conservative conflict key.
func ForSQLServerCatalog(catalogCollation string) Semantics {
	return Semantics{
		DefaultSchema:    "dbo",
		IndexNamespace:   IndexNamespaceTable,
		IndexNames:       ComparisonCatalogResolved,
		TableNames:       ComparisonCatalogResolved,
		ColumnNames:      ComparisonCatalogResolved,
		CatalogCollation: catalogCollation,
	}
}

// WithResolvedNames returns a copy with deterministic catalog-equivalence
// mappings. The input may be in any order but must contain at most one mapping
// per raw name; invalid mappings make Normalize fall back conservatively.
func (s Semantics) WithResolvedNames(names []ResolvedName) Semantics {
	s.ResolvedNames = slices.Clone(names)
	sort.Slice(s.ResolvedNames, func(i, j int) bool {
		return s.ResolvedNames[i].Name < s.ResolvedNames[j].Name
	})
	return s
}

// Clone returns a deep copy.
func (s Semantics) Clone() Semantics {
	s.ResolvedNames = slices.Clone(s.ResolvedNames)
	return s
}

// IsZero reports whether no identifier rules are present.
func (s Semantics) IsZero() bool {
	return s.DefaultSchema == "" &&
		s.IndexNamespace == "" &&
		s.IndexNames == "" &&
		s.TableNames == "" &&
		s.ColumnNames == "" &&
		s.CatalogCollation == "" &&
		len(s.ResolvedNames) == 0
}

// Equal reports whether two semantics values carry the same rules and resolved
// catalog equivalence classes. CatalogCollation is diagnostic and does not
// participate in equality.
func (s Semantics) Equal(other Semantics) bool {
	return s.DefaultSchema == other.DefaultSchema &&
		s.IndexNamespace == other.IndexNamespace &&
		s.IndexNames == other.IndexNames &&
		s.TableNames == other.TableNames &&
		s.ColumnNames == other.ColumnNames &&
		slices.Equal(s.ResolvedNames, other.ResolvedNames)
}

// Normalize returns s when it is complete and internally consistent. Zero,
// partial, or invalid public values fall back to conservative dialect rules.
func (s Semantics) Normalize(dialect string) Semantics {
	fallback := ForDialect(dialect)
	if !s.valid() {
		return fallback
	}
	return s.Clone()
}

// IndexIdentityKey returns the confirmed comparison key for an index name.
func (s Semantics) IndexIdentityKey(value string) string {
	return s.identityKey(s.IndexNames, value)
}

// IndexConflictKey returns the conservative collision key for an index name.
func (s Semantics) IndexConflictKey(value string) string {
	return s.conflictKey(s.IndexNames, value)
}

// TableIdentityKey returns the confirmed comparison key for a schema or table
// name.
func (s Semantics) TableIdentityKey(value string) string {
	return s.identityKey(s.TableNames, value)
}

// TableConflictKey returns the conservative collision key for a schema or table
// name.
func (s Semantics) TableConflictKey(value string) string {
	return s.conflictKey(s.TableNames, value)
}

// QualifiedTableIdentityKey returns the confirmed comparison key for a
// schema-qualified table name. Unqualified names use DefaultSchema.
func (s Semantics) QualifiedTableIdentityKey(value string) string {
	return s.qualifiedTableKey(value, s.TableIdentityKey)
}

// QualifiedTableConflictKey returns the conservative collision key for a
// schema-qualified table name. Unqualified names use DefaultSchema.
func (s Semantics) QualifiedTableConflictKey(value string) string {
	return s.qualifiedTableKey(value, s.TableConflictKey)
}

// ColumnIdentityKey returns the confirmed comparison key for a column name.
func (s Semantics) ColumnIdentityKey(value string) string {
	return s.identityKey(s.ColumnNames, value)
}

// ColumnConflictKey returns the conservative collision key for a column name.
func (s Semantics) ColumnConflictKey(value string) string {
	return s.conflictKey(s.ColumnNames, value)
}

// Resolves reports whether value is covered when catalog-resolved comparison
// is active. Static comparison modes resolve every value.
func (s Semantics) Resolves(value string) bool {
	if s.IndexNames != ComparisonCatalogResolved &&
		s.TableNames != ComparisonCatalogResolved &&
		s.ColumnNames != ComparisonCatalogResolved {
		return true
	}
	_, found := slices.BinarySearchFunc(
		s.ResolvedNames,
		value,
		func(resolved ResolvedName, target string) int {
			return strings.Compare(resolved.Name, target)
		},
	)
	return found
}

// ResolvesQualifiedTable reports whether both parts of a table identifier are
// covered by catalog-resolved semantics. Unqualified names use DefaultSchema.
func (s Semantics) ResolvesQualifiedTable(value string) bool {
	ref, ok := tableref.Parse(value)
	if !ok {
		return false
	}
	schema := ref.Schema
	if !ref.Qualified {
		schema = s.DefaultSchema
	}
	return s.Resolves(schema) && s.Resolves(ref.Name)
}

// IdentityKey returns the canonical key used for confirmed identifier equality.
func (c Comparison) IdentityKey(value string) string {
	switch c {
	case ComparisonASCIIInsensitive:
		return foldASCII(value)
	case ComparisonUnicodeInsensitive:
		return strings.ToLower(value)
	case ComparisonCatalogResolved:
		return unresolvedCatalogKey
	default:
		return value
	}
}

// ConflictKey returns the canonical key used to detect identifiers that may
// collide in the target database.
func (c Comparison) ConflictKey(value string) string {
	switch c {
	case ComparisonCatalogUnknown:
		return unresolvedCatalogKey
	case ComparisonCatalogResolved:
		return unresolvedCatalogKey
	default:
		return c.IdentityKey(value)
	}
}

const unresolvedCatalogKey = "\x00ptah:unresolved-catalog-identifier"

func (s Semantics) identityKey(comparison Comparison, value string) string {
	if comparison != ComparisonCatalogResolved {
		return comparison.IdentityKey(value)
	}
	position, found := slices.BinarySearchFunc(
		s.ResolvedNames,
		value,
		func(resolved ResolvedName, target string) int {
			return strings.Compare(resolved.Name, target)
		},
	)
	if !found {
		return unresolvedCatalogKey
	}
	return s.ResolvedNames[position].Key
}

func (s Semantics) conflictKey(comparison Comparison, value string) string {
	if comparison == ComparisonCatalogResolved {
		return s.identityKey(comparison, value)
	}
	return comparison.ConflictKey(value)
}

func (s Semantics) qualifiedTableKey(
	value string,
	key func(string) string,
) string {
	ref, ok := tableref.Parse(value)
	if !ok {
		return key(value)
	}
	schema := ref.Schema
	if !ref.Qualified {
		if s.DefaultSchema == "" {
			return tableref.Canonical("", key(ref.Name))
		}
		schema = s.DefaultSchema
	}
	return tableref.Canonical(key(schema), key(ref.Name))
}

func (s Semantics) valid() bool {
	if s.IndexNamespace != IndexNamespaceTable &&
		s.IndexNamespace != IndexNamespaceSchema {
		return false
	}
	if !validComparison(s.IndexNames) ||
		!validComparison(s.TableNames) ||
		!validComparison(s.ColumnNames) {
		return false
	}
	catalogResolved := s.IndexNames == ComparisonCatalogResolved ||
		s.TableNames == ComparisonCatalogResolved ||
		s.ColumnNames == ComparisonCatalogResolved
	if catalogResolved != (s.CatalogCollation != "") {
		return false
	}
	if catalogResolved &&
		(s.IndexNames != ComparisonCatalogResolved ||
			s.TableNames != ComparisonCatalogResolved ||
			s.ColumnNames != ComparisonCatalogResolved) {
		return false
	}
	if catalogResolved && len(s.ResolvedNames) == 0 {
		return false
	}
	if !catalogResolved && len(s.ResolvedNames) > 0 {
		return false
	}
	return validResolvedNames(s.ResolvedNames)
}

func validResolvedNames(names []ResolvedName) bool {
	nameSet := make(map[string]struct{}, len(names))
	classMinimums := make(map[string]string, len(names))
	for position, resolved := range names {
		if resolved.Name == "" || resolved.Key == "" {
			return false
		}
		if position > 0 && names[position-1].Name >= resolved.Name {
			return false
		}
		nameSet[resolved.Name] = struct{}{}
		minimum, exists := classMinimums[resolved.Key]
		if !exists || resolved.Name < minimum {
			classMinimums[resolved.Key] = resolved.Name
		}
	}
	for _, resolved := range names {
		if _, exists := nameSet[resolved.Key]; !exists {
			return false
		}
		if classMinimums[resolved.Key] != resolved.Key {
			return false
		}
	}
	return true
}

func validComparison(comparison Comparison) bool {
	switch comparison {
	case ComparisonExact,
		ComparisonASCIIInsensitive,
		ComparisonUnicodeInsensitive,
		ComparisonCatalogUnknown,
		ComparisonCatalogResolved:
		return true
	default:
		return false
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
