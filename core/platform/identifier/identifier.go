// Package identifier models how each database compares identifiers: schema,
// table, column, and index names. Schema comparison and migration planning
// consume it to decide whether two spellings name the same object and whether
// two distinct names could collide in the target.
//
// Every comparison rule yields two keys per name. An identity key answers
// "are these the same object": names with equal identity keys are confirmed
// equal under the dialect's rules. A conflict key answers "could these
// collide in the target": the conservative question, so names whose
// equivalence class is unknown share one conflict key while keeping distinct
// identity keys. The split matters most under [ComparisonCatalogResolved],
// where a name the live catalog did not resolve stays a distinct object
// without being promised unique.
//
// [ForDialect] returns conservative offline defaults for a dialect.
// [ForSQLServerCatalog] with [Semantics.WithResolvedNames] carries the
// equivalence classes a live SQL Server catalog resolved under its collation.
// [Semantics] round-trips through JSON; [Semantics.Normalize] is the safety
// valve for deserialized values, falling back to [ForDialect] rules for
// anything incomplete or internally inconsistent.
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
	// ComparisonASCIIFoldedNonASCIIUnknown folds ASCII letters for identity and
	// treats any name carrying a non-ASCII rune as an unresolved catalog
	// identifier for conflict purposes.
	//
	// It exists because the MySQL family folds identifiers with a collation
	// table Ptah has no offline copy of, and the two engines disagree. Measured
	// on mysql:8.4.11 and mariadb:11.8.9, on the same four index-name pairs:
	// MySQL refuses `İ`/`i` and `K`(U+212A)/`K` as duplicates while accepting
	// `I`/`ı` and `Σ`/`ς`; MariaDB does exactly the opposite. So neither ASCII
	// folding nor Unicode folding is right for either engine, and the honest
	// offline answer is that a non-ASCII name's equivalence class is unknown
	// (stokaro/ptah#2768).
	ComparisonASCIIFoldedNonASCIIUnknown Comparison = "ascii_folded_non_ascii_unknown"
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
// The argument is folded through [platform.NormalizeDialect], so every
// accepted spelling of a dialect name selects the same rules. A dialect Ptah
// does not model still receives usable rules rather than a zero value:
// comparison that never treats two different spellings as one object, and no
// default schema. Those are a fallback for an unmodelled target rather than a
// description of one, so a caller who knows the target's rules should build
// [Semantics] directly.
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
	// Both engines fold index names with a collation table Ptah has no offline
	// copy of, and they disagree about every non-ASCII pair measured. ASCII
	// folding is shared and exact, so it is kept; beyond ASCII the equivalence
	// class is unknown and is reported as a possible conflict rather than
	// guessed at in either direction (stokaro/ptah#2768).
	case platform.MySQL:
		return Semantics{
			IndexNamespace: IndexNamespaceTable,
			IndexNames:     ComparisonASCIIFoldedNonASCIIUnknown,
			TableNames:     ComparisonExact,
			ColumnNames:    ComparisonExact,
		}
	case platform.MariaDB:
		return Semantics{
			IndexNamespace: IndexNamespaceTable,
			IndexNames:     ComparisonASCIIFoldedNonASCIIUnknown,
			TableNames:     ComparisonExact,
			ColumnNames:    ComparisonExact,
		}
	case platform.Oracle:
		// Case-insensitive because the renderer writes a plain name bare and
		// Oracle folds it to upper case: a declared `view_count` is stored as
		// VIEW_COUNT, and comparing exactly would report a difference on every
		// run against a catalog Ptah itself wrote. Measured -- user_tables
		// reports PTAH_FOLD for a bare CREATE and ptah_fold for a quoted one,
		// and the two coexist as separate tables.
		//
		// Index names are schema-scoped, not table-scoped: `DROP INDEX <name>`
		// is the whole statement here, with no ON clause naming a table, so
		// two tables in one schema cannot both carry an index called `idx_name`
		// (stokaro/ptah#1875).
		return Semantics{
			IndexNamespace: IndexNamespaceSchema,
			IndexNames:     ComparisonASCIIInsensitive,
			TableNames:     ComparisonASCIIInsensitive,
			ColumnNames:    ComparisonASCIIInsensitive,
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
// Without resolved names the value does not survive [Semantics.Normalize],
// which falls back to the offline [ForDialect] rules instead.
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
// mappings, sorted by raw name; the input may arrive in any order. The sort
// is part of the value: lookups rely on it, and two semantics built from the
// same mappings in different orders compare [Semantics.Equal].
//
// For [Semantics.Normalize] to keep them, the mappings must form canonical
// equivalence classes: every Name and Key non-empty, Names strictly ascending
// and therefore unique, every Key itself present as a Name, and each Key equal
// to the byte-wise smallest Name in its class. Anything else makes Normalize
// fall back to the conservative [ForDialect] rules.
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

// Normalize returns a deep copy of s when it is complete and internally
// consistent, and the conservative [ForDialect] rules for dialect otherwise.
// Complete means: IndexNamespace and all three comparison fields hold
// declared values; catalog-resolved comparison is all-or-nothing across the
// three name kinds and requires both a CatalogCollation and at least one
// resolved name, while every other mode carries neither; and ResolvedNames
// satisfy the validity rules on [Semantics.WithResolvedNames]. Zero and
// partial values therefore fall back too, which is the safety valve for a
// [Semantics] deserialized from JSON.
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

// IndexConflictUnresolved reports whether an index name's conflict key stands
// for an equivalence class this target cannot resolve.
//
// A caller grouping names by conflict key needs this, because such a key is
// not one more bucket: it may collide with every other name in the namespace,
// ASCII ones included. It is asked of the [Semantics] rather than of the
// [Comparison] on purpose -- a catalog-resolved target answers false for a
// name its ResolvedNames cover, and asking the bare comparison would report
// every name unresolved and collapse a resolved namespace into one bucket
// (stokaro/ptah#2768).
func (s Semantics) IndexConflictUnresolved(value string) bool {
	return s.IndexConflictKey(value) == unresolvedCatalogKey
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

// IdentityKey returns the canonical key used for confirmed identifier
// equality: two values with equal keys are the same object under this
// comparison. A bare Comparison holds no resolved-name table, so under
// [ComparisonCatalogResolved] every value maps to one shared unresolved key
// and compares identity-equal to every other. Callers holding a [Semantics]
// must use its key methods -- [Semantics.TableIdentityKey] and its index and
// column siblings -- which consult ResolvedNames and keep an unresolved
// name's spelling as its identity (stokaro/ptah#1290).
func (c Comparison) IdentityKey(value string) string {
	switch c {
	case ComparisonASCIIInsensitive, ComparisonASCIIFoldedNonASCIIUnknown:
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
// collide in the target database. Under [ComparisonCatalogUnknown] and
// [ComparisonCatalogResolved] every value shares one key -- the conservative
// answer when nothing is known about the target's equivalence classes; the
// other modes collide exactly where their identity keys agree.
func (c Comparison) ConflictKey(value string) string {
	switch c {
	case ComparisonCatalogUnknown:
		return unresolvedCatalogKey
	case ComparisonCatalogResolved:
		return unresolvedCatalogKey
	case ComparisonASCIIFoldedNonASCIIUnknown:
		if isASCII(value) {
			return foldASCII(value)
		}
		return unresolvedCatalogKey
	default:
		return c.IdentityKey(value)
	}
}

// isASCII reports whether every byte is ASCII, which for UTF-8 is the same
// question as whether every rune is.
func isASCII(value string) bool {
	for index := range len(value) {
		if value[index] >= 0x80 {
			return false
		}
	}
	return true
}

const unresolvedCatalogKey = "\x00ptah:unresolved-catalog-identifier"

// resolvedKey returns the target's equivalence key for a name and whether the
// catalog resolved it at all.
func (s Semantics) resolvedKey(value string) (string, bool) {
	position, found := slices.BinarySearchFunc(
		s.ResolvedNames,
		value,
		func(resolved ResolvedName, target string) int {
			return strings.Compare(resolved.Name, target)
		},
	)
	if !found {
		return "", false
	}
	return s.ResolvedNames[position].Key, true
}

// identityKey answers "are these two names the same object".
//
// A name the catalog did not resolve has no equivalence class, so it falls back
// to what [ComparisonCatalogUnknown] already does for identity: it keeps its
// spelling. Returning one shared constant instead made every unresolved name
// equal to every other, and a map keyed by identity then kept exactly one of
// them -- two grants declared on two tables that do not exist yet became one
// grant, silently (stokaro/ptah#1290).
//
// The names that reach here unresolved are the desired schema's, because the
// database side is read from the catalog and therefore always resolves. A
// desired name the target does not have is a name it does not have, so keeping
// such names distinct is also the answer that describes the target truthfully.
func (s Semantics) identityKey(comparison Comparison, value string) string {
	if comparison != ComparisonCatalogResolved {
		return comparison.IdentityKey(value)
	}
	if key, ok := s.resolvedKey(value); ok {
		return key
	}
	return ComparisonCatalogUnknown.IdentityKey(value)
}

// conflictKey answers "could these two names collide in the target", which is
// the conservative question and keeps the shared key for anything unresolved.
//
// The split from [Semantics.identityKey] is the point. Two unresolved names
// differing only in case are distinct objects as far as identity goes, and may
// still collide once the target's collation has its say -- so identity
// distinguishes them and conflict detection warns about them. That is exactly
// what [ComparisonCatalogUnknown] documents, and an unresolved name under
// [ComparisonCatalogResolved] is in the same position: nothing is known about
// its equivalence class.
func (s Semantics) conflictKey(comparison Comparison, value string) string {
	if comparison != ComparisonCatalogResolved {
		return comparison.ConflictKey(value)
	}
	if key, ok := s.resolvedKey(value); ok {
		return key
	}
	return unresolvedCatalogKey
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
		ComparisonASCIIFoldedNonASCIIUnknown,
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
