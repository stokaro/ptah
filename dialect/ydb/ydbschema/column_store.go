package ydbschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/internal/ydbttl"
)

// ColumnStoreKind identifies the column storage of a YDB table: `CREATE TABLE
// ... PARTITION BY HASH (...) WITH (STORE = COLUMN, ...)`. A table holding the
// value is a column table and one without it is a row table, so the value's
// absence under complete coverage asks for row storage.
//
// The storage kind, the hash key and the shard count are fixed when YDB
// creates the table; only the tiered TTL changes in place.
const ColumnStoreKind schemaext.Kind = "ptah.run/ydb/column-store"

// ColumnStore is one column table's layout and tiered TTL.
type ColumnStore struct {
	// HashColumns are the primary-key columns YDB hashes rows by, in order.
	// A declaration that leaves them out takes the server's choice, the
	// primary key.
	HashColumns []string `json:"hash_columns,omitempty"`
	// Partitions is the number of column shards. A declaration that leaves
	// it at zero takes the server's default.
	Partitions uint64 `json:"partitions,omitempty"`
	// TTL moves older rows to external data sources and may delete the
	// oldest. A TTL that only deletes is the table's [TTLKind] value.
	TTL *TieredTTL `json:"ttl,omitempty"`
}

// TieredTTL is a column table's ordered retention policy.
type TieredTTL struct {
	// Column holds the time or the integer epoch value the intervals start
	// from.
	Column string `json:"column"`
	// Unit is the unit an integer epoch column counts in, spelled as YQL
	// writes it after AS, such as SECONDS, and empty for a date column.
	Unit string `json:"unit,omitempty"`
	// Tiers apply in increasing order of age. Only the last may delete.
	Tiers []TTLTier `json:"tiers"`
}

// TTLTier moves rows older than Interval to an external data source, or
// deletes them.
type TTLTier struct {
	// Interval is an ISO 8601 duration with whole-second precision.
	Interval string `json:"interval"`
	// ExternalSource is the absolute path of the external data source rows
	// move to, such as /local/ext/bucket. Empty deletes them.
	ExternalSource string `json:"external_source,omitempty"`
}

// DesiredColumnStore is the column storage a declaration asks for.
type DesiredColumnStore struct {
	ColumnStore
}

// ObservedColumnStore is the column storage a table read found: the hash key
// and the shard count the server chose, and the tiered TTL the table holds.
type ObservedColumnStore struct {
	ColumnStore
}

// Kind returns the owned column storage identity.
func (*DesiredColumnStore) Kind() schemaext.Kind { return ColumnStoreKind }

// Kind returns the owned column storage identity.
func (*ObservedColumnStore) Kind() schemaext.Kind { return ColumnStoreKind }

// Clone returns an independent copy.
func (s ColumnStore) Clone() ColumnStore {
	s.HashColumns = slices.Clone(s.HashColumns)
	s.TTL = s.TTL.Clone()
	return s
}

// Clone returns an independent copy. Nil stays nil.
func (t *TieredTTL) Clone() *TieredTTL {
	if t == nil {
		return nil
	}
	out := *t
	out.Tiers = slices.Clone(t.Tiers)
	return &out
}

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredColumnStore) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredColumnStore)(nil)
	}
	return &DesiredColumnStore{ColumnStore: v.ColumnStore.Clone()}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedColumnStore) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedColumnStore)(nil)
	}
	return &ObservedColumnStore{ColumnStore: v.ColumnStore.Clone()}
}

// Equal compares declarations field by field, intervals as written.
func (v *DesiredColumnStore) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredColumnStore)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return equalStore(v.ColumnStore, w.ColumnStore)
}

// Equal compares observations field by field, intervals as written.
func (v *ObservedColumnStore) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedColumnStore)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return equalStore(v.ColumnStore, w.ColumnStore)
}

func equalStore(a, b ColumnStore) bool {
	if a.Partitions != b.Partitions || !slices.Equal(a.HashColumns, b.HashColumns) || (a.TTL == nil) != (b.TTL == nil) {
		return false
	}
	return a.TTL == nil || (a.TTL.Column == b.TTL.Column && a.TTL.Unit == b.TTL.Unit && slices.Equal(a.TTL.Tiers, b.TTL.Tiers))
}

// Desired captures the observation as a declaration that keeps it exactly. A
// nil receiver remains nil.
func (v *ObservedColumnStore) Desired() *DesiredColumnStore {
	if v == nil {
		return nil
	}
	return &DesiredColumnStore{ColumnStore: v.ColumnStore.Clone()}
}

// LayoutSatisfied reports whether a table holding current has the layout
// desired asks for: the hash key and the shard count where desired states
// them. The TTL is not part of the layout.
func LayoutSatisfied(desired, current ColumnStore) bool {
	return (desired.Partitions == 0 || desired.Partitions == current.Partitions) &&
		(len(desired.HashColumns) == 0 || slices.Equal(desired.HashColumns, current.HashColumns))
}

// TieredTTLEqual compares retention policies by their effective intervals:
// `P1D` equals `PT86400S`. Nil equals only nil.
func TieredTTLEqual(a, b *TieredTTL) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Column != b.Column || a.Unit != b.Unit || len(a.Tiers) != len(b.Tiers) {
		return false
	}
	for i, tier := range a.Tiers {
		as, ae := ydbttl.IntervalSeconds(tier.Interval)
		bs, be := ydbttl.IntervalSeconds(b.Tiers[i].Interval)
		if ae != nil || be != nil || as != bs || tier.ExternalSource != b.Tiers[i].ExternalSource {
			return false
		}
	}
	return true
}

// ExternalSources lists the external data sources the policy moves rows to,
// in tier order, each once. A nil policy reads none.
func (t *TieredTTL) ExternalSources() []string {
	if t == nil {
		return nil
	}
	var sources []string
	for _, tier := range t.Tiers {
		if tier.ExternalSource != "" && !slices.Contains(sources, tier.ExternalSource) {
			sources = append(sources, tier.ExternalSource)
		}
	}
	return sources
}

// ValidateDesiredColumnStore refuses a declaration YDB cannot hold; see
// [ValidateColumnStore]. Nil is invalid. Errors are
// schemaext.InvalidModelError values wrapping schemaext.ErrInvalidValue.
func ValidateDesiredColumnStore(v *DesiredColumnStore) error {
	if v == nil {
		return storeValidation(schemaext.Desired, fmt.Errorf("%w: nil YDB column storage declaration", schemaext.ErrInvalidValue))
	}
	return storeValidation(schemaext.Desired, ValidateColumnStore(v.ColumnStore))
}

// ValidateObservedColumnStore refuses an observation YDB cannot hold; see
// [ValidateColumnStore]. A read reports the hash key and the shard count; an
// observation projected from a declaration that leaves them out has none.
// Nil is invalid.
func ValidateObservedColumnStore(v *ObservedColumnStore) error {
	if v == nil {
		return storeValidation(schemaext.Observed, fmt.Errorf("%w: nil YDB column storage observation", schemaext.ErrInvalidValue))
	}
	return storeValidation(schemaext.Observed, ValidateColumnStore(v.ColumnStore))
}

// Observed projects a declaration as the storage a table created from it
// holds: the layout it states and its TTL. Nil and invalid declarations are
// refused with schemaext.ErrInvalidValue.
func (v *DesiredColumnStore) Observed() (*ObservedColumnStore, error) {
	if err := ValidateDesiredColumnStore(v); err != nil {
		return nil, err
	}
	return &ObservedColumnStore{ColumnStore: v.ColumnStore.Clone()}, nil
}

// ValidateColumnStore checks what holds whatever the table's columns and the
// server's capabilities; see [CheckColumnStore]. Errors wrap
// schemaext.ErrInvalidValue.
func ValidateColumnStore(s ColumnStore) error {
	err := CheckColumnStore(s)
	if err == nil || errors.Is(err, schemaext.ErrInvalidValue) {
		return err
	}
	return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
}

// CheckColumnStore refuses hash columns that are empty or named twice, a
// shard count beyond 32 bits, and a TTL without a column or a tier, with a
// unit YQL does not write, with intervals that do not grow strictly, deleting
// before its last tier, naming an external source by a relative path, or
// moving no row. Text must be valid UTF-8 without NUL. The error says what is
// wrong in the words a declaration's author reads; only a text refusal wraps
// schemaext.ErrInvalidValue.
func CheckColumnStore(s ColumnStore) error {
	seen := make(map[string]bool, len(s.HashColumns))
	for _, name := range s.HashColumns {
		if err := schemaext.ValidText("hash partitioning column", name); err != nil {
			return err
		}
		if strings.TrimSpace(name) == "" || seen[name] {
			return fmt.Errorf("hash partitioning column %q is empty or repeated", name)
		}
		seen[name] = true
	}
	if s.Partitions > 1<<32-1 {
		return fmt.Errorf("column shard count exceeds a 32-bit integer")
	}
	return validateTieredTTL(s.TTL)
}

func validateTieredTTL(t *TieredTTL) error {
	if t == nil {
		return nil
	}
	if strings.TrimSpace(t.Column) == "" || len(t.Tiers) == 0 {
		return fmt.Errorf("tiered TTL requires a column and at least one tier")
	}
	if err := schemaext.ValidText("TTL column", t.Column); err != nil {
		return err
	}
	unit, err := ydbttl.Unit(t.Unit)
	if err != nil {
		return err
	}
	if unit != t.Unit {
		return fmt.Errorf("the TTL unit %q is written %q", t.Unit, unit)
	}
	var previous uint64
	moves := false
	for position, tier := range t.Tiers {
		seconds, err := ydbttl.IntervalSeconds(tier.Interval)
		if err != nil {
			return fmt.Errorf("TTL tier %d: %w", position+1, err)
		}
		if position > 0 && seconds <= previous {
			return fmt.Errorf("TTL tier intervals must increase strictly")
		}
		if tier.ExternalSource == "" && position != len(t.Tiers)-1 {
			return fmt.Errorf("only the last TTL tier may delete data")
		}
		if err := schemaext.ValidText("TTL tier external source", tier.ExternalSource); err != nil {
			return err
		}
		if tier.ExternalSource != "" && !strings.HasPrefix(tier.ExternalSource, "/") {
			return fmt.Errorf("TTL tier external source must be an absolute database path")
		}
		moves = moves || tier.ExternalSource != ""
		previous = seconds
	}
	if !moves {
		return fmt.Errorf("a tiered TTL requires a tier that moves rows to an external data source; a TTL that only deletes is the table's TTL")
	}
	return nil
}

func storeValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: ColumnStoreKind, Representation: representation, Message: err.Error()}
}

// DeclaredColumnStore returns the column storage facets declare, or nil for a
// row table. A value of another type under the kind is an error.
func DeclaredColumnStore(facets schemaext.Facets) (*DesiredColumnStore, error) {
	value, found, err := schemaext.FacetAs[*DesiredColumnStore](facets, ColumnStoreKind)
	if err != nil || !found {
		return nil, err
	}
	return value, nil
}
