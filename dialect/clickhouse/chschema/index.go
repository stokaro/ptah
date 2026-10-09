package chschema

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// IndexKind identifies data-skipping settings attached to a common index.
const IndexKind schemaext.Kind = "ptah.run/clickhouse/skipping-index"

// GranularitySetting distinguishes an unmanaged granularity, a default request,
// and an explicit positive number of granules. Value is zero unless explicit.
type GranularitySetting struct {
	State SettingState `json:"state"`
	Value uint64       `json:"value,omitempty"`
}

// IsZero reports an omitted granularity for explicit omitzero serialization.
func (s GranularitySetting) IsZero() bool { return s.State == Unspecified && s.Value == 0 }

// DesiredIndex declares storage settings attached to one common index.
// Key expressions remain in the common index parts, not in this facet.
// Omitted settings remain unmanaged until the owner resolves a creation or
// comparison request. Equality preserves intent and index-type spelling.
type DesiredIndex struct {
	IndexType   Setting            `json:"index_type,omitzero"`
	Granularity GranularitySetting `json:"granularity,omitzero"`
}

// ObservedIndex contains complete data-skipping storage settings reported by a
// server. Empty types and zero granularity are invalid observations. Incomplete
// inspection belongs in coverage rather than in a fabricated default value.
type ObservedIndex struct {
	IndexType   string `json:"index_type"`
	Granularity uint64 `json:"granularity"`
}

// Kind returns the owned data-skipping model identity.
func (*DesiredIndex) Kind() schemaext.Kind { return IndexKind }

// Kind returns the owned data-skipping model identity.
func (*ObservedIndex) Kind() schemaext.Kind { return IndexKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredIndex) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredIndex)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedIndex) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedIndex)(nil)
	}
	return new(*v)
}

// Equal compares declaration intent without resolving defaults or SQL semantics.
func (v *DesiredIndex) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredIndex)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares complete observations without normalizing SQL expressions.
func (v *ObservedIndex) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedIndex)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures the observed type and granularity as explicit values. A nil
// receiver remains nil; no default request is inferred from an observed value.
func (v *ObservedIndex) Desired() *DesiredIndex {
	if v == nil {
		return nil
	}
	return &DesiredIndex{
		IndexType:   Setting{State: Explicit, Value: v.IndexType},
		Granularity: GranularitySetting{State: Explicit, Value: v.Granularity}}
}

// Observed projects a fully resolved declaration without inspecting a server.
// Unmanaged settings and default requests return ErrInvalidValue because they
// do not establish an observed value. A nil receiver is invalid.
func (v *DesiredIndex) Observed() (*ObservedIndex, error) {
	if err := ValidateDesiredIndex(v); err != nil {
		return nil, err
	}
	if v.IndexType.State != Explicit || v.Granularity.State != Explicit {
		return nil, fmt.Errorf("%w: ClickHouse index settings need target resolution before observation projection", schemaext.ErrInvalidValue)
	}
	return &ObservedIndex{IndexType: v.IndexType.Value, Granularity: v.Granularity.Value}, nil
}

// ValidateDesiredIndex checks representation invariants without parsing SQL or
// selecting target defaults. Invalid data returns a schemaext.InvalidModelError.
// An explicit type that names a PostgreSQL or MySQL access method, such as GIN
// or BTREE, is invalid: no ClickHouse server accepts it, so every path that
// creates the index would fail, after a settings replacement had already
// dropped the old one.
func ValidateDesiredIndex(v *DesiredIndex) error {
	return indexModelError(schemaext.Desired, validateDesiredIndex(v))
}

// foreignIndexAccessMethods holds index access-method names that belong to the
// PostgreSQL or MySQL families and name no ClickHouse index type.
//
// A source's common index type reaches the ClickHouse owner through the field
// those families fill, so `USING GIN` in a SQL source and a `type="GIN"`
// annotation arrive indistinguishable from a data-skipping type authored for
// this target. A statement that carries one is syntactically valid ClickHouse
// the server refuses with `Code: 80 ... Unknown Index type`, and the ALTER adds
// nothing (stokaro/ptah#3060).
//
// The same property makes the list safe to pin: this direction of the question
// is closed. ClickHouse's index-type set moves between releases -- an inverted
// index was renamed and a vector index added -- so "is this a ClickHouse index
// type" cannot be decided without a server, while the PostgreSQL and MySQL
// access-method sets are fixed. Refusing only these can never refuse a valid
// ClickHouse type, including one a future release adds.
//
// Measured against ClickHouse 25.8.33.6 on 2026-09-08: every name below is
// refused as an index type, and none appears among the types the server names
// in its own error.
var foreignIndexAccessMethods = map[string]bool{
	"bloom":    true,
	"brin":     true,
	"btree":    true,
	"fulltext": true,
	"gin":      true,
	"gist":     true,
	"hash":     true,
	"rtree":    true,
	"spatial":  true,
	"spgist":   true,
}

// refuseForeignAccessMethod refuses a type that names a PostgreSQL or MySQL
// access method. The base name is taken before the parenthesis, because a
// ClickHouse type carries arguments -- `tokenbf_v1(256, 2, 0)` -- and a foreign
// method written with them would otherwise walk past the check.
func refuseForeignAccessMethod(indexType string) error {
	base, _, _ := strings.Cut(strings.TrimSpace(indexType), "(")
	if !foreignIndexAccessMethods[strings.ToLower(strings.TrimSpace(base))] {
		return nil
	}
	return fmt.Errorf("%w: index type %q names a PostgreSQL or MySQL access method and no ClickHouse index type,"+
		" so the server would refuse the statement and add nothing; declare a ClickHouse"+
		" data-skipping index type such as minmax, or drop the type to take the minmax default",
		schemaext.ErrInvalidValue, strings.TrimSpace(indexType))
}

func validateDesiredIndex(v *DesiredIndex) error {
	if v == nil {
		return fmt.Errorf("%w: nil ClickHouse index declaration", schemaext.ErrInvalidValue)
	}
	if err := validateSetting(v.IndexType); err != nil {
		return err
	}
	if v.IndexType.State == Explicit {
		if err := indexText(v.IndexType.Value, "type"); err != nil {
			return err
		}
		if err := refuseForeignAccessMethod(v.IndexType.Value); err != nil {
			return err
		}
	}
	switch v.Granularity.State {
	case Explicit:
		if v.Granularity.Value == 0 {
			return fmt.Errorf("%w: explicit ClickHouse index granularity must be positive", schemaext.ErrInvalidValue)
		}
	case Unspecified, Default:
		if v.Granularity.Value != 0 {
			return fmt.Errorf("%w: a non-explicit ClickHouse index granularity cannot carry a value", schemaext.ErrInvalidValue)
		}
	default:
		return fmt.Errorf("%w: unknown ClickHouse index granularity state %q", schemaext.ErrInvalidValue, v.Granularity.State)
	}
	return nil
}

// ValidateObservedIndex requires complete, representable settings. It validates
// neither server support nor SQL expression semantics; those belong to adapters.
func ValidateObservedIndex(v *ObservedIndex) error {
	if v == nil {
		return indexModelError(schemaext.Observed, fmt.Errorf("%w: nil ClickHouse index observation", schemaext.ErrInvalidValue))
	}
	return indexModelError(schemaext.Observed, validateDesiredIndex(v.Desired()))
}

func indexText(value, property string) error {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: ClickHouse index %s must be nonempty valid text without NUL bytes", schemaext.ErrInvalidValue, property)
	}
	return nil
}

func indexModelError(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: IndexKind, Representation: representation, Message: err.Error()}
}
