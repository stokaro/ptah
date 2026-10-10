package ydbschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// ColumnFamiliesKind identifies the column families of a YDB row table: groups
// of columns YDB stores together with settings of their own, as
// `CREATE TABLE ... (c T FAMILY f, ..., FAMILY f (DATA = ..., COMPRESSION =
// ..., CACHE_MODE = ...))` declares them.
//
// Every row table has the family named [DefaultColumnFamily], which holds the
// key columns and every column no other family names. Declaring it changes
// its settings and lists no columns.
//
// A setting left empty is one the declaration does not state, and a table
// keeps the value it holds: a new table's family takes it from the cluster's
// table profile or from YDB, and no change of an existing table writes it. A
// family the table holds and the declaration leaves out stays as well, since a
// table profile can add a family to every new table and YQL drops none. A
// column the declaration places in no family sits in the default family.
const ColumnFamiliesKind schemaext.Kind = "ptah.run/ydb/column-families"

// DefaultColumnFamily is the name of the family every YDB row table has.
const DefaultColumnFamily = "default"

// The values COMPRESSION and CACHE_MODE take on a row table, as Ptah writes
// them. YDB takes them in any case; the model holds them in lower case.
const (
	CompressionOff    = "off"
	CompressionLZ4    = "lz4"
	CacheModeRegular  = "regular"
	CacheModeInMemory = "in_memory"
)

// ColumnFamily is one family of a row table.
type ColumnFamily struct {
	// Name is the family's name, unique within its table and compared as
	// written: YDB family names are case-sensitive.
	Name string `json:"name"`
	// Data is DATA, the kind of storage pool the family's columns are kept
	// in, such as `ssd` or `hdd`. Which kinds exist is the database's own
	// configuration. Empty states none.
	Data string `json:"data,omitempty"`
	// Compression is COMPRESSION, [CompressionOff] or [CompressionLZ4]. Empty
	// states none.
	Compression string `json:"compression,omitempty"`
	// CacheMode is CACHE_MODE, [CacheModeRegular] or [CacheModeInMemory],
	// which asks YDB to keep the family's columns in memory. Empty states
	// none.
	CacheMode string `json:"cache_mode,omitempty"`
	// KeepInMemory is true when a read finds the family's keep_in_memory
	// setting enabled, which a table profile's `column_cache` sets. No source
	// states it: YQL takes no such family setting, so no statement Ptah
	// writes sets it. A declaration holds it only where a comparison adopted
	// the families a table holds.
	KeepInMemory bool `json:"keep_in_memory,omitempty"`
	// Columns are the columns the family holds. The default family lists
	// none. The list is a set: its order carries no meaning.
	Columns []string `json:"columns,omitempty"`
}

// Clone returns an independent family.
func (f ColumnFamily) Clone() ColumnFamily {
	f.Columns = slices.Clone(f.Columns)
	return f
}

// DesiredColumnFamilies is the families a declaration states for one table. A
// table whose declaration states none, under complete source coverage, asks
// for nothing: a family the table holds stays, and every column sits in the
// default family.
type DesiredColumnFamilies struct {
	Families []ColumnFamily `json:"families"`
}

// ObservedColumnFamilies is the families a table read found, the default
// family included, with each setting as the table holds it.
type ObservedColumnFamilies struct {
	Families []ColumnFamily `json:"families"`
}

// Kind returns the owned column family identity.
func (*DesiredColumnFamilies) Kind() schemaext.Kind { return ColumnFamiliesKind }

// Kind returns the owned column family identity.
func (*ObservedColumnFamilies) Kind() schemaext.Kind { return ColumnFamiliesKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredColumnFamilies) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredColumnFamilies)(nil)
	}
	return &DesiredColumnFamilies{Families: CloneColumnFamilies(v.Families)}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedColumnFamilies) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedColumnFamilies)(nil)
	}
	return &ObservedColumnFamilies{Families: CloneColumnFamilies(v.Families)}
}

// Equal compares declarations as sets: family order and column order carry
// no meaning.
func (v *DesiredColumnFamilies) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredColumnFamilies)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return slices.EqualFunc(SortedColumnFamilies(v.Families), SortedColumnFamilies(w.Families), equalFamily)
}

// Equal compares observations as sets: family order and column order carry
// no meaning.
func (v *ObservedColumnFamilies) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedColumnFamilies)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return slices.EqualFunc(SortedColumnFamilies(v.Families), SortedColumnFamilies(w.Families), equalFamily)
}

func equalFamily(a, b ColumnFamily) bool {
	return a.Name == b.Name && a.Data == b.Data && a.Compression == b.Compression && a.CacheMode == b.CacheMode &&
		a.KeepInMemory == b.KeepInMemory && slices.Equal(a.Columns, b.Columns)
}

// Desired captures the observed families as a declaration that keeps them
// exactly, keep_in_memory included. A nil receiver remains nil.
func (v *ObservedColumnFamilies) Desired() *DesiredColumnFamilies {
	if v == nil {
		return nil
	}
	return &DesiredColumnFamilies{Families: CloneColumnFamilies(v.Families)}
}

// Observed projects a declaration as the families a table created from it
// holds where no table profile adds any: each family as declared. Nil and
// invalid declarations are refused with schemaext.ErrInvalidValue.
func (v *DesiredColumnFamilies) Observed() (*ObservedColumnFamilies, error) {
	if err := ValidateDesiredColumnFamilies(v); err != nil {
		return nil, err
	}
	return &ObservedColumnFamilies{Families: CloneColumnFamilies(v.Families)}, nil
}

// CloneColumnFamilies copies a list of families with [ColumnFamily.Clone].
// Nil stays nil.
func CloneColumnFamilies(families []ColumnFamily) []ColumnFamily {
	if families == nil {
		return nil
	}
	out := make([]ColumnFamily, len(families))
	for i, family := range families {
		out[i] = family.Clone()
	}
	return out
}

// SortedColumnFamilies returns an independent copy of families sorted by
// name, each with its columns sorted: the order a codec writes. Nil stays
// nil.
func SortedColumnFamilies(families []ColumnFamily) []ColumnFamily {
	out := CloneColumnFamilies(families)
	for i := range out {
		slices.Sort(out[i].Columns)
	}
	slices.SortFunc(out, func(a, b ColumnFamily) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// ValidateDesiredColumnFamilies refuses a declaration no source writes and YDB
// cannot hold: see [ValidateObservedColumnFamilies]. Nil is invalid. Errors
// are schemaext.InvalidModelError values wrapping schemaext.ErrInvalidValue.
//
// Whether the columns a family names are columns of the table, and whether a
// key column stays in the default family, depends on the table and is checked
// where the families are written.
func ValidateDesiredColumnFamilies(v *DesiredColumnFamilies) error {
	if v == nil {
		return familyValidation(schemaext.Desired, fmt.Errorf("%w: nil YDB column family declaration", schemaext.ErrInvalidValue))
	}
	return familyValidation(schemaext.Desired, validateFamilies(v.Families))
}

// ValidateObservedColumnFamilies refuses families YDB cannot hold: a family
// without a name, two families of one name, a compression or a cache mode
// that is not one of the values a row table takes, in lower case, an empty
// storage pool kind, a column named twice or with an empty name, a column in
// two families, and a default family listing columns. Text is valid UTF-8
// without NUL. Nil is invalid.
func ValidateObservedColumnFamilies(v *ObservedColumnFamilies) error {
	if v == nil {
		return familyValidation(schemaext.Observed, fmt.Errorf("%w: nil YDB column family observation", schemaext.ErrInvalidValue))
	}
	return familyValidation(schemaext.Observed, validateFamilies(v.Families))
}

func validateFamilies(families []ColumnFamily) error {
	names := make(map[string]bool, len(families))
	holder := make(map[string]string)
	for _, family := range families {
		if err := validateFamily(family); err != nil {
			return err
		}
		if names[family.Name] {
			return fmt.Errorf("%w: column family %q is listed twice", schemaext.ErrInvalidValue, family.Name)
		}
		names[family.Name] = true
		for _, column := range family.Columns {
			if held, found := holder[column]; found {
				return fmt.Errorf("%w: column %q is in two column families, %q and %q", schemaext.ErrInvalidValue, column, held, family.Name)
			}
			holder[column] = family.Name
		}
	}
	return nil
}

func validateFamily(family ColumnFamily) error {
	if strings.TrimSpace(family.Name) == "" {
		return fmt.Errorf("%w: a column family needs a name", schemaext.ErrInvalidValue)
	}
	for _, text := range []struct{ field, value string }{
		{"column family name", family.Name}, {"column family storage pool", family.Data},
	} {
		if err := schemaext.ValidText(text.field, text.value); err != nil {
			return err
		}
	}
	if family.Data != "" && strings.TrimSpace(family.Data) == "" {
		return fmt.Errorf("%w: column family %q names an empty storage pool kind", schemaext.ErrInvalidValue, family.Name)
	}
	if !slices.Contains([]string{"", CompressionOff, CompressionLZ4}, family.Compression) {
		return fmt.Errorf("%w: column family %q has compression %q, which is not %s or %s",
			schemaext.ErrInvalidValue, family.Name, family.Compression, CompressionOff, CompressionLZ4)
	}
	if !slices.Contains([]string{"", CacheModeRegular, CacheModeInMemory}, family.CacheMode) {
		return fmt.Errorf("%w: column family %q has cache mode %q, which is not %s or %s",
			schemaext.ErrInvalidValue, family.Name, family.CacheMode, CacheModeRegular, CacheModeInMemory)
	}
	if family.Name == DefaultColumnFamily && len(family.Columns) > 0 {
		return fmt.Errorf("%w: the default column family lists columns, and it holds every column no other family names",
			schemaext.ErrInvalidValue)
	}
	seen := make(map[string]bool, len(family.Columns))
	for _, column := range family.Columns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("%w: column family %q names an empty column", schemaext.ErrInvalidValue, family.Name)
		}
		if err := schemaext.ValidText("column family column", column); err != nil {
			return err
		}
		if seen[column] {
			return fmt.Errorf("%w: column family %q names column %q twice", schemaext.ErrInvalidValue, family.Name, column)
		}
		seen[column] = true
	}
	return nil
}

func familyValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: ColumnFamiliesKind, Representation: representation, Message: err.Error()}
}
