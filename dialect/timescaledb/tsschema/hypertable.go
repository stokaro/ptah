package tsschema

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DesiredHypertable declares that its table is a TimescaleDB hypertable
// partitioned by range on one column.
//
// It is a facet of the table rather than an object of its own: the declaration
// is about a table, and a target without the extension has the same table
// without it rather than a different table. Nothing outside TimescaleDB's own
// catalog can see one. Measured on 2.29.2 / PostgreSQL 17.11 after
// `create_hypertable('conditions', by_range('time'))`, `pg_class.relkind`
// answers `r`, `pg_depend` reports no extension ownership, and the index the
// call created carries `deptype 'a'` (stokaro/ptah#1026).
//
// One range dimension. A second is a separate call -- `add_dimension` with
// `by_hash` -- and a separate concept that no declaration carries.
type DesiredHypertable struct {
	// Column is the range dimension: the column chunks are cut on.
	Column string `json:"column"`
	// ChunkInterval is the width of one chunk, written the way PostgreSQL
	// spells an interval -- `7 days`, `1 hour`. Empty requests TimescaleDB's
	// own default, which is 7 days for a timestamptz column, and is never
	// compared against the interval a server reports.
	//
	// It is a string rather than a duration because the server's own spelling
	// is what `timescaledb_information.dimensions` reports back, and a
	// declaration that had to be converted to compare would differ from the
	// catalog on every run.
	ChunkInterval string `json:"chunk_interval,omitempty"`
	// IfNotExists renders `if_not_exists => TRUE`, which turns the server's
	// `table "x" is already a hypertable` error into a skipped notice.
	IfNotExists bool `json:"if_not_exists,omitempty"`
	// Comment is written before the call. It is source documentation, not a
	// catalog property, and never takes part in a comparison.
	Comment string `json:"comment,omitempty"`
}

// ObservedHypertable is a hypertable as the extension's catalog reports it:
// its first dimension, and how many dimensions it has in all.
type ObservedHypertable struct {
	// Column is the column the hypertable partitions on first. A read that
	// finds no dimension for a hypertable records that as a coverage limit
	// rather than an observation, because a declaration cannot carry it.
	Column string `json:"column"`
	// ColumnType is that column's type as the catalog spells it.
	ColumnType string `json:"column_type,omitempty"`
	// ChunkInterval is the width of one chunk on the first dimension, in the
	// server's own spelling. It is empty for a dimension the catalog reports no
	// time interval for, which is every hash dimension and an integer range one.
	ChunkInterval string `json:"chunk_interval,omitempty"`
	// Dimensions counts the partitioning dimensions. A declaration carries one,
	// so a hypertable with more is described with its first only.
	Dimensions int `json:"dimensions"`
}

// Kind returns the hypertable model identity.
func (*DesiredHypertable) Kind() schemaext.Kind { return HypertableKind }

// Kind returns the hypertable model identity.
func (*ObservedHypertable) Kind() schemaext.Kind { return HypertableKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredHypertable) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredHypertable)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedHypertable) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedHypertable)(nil)
	}
	return new(*v)
}

// Equal compares declarations field by field, without resolving defaults.
func (v *DesiredHypertable) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredHypertable)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return *v == *right
}

// Equal compares observations field by field.
func (v *ObservedHypertable) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedHypertable)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return *v == *right
}

// Desired declares the observed first dimension and its chunk interval. A
// further dimension has no declaration and is not carried; the read surfaces
// name such a table instead.
func (v *ObservedHypertable) Desired() (*DesiredHypertable, error) {
	if err := ValidateObservedHypertable(v); err != nil {
		return nil, err
	}
	return &DesiredHypertable{Column: v.Column, ChunkInterval: v.ChunkInterval}, nil
}

// Observed predicts the hypertable a declaration creates: one dimension, and
// the declared interval. An omitted interval predicts an unreported one, which
// is what a comparison treats as unknown rather than as a difference.
func (v *DesiredHypertable) Observed() (*ObservedHypertable, error) {
	if err := ValidateDesiredHypertable(v); err != nil {
		return nil, err
	}
	return &ObservedHypertable{Column: v.Column, ChunkInterval: v.ChunkInterval, Dimensions: 1}, nil
}

// SamePartitioning reports whether a declaration asks for the partitioning
// the observation describes.
//
// The column is compared without letter case, the way PostgreSQL folds an
// unquoted column name. An empty declared interval always matches: it takes
// TimescaleDB's own default, and comparing it against the interval the catalog
// reports would plan a change on every run for a declaration that asked for
// whatever the server chose.
func SamePartitioning(declared *DesiredHypertable, observed *ObservedHypertable) bool {
	sameColumn := strings.EqualFold(strings.TrimSpace(declared.Column), strings.TrimSpace(observed.Column))
	sameInterval := strings.TrimSpace(declared.ChunkInterval) == "" ||
		strings.EqualFold(strings.TrimSpace(declared.ChunkInterval), strings.TrimSpace(observed.ChunkInterval))
	return sameColumn && sameInterval
}

// ValidateDesiredHypertable checks representation invariants without claiming
// target support: a nil declaration and an empty column are invalid.
func ValidateDesiredHypertable(v *DesiredHypertable) error {
	if v == nil {
		return modelError(HypertableKind, schemaext.Desired, fmt.Errorf("%w: nil hypertable declaration", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.Column) == "" {
		return modelError(HypertableKind, schemaext.Desired, fmt.Errorf("%w: a hypertable requires a column to partition on", schemaext.ErrInvalidValue))
	}
	if err := validTexts("column", v.Column, "chunk interval", v.ChunkInterval, "comment", v.Comment); err != nil {
		return modelError(HypertableKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedHypertable checks that an observation names its first
// dimension, counts at least that one, and carries representable text.
func ValidateObservedHypertable(v *ObservedHypertable) error {
	if v == nil {
		return modelError(HypertableKind, schemaext.Observed, fmt.Errorf("%w: nil hypertable observation", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.Column) == "" {
		return modelError(HypertableKind, schemaext.Observed, fmt.Errorf("%w: a hypertable observation requires its first dimension", schemaext.ErrInvalidValue))
	}
	if v.Dimensions < 1 {
		return modelError(HypertableKind, schemaext.Observed, fmt.Errorf("%w: a hypertable has at least one dimension", schemaext.ErrInvalidValue))
	}
	if err := validTexts("column", v.Column, "column type", v.ColumnType, "chunk interval", v.ChunkInterval); err != nil {
		return modelError(HypertableKind, schemaext.Observed, err)
	}
	return nil
}

// HypertableSubject is the identity of a table's hypertable settings in a
// plan's effects: the table's own identity under the hypertable kind, so a
// partitioning change and a change to the table's definition stay separate
// writers of separate subjects.
func HypertableSubject(table objectidentity.ID) objectidentity.ID {
	table.Kind = objectidentity.Kind(HypertableKind)
	return table
}

func modelError(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
}
