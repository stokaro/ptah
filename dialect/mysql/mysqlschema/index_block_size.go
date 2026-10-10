package mysqlschema

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/mysqlindex"
)

// IndexBlockSizeKind identifies the KEY_BLOCK_SIZE hint of a MySQL or MariaDB
// index, `ADD INDEX k (a) KEY_BLOCK_SIZE=8`. Unlike the options of
// [IndexKind], it is read back and compared: a different hint on an index that
// exists replaces the index.
//
// The value is separate from [IndexKind] because the two arrive differently.
// A Go annotation spells the hint as the index directive's own key_block_size
// attribute, and the parser as a platform property, and one index may write
// both.
const IndexBlockSizeKind schemaext.Kind = "ptah.run/mysql/index-block-size"

// DesiredIndexBlockSize is the hint a declaration states for one index.
type DesiredIndexBlockSize struct {
	// KeyBlockSize is the hint in kilobytes. Zero declares an index without
	// one, which is what a source that can spell the hint and did not write it
	// declares too.
	KeyBlockSize uint64 `json:"key_block_size,omitempty"`
}

// ObservedIndexBlockSize is the hint an index holds as a read reports it.
type ObservedIndexBlockSize struct {
	// KeyBlockSize is the hint in kilobytes, zero for none.
	KeyBlockSize uint64 `json:"key_block_size,omitempty"`
	// Retained reports whether the server keeps a hint on this index at all.
	// MariaDB keeps it on every table; MySQL keeps it only on a table with
	// ROW_FORMAT=COMPRESSED and accepts and discards it elsewhere. Where it is
	// false, no declared hint can change the index, so none is compared.
	Retained bool `json:"retained,omitempty"`
}

// Kind returns the owned block-size identity.
func (*DesiredIndexBlockSize) Kind() schemaext.Kind { return IndexBlockSizeKind }

// Kind returns the owned block-size identity.
func (*ObservedIndexBlockSize) Kind() schemaext.Kind { return IndexBlockSizeKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredIndexBlockSize) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredIndexBlockSize)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedIndexBlockSize) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedIndexBlockSize)(nil)
	}
	return new(*v)
}

// Equal compares declarations by their hint.
func (v *DesiredIndexBlockSize) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredIndexBlockSize)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations field by field.
func (v *ObservedIndexBlockSize) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedIndexBlockSize)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures the observation as a declaration of the hint it holds. A
// nil receiver remains nil.
func (v *ObservedIndexBlockSize) Desired() *DesiredIndexBlockSize {
	if v == nil {
		return nil
	}
	return &DesiredIndexBlockSize{KeyBlockSize: v.KeyBlockSize}
}

// Observed projects a declaration as a read of an index created from it on
// target reports it, on a table of the default row format: MariaDB retains
// the hint there and MySQL does not, so the observation keeps the declared
// size and says whether it is retained. A prediction of the whole table's
// CREATE, which knows its row format, refines it. Nil and invalid
// declarations are refused with schemaext.ErrInvalidValue.
func (v *DesiredIndexBlockSize) Observed(target string) (*ObservedIndexBlockSize, error) {
	if err := ValidateDesiredIndexBlockSize(v); err != nil {
		return nil, err
	}
	return &ObservedIndexBlockSize{KeyBlockSize: v.KeyBlockSize, Retained: mysqlindex.KeepsBlockSize(target, "")}, nil
}

// ValidateDesiredIndexBlockSize refuses a hint above the largest one MySQL
// stores. Nil is invalid. Errors are schemaext.InvalidModelError values
// wrapping schemaext.ErrInvalidValue. The smaller MariaDB limit is the
// renderer's to check, since a declaration names no target.
func ValidateDesiredIndexBlockSize(v *DesiredIndexBlockSize) error {
	if v == nil {
		return blockSizeValidation(schemaext.Desired, fmt.Errorf("%w: nil MySQL index block size declaration", schemaext.ErrInvalidValue))
	}
	return blockSizeValidation(schemaext.Desired, validBlockSize(v.KeyBlockSize))
}

// ValidateObservedIndexBlockSize refuses a hint above the largest one MySQL
// stores. Nil is invalid.
func ValidateObservedIndexBlockSize(v *ObservedIndexBlockSize) error {
	if v == nil {
		return blockSizeValidation(schemaext.Observed, fmt.Errorf("%w: nil MySQL index block size observation", schemaext.ErrInvalidValue))
	}
	return blockSizeValidation(schemaext.Observed, validBlockSize(v.KeyBlockSize))
}

func validBlockSize(size uint64) error {
	if err := mysqlindex.ValidateBlockSize(platform.MySQL, size); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

func blockSizeValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: IndexBlockSizeKind, Representation: representation, Message: err.Error()}
}

// WithIndexBlockSize returns facets with a declaration of size added, as a
// Go annotation and a SQL statement write KEY_BLOCK_SIZE. Zero returns facets
// unchanged, since a source that writes no hint declares none through its
// coverage claim. The value is bound to no target: a schema that declares the
// hint for another engine is refused there, as it always was. A collection
// that already holds the kind, and a size above the MySQL limit, are refused.
func WithIndexBlockSize(facets schemaext.Facets, size uint64) (schemaext.Facets, error) {
	if size == 0 {
		return facets, nil
	}
	declared := &DesiredIndexBlockSize{KeyBlockSize: size}
	if err := ValidateDesiredIndexBlockSize(declared); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(declared)
}

// WithObservedIndexBlockSize returns facets with an observation added, as a
// read of a MySQL or MariaDB server records it for every index. A collection
// that already holds the kind, and a size above the MySQL limit, are refused.
func WithObservedIndexBlockSize(facets schemaext.Facets, value ObservedIndexBlockSize) (schemaext.Facets, error) {
	observed := new(value)
	if err := ValidateObservedIndexBlockSize(observed); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(observed)
}

// ObservationNeeded reports whether an index's observation says more than
// its absence does. A read or a prediction that describes every index's hint
// claims so in its coverage, and an index it gives no observation holds what
// a declaration without a hint reads as on target (see
// [DesiredIndexBlockSize.Observed]): no hint, retained on MariaDB and not on
// MySQL. Only a hint, or a MySQL table that keeps one, needs the value.
func ObservationNeeded(target string, value ObservedIndexBlockSize) bool {
	return value.KeyBlockSize != 0 || value.Retained != mysqlindex.KeepsBlockSize(target, "")
}

// IndexBlockSize returns the hint facets hold in either representation:
// zero and false when they hold none. A value of another type, and an
// invalid value, are refused with schemaext.ErrInvalidValue.
func IndexBlockSize(facets schemaext.Facets) (uint64, bool, error) {
	value, found, err := facets.Get(IndexBlockSizeKind)
	if err != nil || !found {
		return 0, false, err
	}
	switch typed := value.(type) {
	case *DesiredIndexBlockSize:
		return typed.blockSize(ValidateDesiredIndexBlockSize(typed))
	case *ObservedIndexBlockSize:
		return typed.blockSize(ValidateObservedIndexBlockSize(typed))
	}
	return 0, true, fmt.Errorf("%w: facet %q has unexpected type %T", schemaext.ErrInvalidValue, IndexBlockSizeKind, value)
}

func (v *DesiredIndexBlockSize) blockSize(err error) (uint64, bool, error) {
	if err != nil {
		return 0, true, err
	}
	return v.KeyBlockSize, true, nil
}

func (v *ObservedIndexBlockSize) blockSize(err error) (uint64, bool, error) {
	if err != nil {
		return 0, true, err
	}
	return v.KeyBlockSize, true, nil
}
