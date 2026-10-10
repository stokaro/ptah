package sqlitetable

import (
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
)

// VirtualKind identifies the declaration that makes a SQLite table virtual:
// `CREATE VIRTUAL TABLE name USING module(arguments)`.
//
// Its values define what their table is ([schemaext.TableDefinition]): a
// virtual table has no column list of its own, the module answers what its
// columns are, and the module declaration is what recreates it. So the common
// comparison compares no columns of a virtual table, and it removes a live one
// only when the desired source describes virtual tables, which only a SQLite
// SQL document and a SQLite read do.
const VirtualKind schemaext.Kind = "ptah.run/sqlite/virtual-table"

// Virtual is the module declaration that creates a virtual table.
type Virtual struct {
	// Module is the module that owns the table, such as fts5, as the USING
	// clause names it.
	Module string `json:"module"`
	// Arguments is the text between the module's parentheses, verbatim. Module
	// arguments are not SQL and only the module interprets them, so the text
	// is kept as written. Empty renders a bare `USING module`.
	Arguments string `json:"arguments,omitempty"`
}

// SameDeclaration reports whether two declarations create the same table.
// The module is an identifier, which SQLite resolves without regard to ASCII
// letter case, and only ASCII: SQLite folds no other letter. The arguments are
// compared as written, because normalizing them would equate two different
// tokenizer settings.
func (v Virtual) SameDeclaration(other Virtual) bool {
	return asciiLower(v.Module) == asciiLower(other.Module) && v.Arguments == other.Arguments
}

// SQLiteVirtualDeclaration returns the module and its arguments. The SQLite
// virtual table guard reads a table's facets through this method, so the
// comparison that calls the guard does not depend on this package.
func (v Virtual) SQLiteVirtualDeclaration() (module, arguments string) {
	return v.Module, v.Arguments
}

func asciiLower(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, value)
}

// DesiredVirtual is the module declaration a desired state states for a table.
type DesiredVirtual struct {
	Virtual
}

// ObservedVirtual is the module declaration a read found a table created with.
type ObservedVirtual struct {
	Virtual
}

// Kind returns the owned virtual table identity.
func (*DesiredVirtual) Kind() schemaext.Kind { return VirtualKind }

// Kind returns the owned virtual table identity.
func (*ObservedVirtual) Kind() schemaext.Kind { return VirtualKind }

// DefinesTable reports true: the declaration is what the table is.
func (*DesiredVirtual) DefinesTable() bool { return true }

// DefinesTable reports true: the declaration is what the table is.
func (*ObservedVirtual) DefinesTable() bool { return true }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredVirtual) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredVirtual)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedVirtual) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedVirtual)(nil)
	}
	return new(*v)
}

// Equal compares declarations field by field, as written.
func (v *DesiredVirtual) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredVirtual)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations field by field, as written.
func (v *ObservedVirtual) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedVirtual)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures the observation as a declaration of the same table. A nil
// receiver remains nil.
func (v *ObservedVirtual) Desired() *DesiredVirtual {
	if v == nil {
		return nil
	}
	return &DesiredVirtual{Virtual: v.Virtual}
}

// Observed projects a declaration as the module declaration a read of the
// table created from it reports. An invalid declaration is refused with
// schemaext.ErrInvalidValue.
func (v *DesiredVirtual) Observed() (*ObservedVirtual, error) {
	if err := ValidateDesiredVirtual(v); err != nil {
		return nil, err
	}
	return &ObservedVirtual{Virtual: v.Virtual}, nil
}

// ValidateDesiredVirtual refuses nil and a declaration that names no module.
// Errors are schemaext.InvalidModelError values wrapping
// schemaext.ErrInvalidValue.
func ValidateDesiredVirtual(v *DesiredVirtual) error {
	if v == nil {
		return virtualValidation(schemaext.Desired, fmt.Errorf("%w: nil SQLite virtual table declaration", schemaext.ErrInvalidValue))
	}
	return virtualValidation(schemaext.Desired, validateVirtual(v.Virtual))
}

// ValidateObservedVirtual refuses nil and an observation that names no module.
func ValidateObservedVirtual(v *ObservedVirtual) error {
	if v == nil {
		return virtualValidation(schemaext.Observed, fmt.Errorf("%w: nil SQLite virtual table observation", schemaext.ErrInvalidValue))
	}
	return virtualValidation(schemaext.Observed, validateVirtual(v.Virtual))
}

func validateVirtual(v Virtual) error {
	if strings.TrimSpace(v.Module) == "" {
		return fmt.Errorf("%w: a SQLite virtual table names no module", schemaext.ErrInvalidValue)
	}
	return nil
}

func virtualValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: VirtualKind, Representation: representation, Message: err.Error()}
}

// VirtualDeclaration returns the module declaration a table's facets state, or
// nil for an ordinary table. It is what the SQLite renderer writes in place of
// a column list. A virtual table carries no other SQLite table facet: STRICT
// and WITHOUT ROWID belong to CREATE TABLE, so a table holding both is
// refused with schemaext.ErrInvalidValue.
func VirtualDeclaration(facets schemaext.Facets) (*DesiredVirtual, error) {
	value, found, err := schemaext.FacetAs[*DesiredVirtual](facets, VirtualKind)
	if err != nil || !found {
		return nil, err
	}
	if err := ValidateDesiredVirtual(value); err != nil {
		return nil, err
	}
	if _, options, _ := facets.Get(TableKind); options {
		return nil, fmt.Errorf("%w: a SQLite virtual table has no STRICT or WITHOUT ROWID option", schemaext.ErrInvalidValue)
	}
	return value, nil
}

// VirtualOf returns the module declaration a table's facets hold, in either
// representation, and false for an ordinary table. A comparison holds a
// declaration on one side and an observation on the other, and a reader of
// both asks this rather than the representation. A value that cannot be read
// is returned as an error, never as an ordinary table.
func VirtualOf(facets schemaext.Facets) (Virtual, bool, error) {
	value, found, err := facets.Get(VirtualKind)
	if err != nil || !found {
		return Virtual{}, false, err
	}
	switch value := value.(type) {
	case *DesiredVirtual:
		return value.Virtual, true, ValidateDesiredVirtual(value)
	case *ObservedVirtual:
		return value.Virtual, true, ValidateObservedVirtual(value)
	default:
		return Virtual{}, false, fmt.Errorf("%w: SQLite virtual table facet has unexpected type %T", schemaext.ErrInvalidValue, value)
	}
}
