package mysqlschema

import (
	"fmt"

	"ptah.run/core/schemaext"
)

// IndexKind identifies the options a MySQL or MariaDB index is created with
// beyond its common definition: the parser of a FULLTEXT index, `WITH PARSER
// ngram`. The options apply when the index is created. A read does not report
// the parser, so no plan changes it on an index that exists.
const IndexKind schemaext.Kind = "ptah.run/mysql/index"

// DesiredIndex is the options a declaration states for one index. An empty
// field is an option the declaration leaves out.
type DesiredIndex struct {
	// Parser is the FULLTEXT parser plugin, such as ngram.
	Parser string `json:"parser,omitempty"`
}

// ObservedIndex is the options an index created from a declaration holds as
// a read would report them. The reader reports no parser, so an observation
// comes from converting a declaration.
type ObservedIndex struct {
	Parser string `json:"parser,omitempty"`
}

// Kind returns the owned index options identity.
func (*DesiredIndex) Kind() schemaext.Kind { return IndexKind }

// Kind returns the owned index options identity.
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

// Equal compares declarations option by option, as written.
func (v *DesiredIndex) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredIndex)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations option by option.
func (v *ObservedIndex) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedIndex)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures the observation as a declaration that keeps it. A nil
// receiver remains nil.
func (v *ObservedIndex) Desired() *DesiredIndex {
	if v == nil {
		return nil
	}
	return &DesiredIndex{Parser: v.Parser}
}

// Observed projects a declaration as the options an index created from it
// holds. Nil and invalid declarations are refused with
// schemaext.ErrInvalidValue.
func (v *DesiredIndex) Observed() (*ObservedIndex, error) {
	if err := ValidateDesiredIndex(v); err != nil {
		return nil, err
	}
	return &ObservedIndex{Parser: v.Parser}, nil
}

// ValidateDesiredIndex refuses a parser that is not a name of letters, digits
// and underscores. Nil is invalid. Errors are schemaext.InvalidModelError
// values wrapping schemaext.ErrInvalidValue.
func ValidateDesiredIndex(v *DesiredIndex) error {
	if v == nil {
		return indexValidation(schemaext.Desired, fmt.Errorf("%w: nil MySQL index options declaration", schemaext.ErrInvalidValue))
	}
	return indexValidation(schemaext.Desired, validName("parser", v.Parser))
}

// ValidateObservedIndex refuses a parser that is not a name. Nil is invalid.
func ValidateObservedIndex(v *ObservedIndex) error {
	if v == nil {
		return indexValidation(schemaext.Observed, fmt.Errorf("%w: nil MySQL index options observation", schemaext.ErrInvalidValue))
	}
	return indexValidation(schemaext.Observed, validName("parser", v.Parser))
}

func indexValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: IndexKind, Representation: representation, Message: err.Error()}
}

// WithIndexOptions returns facets with value added as a declaration bound to
// [Targets], as the SQL parser reads `WITH PARSER`. A collection that already
// holds the kind, and an invalid value, are refused.
func WithIndexOptions(facets schemaext.Facets, value DesiredIndex) (schemaext.Facets, error) {
	declared := &DesiredIndex{Parser: value.Parser}
	if err := ValidateDesiredIndex(declared); err != nil {
		return schemaext.Facets{}, err
	}
	return withScoped(facets, declared)
}
