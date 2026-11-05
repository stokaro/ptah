package annotation

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// IndexOwner returns the owner that reads a ptah:schema:index declaration of
// indexType in place of the frontend (see [Extension.ReadsIndex]), and false
// when none does and the frontend builds the index itself. Two owners that
// read one type are an error: each would read the declaration as an index of
// its own.
func (s Set) IndexOwner(indexType string) (string, bool, error) {
	index, found, err := s.indexOwner(indexType)
	if err != nil || !found {
		return "", false, err
	}
	return s.extensions[index].Owner, true, nil
}

func (s Set) indexOwner(indexType string) (int, bool, error) {
	found := -1
	for index, extension := range s.extensions {
		if extension.ReadsIndex == nil || !extension.ReadsIndex(indexType) {
			continue
		}
		if found >= 0 {
			return -1, false, fmt.Errorf("%w: an index of type %q is read by %s and %s", schemaext.ErrDuplicate, indexType,
				s.extensions[found].Owner, extension.Owner)
		}
		found = index
	}
	return found, found >= 0, nil
}

// DecodeIndex hands a ptah:schema:index declaration whose type an owner reads
// in place of the frontend to that owner's [FileDecoder]: the declaration
// carries every attribute it writes and the index as its [Subject]. The
// contributions the decoder returns for it are checked as [Reader.Decode]
// checks them. A declaration of a type no owner reads is an error.
func (r *Reader) DecodeIndex(declaration Declaration) ([]Contribution, error) {
	index, found, err := r.set.indexOwner(declaration.Attributes["type"])
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("no selected owner reads an index of type %q", declaration.Attributes["type"])
	}
	decoder, err := r.file(index)
	if err != nil {
		return nil, err
	}
	declaration.Attributes = cloneAttributes(declaration.Attributes)
	declaration.Targets = slices.Clone(declaration.Targets)
	contributions, err := decoder.Decode(declaration)
	if err != nil {
		return nil, err
	}
	if err := checkContributions(r.set.extensions[index], declaration.Directive, contributions); err != nil {
		return nil, err
	}
	return contributions, nil
}

// EncodedIndex is an index an owner writes as the frontend's ptah:schema:index
// directive, for an index type the owner reads in place of the frontend (see
// [Extension.ReadsIndex]). The writer writes the directive in the struct of
// Table, with the frontend's attributes the index states and then Attributes,
// so reading the source back hands the declaration to the owner again.
type EncodedIndex struct {
	// Table is the table the index is on: one of [EncodeRequest.Tables].
	Table Table
	// Name is the index's name, unique among the table's indexes.
	Name string
	// Type is the index's type, one the owner reads.
	Type string
	// Columns are the index's key columns, in order. There is at least one.
	Columns []string
	// Include are the columns the index covers, or none.
	Include []string
	// Unique marks a unique index.
	Unique bool
	// Comment is the index's comment, or empty.
	Comment string
	// Ops is the operator class the index names, or empty.
	Ops string
	// Attributes are the attributes the owner adds to the directive, in
	// order. Each is one the owner adds to ptah:schema:index (see
	// [DirectiveAttributes]).
	Attributes []EncodedAttribute
}

// checkEncodedIndexes refuses an index the extension at index writes at a
// table the request does not hold, without a name or a column, twice at one
// table, of a type the extension does not read, or with an attribute without
// a name, written twice or one the extension does not add to the directive.
func (s Set) checkEncodedIndexes(index int, request EncodeRequest, indexes []EncodedIndex) error {
	extension := s.extensions[index]
	written := make(map[[2]string]bool, len(indexes))
	for _, encoded := range indexes {
		key := [2]string{encoded.Table.QualifiedName(), encoded.Name}
		if !slices.Contains(request.Tables, encoded.Table) || strings.TrimSpace(encoded.Name) == "" || len(encoded.Columns) == 0 || written[key] ||
			extension.ReadsIndex == nil || !extension.ReadsIndex(encoded.Type) {
			return fmt.Errorf("%w: Go annotation encoder of %s wrote index %q of type %q at a table the source does not write, without a column, twice or of a type it does not read",
				schemaext.ErrInvalidValue, extension.Owner, encoded.Name, encoded.Type)
		}
		written[key] = true
		slot := Slot{Directive: indexDirective, Table: encoded.Table, Name: encoded.Name}
		names := make(map[string]bool, len(encoded.Attributes))
		for _, attribute := range encoded.Attributes {
			if strings.TrimSpace(attribute.Name) == "" || names[attribute.Name] || !extension.adds(slot, attribute.Name) {
				return fmt.Errorf("%w: Go annotation encoder of %s wrote attribute %q of index %q unnamed, twice or as one it does not add",
					schemaext.ErrInvalidValue, extension.Owner, attribute.Name, encoded.Name)
			}
			names[attribute.Name] = true
		}
	}
	return nil
}
