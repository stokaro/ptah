package ast

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// OwnedIndexConstraint is an owner's declaration of an index-backed table
// constraint. Its name may be absent until the schema reader settles names
// alongside common constraints. It is source syntax, not a schema object.
// Value contains the owner's semantics; the other fields are data the shared
// reader needs to resolve the declaration without interpreting that value.
type OwnedIndexConstraint struct {
	// Name is an unquoted logical name, or empty when the source leaves it out.
	Name string
	// Value is the owner's desired constraint model.
	Value schemaext.Value
	// NameParts and NameSuffix describe the components of a derived name after
	// the table name. The target's naming rules shorten and number that name.
	NameParts  []string
	NameSuffix string
	// FoldKey identifies declarations of this kind that build the same index
	// within one CREATE TABLE. Empty disables folding. Keys of different kinds
	// never match. An ALTER TABLE addition never folds.
	FoldKey string
	// Expressions are source expressions whose column and routine references
	// belong to the constraint. They carry no instructions for the host.
	Expressions []string
}

// Clone returns an independent declaration and validates its local payload.
// It performs no provider call and accepts no absent payload or naming rule.
func (c *OwnedIndexConstraint) Clone() (*OwnedIndexConstraint, error) {
	if c == nil || strings.TrimSpace(c.NameSuffix) == "" || strings.ContainsRune(c.Name, '\x00') {
		return nil, fmt.Errorf("%w: an owned index constraint requires a name suffix and a valid name", schemaext.ErrInvalidValue)
	}
	value, err := schemaext.CloneValue(c.Value)
	if err != nil {
		return nil, err
	}
	result := *c
	result.Value = value
	result.NameParts = slices.Clone(c.NameParts)
	result.Expressions = slices.Clone(c.Expressions)
	return &result, nil
}
