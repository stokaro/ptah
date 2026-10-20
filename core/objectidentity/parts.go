package objectidentity

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"unique"
)

// Parts is an immutable, ordered tuple of identifier components. Its zero value
// is empty. Copies share interned immutable nodes, so IDs remain comparable and
// schema snapshots cannot be changed through a caller's slice.
type Parts struct {
	head unique.Handle[partNode]
	key  unique.Handle[normalizedPartNode]
}

type partNode struct {
	part Part
	next unique.Handle[partNode]
}

type normalizedPartNode struct {
	value string
	next  unique.Handle[normalizedPartNode]
}

// NewParts captures components without joining or normalizing them. Each Part
// already carries the comparison form established by its owner.
func NewParts(parts ...Part) Parts {
	var result Parts
	for _, part := range slices.Backward(parts) {
		result.head = unique.Make(partNode{part: part, next: result.head})
		result.key = unique.Make(normalizedPartNode{value: part.Normalized, next: result.key})
	}
	return result
}

// Values returns an independent slice retaining source spelling and metadata.
func (p Parts) Values() []Part {
	values := make([]Part, 0)
	for node := p.head; node != (unique.Handle[partNode]{}); {
		value := node.Value()
		values = append(values, value.part)
		node = value.next
	}
	return values
}

// IsZero reports whether the tuple contains no components.
func (p Parts) IsZero() bool { return p.head == (unique.Handle[partNode]{}) }

// Equal compares component boundaries and normalized values, ignoring spelling.
func (p Parts) Equal(other Parts) bool { return p.key == other.key }

// Compare orders tuples lexicographically by normalized component, then length.
func (p Parts) Compare(other Parts) int {
	left, right := p.key, other.key
	zero := unique.Handle[normalizedPartNode]{}
	for left != zero && right != zero {
		a, b := left.Value(), right.Value()
		if order := cmp.Compare(a.value, b.value); order != 0 {
			return order
		}
		left, right = a.next, b.next
	}
	if left != zero {
		return 1
	}
	if right != zero {
		return -1
	}
	return 0
}

// MarshalJSON encodes the tuple as an array of structured components.
func (p Parts) MarshalJSON() ([]byte, error) { return json.Marshal(p.Values()) }

// UnmarshalJSON captures an array without exposing mutable storage.
func (p *Parts) UnmarshalJSON(data []byte) error {
	var values []Part
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&values); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("identifier parts require one JSON array")
	}
	*p = NewParts(values...)
	return nil
}
