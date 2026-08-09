package graphqlrender

import (
	"fmt"
	"strings"
)

// Operation names one generated GraphQL operation shape. A shape is emitted
// only when it was asked for by name: an export that selects nothing emits data
// types alone, because Ptah generates no resolver, authorization, tenant
// isolation, pagination behavior, or data access to stand behind an operation.
type Operation string

const (
	// OperationNone selects a types-only schema. It is the default, and it
	// exists as a spelling so a caller can write the default down explicitly.
	OperationNone Operation = "none"
	// OperationList selects the Relay-style connection and edge types and one
	// paginated Query field per exported table.
	OperationList Operation = "list"
	// OperationByID selects one by-key Query field per exported table that has a
	// single-column primary key.
	OperationByID Operation = "by-id"
	// OperationCreateInput selects one create-shaped input type per exported
	// table, built from the write projection.
	OperationCreateInput Operation = "create-input"
	// OperationUpdateInput selects one update-shaped input type per exported
	// table, built from the write projection minus the primary key.
	OperationUpdateInput Operation = "update-input"
)

// operationSpellings lists the accepted selector values in the order the error
// messages name them, so the enumeration is written down exactly once.
var operationSpellings = []Operation{
	OperationNone, OperationList, OperationByID, OperationCreateInput, OperationUpdateInput,
}

// Operations is the resolved set of operation shapes an export emits. Its zero
// value is the types-only default.
type Operations struct {
	List        bool
	ByID        bool
	CreateInput bool
	UpdateInput bool
}

// Any reports whether any operation shape was selected.
func (o Operations) Any() bool {
	return o.List || o.ByID || o.CreateInput || o.UpdateInput
}

// Queries reports whether a Query root type is part of the selection. Input
// shapes alone produce no root operation, so a schema that selects only inputs
// still has no Query.
func (o Operations) Queries() bool {
	return o.List || o.ByID
}

// ParseOperations resolves a selector list such as ["list", "create-input"]
// into the set of shapes to emit. An empty list is the types-only default.
//
// Repetition is accepted because a set has no multiplicity, but an unrecognized
// value is refused rather than ignored: silently dropping a misspelled shape
// would produce a schema missing exactly the part the caller asked for.
func ParseOperations(values []string) (Operations, error) {
	var ops Operations
	var explicitNone bool
	var named []string

	for _, raw := range values {
		value := strings.TrimSpace(raw)
		switch Operation(value) {
		case OperationNone:
			explicitNone = true
		case OperationList:
			ops.List = true
			named = append(named, value)
		case OperationByID:
			ops.ByID = true
			named = append(named, value)
		case OperationCreateInput:
			ops.CreateInput = true
			named = append(named, value)
		case OperationUpdateInput:
			ops.UpdateInput = true
			named = append(named, value)
		default:
			return Operations{}, fmt.Errorf("unknown GraphQL operation %q: expected %s",
				value, spellingList())
		}
	}

	if explicitNone && ops.Any() {
		return Operations{}, fmt.Errorf(
			"GraphQL operation %q selects a types-only schema and cannot be combined with %s",
			OperationNone, strings.Join(named, ", "))
	}
	return ops, nil
}

// spellingList renders the accepted values as "a, b, c, or d" for error text.
func spellingList() string {
	quoted := make([]string, 0, len(operationSpellings))
	for _, spelling := range operationSpellings {
		quoted = append(quoted, string(spelling))
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + ", or " + quoted[len(quoted)-1]
}
