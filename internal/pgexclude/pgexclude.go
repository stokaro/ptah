// Package pgexclude reads the text PostgreSQL prints for an EXCLUDE
// constraint. The PostgreSQL schema reader parses a live constraint with it,
// and the probe that asks the server to spell a declared EXCLUDE parses its
// answer with it, so the two sides of a comparison split the text the same
// way. It is a leaf: neither side has to link the other to share it.
package pgexclude

import (
	"fmt"
	"strings"
)

// Definition is an EXCLUDE constraint split into its index method, its
// element list and its predicate. Elements is the text inside the element
// list's parentheses; WhereCondition is the predicate without its outer
// parentheses, and empty when the constraint has none.
type Definition struct {
	UsingMethod    string
	Elements       string
	WhereCondition string
}

// Parse parses an EXCLUDE constraint definition as
// pg_get_constraintdef prints it, such as
// "EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true)".
// It refuses text that does not start with EXCLUDE USING or whose element list
// is not closed. The deferral clauses PostgreSQL appends are not part of the
// predicate.
func Parse(definition string) (*Definition, error) {
	// Remove leading/trailing whitespace
	definition = strings.TrimSpace(definition)

	// Check if it starts with "EXCLUDE USING"
	if !strings.HasPrefix(strings.ToUpper(definition), "EXCLUDE USING") {
		return nil, fmt.Errorf("invalid EXCLUDE constraint definition: %s", definition)
	}

	// Remove "EXCLUDE USING " prefix
	remaining := strings.TrimSpace(definition[13:]) // len("EXCLUDE USING") = 13

	// Find the using method (first word)
	parts := strings.Fields(remaining)
	if len(parts) == 0 {
		return nil, fmt.Errorf("missing using method in EXCLUDE constraint: %s", definition)
	}
	usingMethod := parts[0]

	// Find the opening parenthesis for elements
	openParenIdx := strings.Index(remaining, "(")
	if openParenIdx == -1 {
		return nil, fmt.Errorf("missing opening parenthesis in EXCLUDE constraint: %s", definition)
	}

	// Find the matching closing parenthesis for elements
	parenCount := 0
	elementsEndIdx := -1
	for i := openParenIdx; i < len(remaining); i++ {
		if remaining[i] == '(' {
			parenCount++
		} else if remaining[i] == ')' {
			parenCount--
			if parenCount == 0 {
				elementsEndIdx = i
				break
			}
		}
	}

	if elementsEndIdx == -1 {
		return nil, fmt.Errorf("missing closing parenthesis in EXCLUDE constraint: %s", definition)
	}

	// Extract elements (content between parentheses)
	elements := strings.TrimSpace(remaining[openParenIdx+1 : elementsEndIdx])

	// Check for WHERE clause. pg_get_constraintdef ends a deferrable
	// constraint with DEFERRABLE and, when it defers by default, INITIALLY
	// DEFERRED; both are read from pg_constraint and are not the predicate.
	whereCondition := ""
	afterElements := trimDeferralSuffix(strings.TrimSpace(remaining[elementsEndIdx+1:]))
	if strings.HasPrefix(strings.ToUpper(afterElements), "WHERE") {
		whereClause := strings.TrimSpace(afterElements[5:]) // len("WHERE") = 5
		// Remove outer parentheses if present
		if strings.HasPrefix(whereClause, "(") && strings.HasSuffix(whereClause, ")") {
			whereCondition = strings.TrimSpace(whereClause[1 : len(whereClause)-1])
		} else {
			whereCondition = whereClause
		}
	}

	return &Definition{
		UsingMethod:    usingMethod,
		Elements:       elements,
		WhereCondition: whereCondition,
	}, nil
}

// trimDeferralSuffix removes the deferral clauses pg_get_constraintdef
// appends to a constraint's definition: ` DEFERRABLE`, then ` INITIALLY
// DEFERRED` when the constraint defers by default. It never prints NOT
// DEFERRABLE or INITIALLY IMMEDIATE.
func trimDeferralSuffix(definition string) string {
	definition = strings.TrimSpace(strings.TrimSuffix(definition, " INITIALLY DEFERRED"))
	return strings.TrimSpace(strings.TrimSuffix(definition, " DEFERRABLE"))
}
