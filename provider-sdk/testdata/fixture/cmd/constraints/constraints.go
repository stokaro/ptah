package main

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// referentialRules are the foreign key rules the fixture writes, in the
// spelling the contract carries.
var referentialRules = []string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}

// tableConstraints reads the CHECKs, foreign keys and UNIQUE constraints of a
// table's text, each in the form the fixture writes. A UNIQUE constraint is
// read only for a host that negotiated it; any other host is refused the read,
// since the table holds what it cannot be told.
func tableConstraints(table stored, unique bool) ([]*protocol.ObservedConstraint, error) {
	elements, err := tableElements(table.sql)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", table.name, err)
	}
	var result []*protocol.ObservedConstraint
	for _, element := range elements {
		rest, found := strings.CutPrefix(element, `CONSTRAINT "`)
		if !found {
			continue
		}
		name, rest, found := strings.Cut(rest, `" `)
		if !found {
			return nil, fmt.Errorf("table %s: unnamed constraint %q", table.name, element)
		}
		constraint := &protocol.ObservedConstraint{Name: name, TableName: table.name}
		switch {
		case strings.HasPrefix(rest, "CHECK (") && strings.HasSuffix(rest, ")"):
			constraint.Type = "CHECK"
			constraint.CheckClause = new(strings.TrimSuffix(strings.TrimPrefix(rest, "CHECK ("), ")"))
		case strings.HasPrefix(rest, "FOREIGN KEY ("):
			if err := readForeignKey(constraint, rest); err != nil {
				return nil, fmt.Errorf("table %s: %w", table.name, err)
			}
		case strings.HasPrefix(rest, "UNIQUE (") && strings.HasSuffix(rest, ")") && unique:
			constraint.Type = "UNIQUE"
			for column := range strings.SplitSeq(strings.TrimSuffix(strings.TrimPrefix(rest, "UNIQUE ("), ")"), ", ") {
				constraint.ColumnNames = append(constraint.ColumnNames, strings.ReplaceAll(strings.Trim(column, `"`), `""`, `"`))
			}
		default:
			return nil, fmt.Errorf("table %s: the fixture reads no constraint %q", table.name, element)
		}
		result = append(result, constraint)
	}
	return result, nil
}

// readForeignKey reads `FOREIGN KEY ("<column>") REFERENCES "<table>"
// ("<column>")`, then an optional `ON DELETE <rule>`.
func readForeignKey(constraint *protocol.ObservedConstraint, text string) error {
	var column, table, referenced string
	if _, err := fmt.Sscanf(text, `FOREIGN KEY (%q) REFERENCES %q (%q)`, &column, &table, &referenced); err != nil {
		return fmt.Errorf("the fixture reads no foreign key %q: %w", text, err)
	}
	constraint.Type = "FOREIGN KEY"
	constraint.ColumnNames, constraint.ForeignTable, constraint.ForeignColumns = []string{column}, new(table), []string{referenced}
	constraint.DeleteRule, constraint.UpdateRule = new("NO ACTION"), new("NO ACTION")
	if _, rule, found := strings.Cut(text, " ON DELETE "); found {
		if !slices.Contains(referentialRules, rule) {
			return fmt.Errorf("the fixture reads no rule %q", rule)
		}
		constraint.DeleteRule = new(rule)
	}
	return nil
}

// tableElements splits the element list of a CREATE TABLE statement at its
// top-level commas, outside parentheses and quotes.
func tableElements(statement string) ([]string, error) {
	open := strings.IndexByte(statement, '(')
	if open < 0 || !strings.HasSuffix(statement, ")") {
		return nil, fmt.Errorf("the fixture reads no table statement %q", statement)
	}
	body := statement[open+1 : len(statement)-1]
	var result []string
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(body); i++ {
		switch c := body[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			result = append(result, strings.TrimSpace(body[start:i]))
			start = i + 1
		}
	}
	return append(result, strings.TrimSpace(body[start:])), nil
}
