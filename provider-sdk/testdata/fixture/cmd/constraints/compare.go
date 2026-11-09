package main

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// quotedNumber is a number written as a string literal, which SQLite compares
// with an INTEGER column as the number.
var quotedNumber = regexp.MustCompile(`'(-?[0-9]+(?:\.[0-9]+)?)'`)

// checkText is the fixture's reading of a CHECK expression: without spaces,
// without parentheses around the whole of it, and with the quotes around a
// number removed. Two expressions with one reading are one constraint.
func checkText(expression string) string {
	text := strings.Join(strings.Fields(quotedNumber.ReplaceAllString(expression, "$1")), "")
	for strings.HasPrefix(text, "(") && strings.HasSuffix(text, ")") && balanced(text[1:len(text)-1]) {
		text = text[1 : len(text)-1]
	}
	return text
}

// balanced reports whether every parenthesis of text closes one it opened.
func balanced(text string) bool {
	depth := 0
	for _, c := range text {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}

// CompareChecks answers each pair by the fixture's reading of the two texts.
func (o *owner) CompareChecks(ctx context.Context, request *protocol.CompareChecksRequest) (*protocol.ComparedChecks, error) {
	result := &protocol.ComparedChecks{Complete: true}
	for _, table := range request.GetTables() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		answered := &protocol.ComparedCheckTable{Subject: proto.CloneOf(table.GetSubject())}
		for _, pair := range table.GetPairs() {
			held := slices.IndexFunc(table.GetCurrent().GetConstraints(), func(constraint *protocol.ObservedConstraint) bool {
				return constraint.GetName() == pair.GetConstraint()
			})
			answered.Checks = append(answered.Checks, &protocol.ComparedCheck{Constraint: pair.GetConstraint(),
				Equivalent: checkText(table.GetCurrent().GetConstraints()[held].GetCheckClause()) == checkText(pair.GetDeclared())})
		}
		result.Tables = append(result.Tables, answered)
	}
	return result, nil
}

// PrepareTables states the key membership of each declared table and, where
// the host negotiated them, the NULLS order of each key part of each declared
// index and its predicate. SQLite sorts NULLs first in an ascending key and
// last in a descending one, so a part that states that order, or none, states
// it empty. A predicate is stated in the spelling the fixture builds an index
// with and reads it back in (predicateSpelling).
func (o *owner) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	nulls := slices.Contains(request.GetTargetFeatures(), provider.IndexNullsOrderFeature)
	predicates := slices.Contains(request.GetTargetFeatures(), provider.IndexPredicatesFeature)
	result := &protocol.PreparedTables{Complete: true}
	for _, input := range request.GetTables() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declared := input.GetDesired()
		output := &protocol.PreparedTable{Subject: proto.CloneOf(input.GetSubject()), ColumnPrimaryKeysPrepared: true}
		for _, column := range declared.GetColumns() {
			if column.GetPrimary() || slices.Contains(declared.GetPrimaryKey(), column.GetName()) {
				output.ColumnPrimaryKeys = append(output.ColumnPrimaryKeys, column.GetName())
			}
		}
		for _, index := range declared.GetIndexes() {
			if !nulls && !predicates {
				break
			}
			prepared := &protocol.PreparedIndex{Name: index.GetName()}
			for _, part := range keyParts(index) {
				if nulls {
					prepared.Nulls = append(prepared.Nulls, nullsOrder(part))
				}
			}
			if predicates && strings.TrimSpace(index.GetCondition()) != "" {
				prepared.Condition = predicateSpelling(index.GetCondition())
			}
			output.Indexes = append(output.Indexes, prepared)
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}

// keyParts are the key parts of a declared index: its parts, or one part for
// each of its fields.
func keyParts(index *protocol.DeclaredIndex) []*protocol.CapturedIndexPart {
	if len(index.GetParts()) != 0 {
		return index.GetParts()
	}
	parts := make([]*protocol.CapturedIndexPart, 0, len(index.GetFields()))
	for _, field := range index.GetFields() {
		parts = append(parts, &protocol.CapturedIndexPart{Column: field})
	}
	return parts
}

// nullsOrder is a declared key part's NULLS order, empty where it is SQLite's
// default for the part's direction.
func nullsOrder(part *protocol.CapturedIndexPart) string {
	order := strings.ToUpper(part.GetNulls())
	if order == "" || order == "FIRST" && !part.GetDescending() || order == "LAST" && part.GetDescending() {
		return ""
	}
	return order
}

// ProjectTableCreations predicts the key membership of each table a plan
// would create, which is what the fixture reads back.
func (o *owner) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	result := &protocol.ProjectedTableCreations{Complete: true}
	for _, input := range request.GetTables() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declared := input.GetDeclaration()
		output := &protocol.ProjectedTableCreation{Subject: proto.CloneOf(input.GetSubject()), ColumnPrimaryKeysPrepared: true}
		for _, column := range declared.GetColumns() {
			if column.GetPrimary() || slices.Contains(declared.GetPrimaryKey(), column.GetName()) {
				output.ColumnPrimaryKeys = append(output.ColumnPrimaryKeys, column.GetName())
			}
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}

// ValidateSchema accepts every declaration: the fixture refuses what it cannot
// write when it plans.
func (o *owner) ValidateSchema(ctx context.Context, _ *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &protocol.SchemaValidation{Complete: true}, nil
}
