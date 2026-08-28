package sqllint

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/parser"
)

const (
	// RuleIndexNamesUnknownColumn reports an index over a column the schema
	// does not declare.
	//
	// It is the first rule here that resolves a name rather than reading the
	// statement in front of it. A CREATE INDEX names a table and columns
	// declared somewhere else, so without the schema the linter could only say
	// the statement parses (stokaro/ptah#1270, criterion 7).
	RuleIndexNamesUnknownColumn = "DDL002"
)

// indexColumnRule reports an index naming a column its table does not have.
type indexColumnRule struct{}

func (indexColumnRule) ID() string { return RuleIndexNamesUnknownColumn }

func (indexColumnRule) CheckStatement(ctx Context, stmt ast.Node) []Finding {
	index, ok := stmt.(*ast.IndexNode)
	if !ok {
		return nil
	}
	columns, known := ctx.tableColumns(index.Table)
	if !known {
		return nil
	}
	unknown := unknownIndexColumns(index, columns)
	if len(unknown) == 0 {
		return nil
	}
	line, column := lineColumn(ctx.Source.SQL, ctx.statement.offset)
	return []Finding{{
		Rule:     RuleIndexNamesUnknownColumn,
		Title:    "index names a column the schema does not declare",
		Severity: SeverityError,
		File:     ctx.Source.Name,
		Line:     line,
		Column:   column,
		Dialect:  ctx.Dialect,
		Message: fmt.Sprintf("%s indexes %s on %s, which the schema does not declare",
			indexLabel(index.Name), strings.Join(unknown, ", "), index.Table),
		Rationale: "An index over a column that does not exist fails when the statement runs, " +
			"and the schema is the only place that can say so before it does.",
	}}
}

// indexLabel names the index, or says it is unnamed rather than printing a gap.
func indexLabel(name string) string {
	if strings.TrimSpace(name) == "" {
		return "an unnamed index"
	}
	return name
}

// unknownIndexColumns returns the index's plain column references the table
// does not declare, in the order the index names them.
//
// Only plain references. An expression part -- `lower(email)` -- names no
// single column, and reporting one out of it would be a guess about SQL this
// package does not parse into an expression tree.
func unknownIndexColumns(index *ast.IndexNode, columns map[string]bool) []string {
	unknown := make([]string, 0)
	for _, name := range indexColumnNames(index) {
		if !columns[foldName(name)] {
			unknown = append(unknown, name)
		}
	}
	return unknown
}

// indexColumnNames reads the plain column references out of an index.
//
// Parts is preferred where it is populated: Columns duplicates it for legacy
// callers, and an expression part appears there as its whole text. Where Parts
// is empty the same filter has to be applied to Columns by shape, or
// `lower(email)` is reported as a missing column of that name -- which is the
// defect stokaro/ptah#2036 recorded for the comparator, arriving here through
// the same duplication.
func indexColumnNames(index *ast.IndexNode) []string {
	if len(index.Parts) == 0 {
		return plainNames(index.Columns)
	}
	names := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		if strings.TrimSpace(part.Expr) != "" {
			continue
		}
		names = append(names, part.Name)
	}
	return names
}

// plainNames keeps the entries that are a bare column reference.
//
// An expression is recognized by shape rather than parsed: this package does
// not build an expression tree, and a name carrying a parenthesis, an operator
// or a space is not one a table declares.
func plainNames(names []string) []string {
	plain := make([]string, 0, len(names))
	for _, name := range names {
		if strings.ContainsAny(name, "( )+-*/|") {
			continue
		}
		plain = append(plain, name)
	}
	return plain
}

// schemaColumns maps each declared table to the columns it declares.
func schemaColumns(schema *schemamodel.Database) map[string]map[string]bool {
	byStruct := make(map[string][]schemamodel.Field, len(schema.Tables))
	for _, field := range schema.Fields {
		byStruct[field.StructName] = append(byStruct[field.StructName], field)
	}
	tables := make(map[string]map[string]bool, len(schema.Tables))
	for _, table := range schema.Tables {
		columns := make(map[string]bool, len(byStruct[table.StructName]))
		for _, field := range byStruct[table.StructName] {
			columns[foldName(field.Name)] = true
		}
		tables[foldName(table.Name)] = columns
		if table.Schema != "" {
			tables[foldName(table.Schema+"."+table.Name)] = columns
		}
	}
	return tables
}

// foldName normalizes an identifier for comparison, dropping the quoting a
// migration may carry and the case a server may not distinguish.
func foldName(name string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(name), `"`+"`[]"))
}

// tableColumns resolves a table's columns, preferring the supplied schema and
// falling back to what the same file declares.
//
// The file first would be wrong: a schema handed in is the state the SQL will
// run against, and a CREATE TABLE earlier in the same file is a statement that
// has not run yet. Where both know the table they agree, and where they differ
// the schema is the one that decides whether the index will work.
func (c Context) tableColumns(table string) (map[string]bool, bool) {
	if c.Schema != nil {
		columns, ok := schemaColumns(c.Schema)[foldName(table)]
		if ok {
			return columns, true
		}
	}
	columns, ok := c.fileTables[foldName(table)]
	return columns, ok
}

// declaredTablesIn reads the tables a file declares, so an index can find its
// table without any schema being supplied.
func declaredTablesIn(source Source, opts Options, caps capability.Capabilities) map[string]map[string]bool {
	tables := make(map[string]map[string]bool)
	for _, statement := range splitSourceStatements(source, opts.Dialect) {
		stmtList, err := parser.NewParser(
			statementParserSQL(statement.sql), parserOptions(opts, caps)...,
		).Parse()
		if err != nil {
			continue
		}
		for _, stmt := range stmtList.Statements {
			create, ok := stmt.(*ast.CreateTableNode)
			if !ok {
				continue
			}
			columns := make(map[string]bool, len(create.Columns))
			for _, column := range create.Columns {
				columns[foldName(column.Name)] = true
			}
			tables[foldName(create.Name)] = columns
		}
	}
	return tables
}
