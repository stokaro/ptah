package clickhouse

import (
	"errors"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/lexer"
)

// tableElements is what a table's CREATE TABLE lists that no other system
// table reports in a form a statement can take back: its projections, its
// constraints, and the storage clauses of each column. ClickHouse has no
// system table of constraints.
//
// system.projections has no definition the server accepts for every
// projection: measured on 26.10, its query column reads `name` for
// `PROJECTION p INDEX name TYPE basic`, and 24.10 has no settings column at all.
// system.tables.create_table_query prints each element the way ADD PROJECTION
// and MODIFY COLUMN take it back, on both lines, so the elements are read from
// there.
type tableElements struct {
	projections []catalog.Projection
	// checks are the CHECK constraints, and assumptions the ASSUME ones.
	checks      []catalog.Constraint
	assumptions []catalog.Assumption
	// clauses holds the storage clauses of each column that has one, by name.
	clauses map[string][]string
}

// storageClauseKeywords open the clauses of a column definition that say how
// the column is stored. The server prints them after the type, the default and
// the comment, in this order, and in upper case.
var storageClauseKeywords = []string{"CODEC", "STATISTICS", "TTL", "SETTINGS"}

var errTableElements = errors.New("cannot read the elements of the CREATE TABLE the server reports")

// parseTableElements reads the element list of a CREATE TABLE as
// system.tables.create_table_query prints it.
//
// The server prints a column's name in backquotes, always, and a projection
// and a constraint as the bare keywords PROJECTION and CONSTRAINT, so the first
// token of an element tells them apart; skipping indexes are read from their own
// system table and are left out here. A constraint is CONSTRAINT, its name, bare
// unless it needs quoting, CHECK or ASSUME, and the expression as the server
// formats it: measured on 24.10 and 26.10, `v>0 AND v<>5` reads back as
// `(v > 0) AND (v != 5)`. A storage clause starts at its keyword where the
// keyword stands outside parentheses, and runs to the next one or to the end of
// the element. CODEC, STATISTICS and SETTINGS are each followed by a
// parenthesis; TTL is followed by an expression, so a column a DEFAULT names
// TTL, unquoted, would read as a clause, and a rebuild would refuse the column
// rather than lose one.
func parseTableElements(createQuery string) (tableElements, error) {
	tokens := engineFullTokens(createQuery)
	start := slices.IndexFunc(tokens, func(token lexer.Token) bool { return token.MatchOperatorValue("(") })
	if start < 0 {
		return tableElements{}, fmt.Errorf("%w: it has no element list", errTableElements)
	}
	elements, err := splitElements(tokens[start+1:])
	if err != nil {
		return tableElements{}, err
	}
	out := tableElements{clauses: make(map[string][]string)}
	for _, element := range elements {
		first := element[0]
		switch {
		case first.Type == lexer.TokenIdentifier && first.Value == "PROJECTION":
			projection, err := readProjection(createQuery, element)
			if err != nil {
				return tableElements{}, err
			}
			out.projections = append(out.projections, projection)
		case first.Type == lexer.TokenIdentifier && first.Value == "CONSTRAINT":
			if err := out.readConstraint(createQuery, element); err != nil {
				return tableElements{}, err
			}
		case first.Type == lexer.TokenIdentifier && len(first.Value) > 0 && first.Value[0] == '`':
			name, ok := chcolumn.UnquoteIdentifier(first.Value)
			if !ok {
				return tableElements{}, fmt.Errorf("%w: column name %s", errTableElements, first.Value)
			}
			if clauses := storageClauses(createQuery, element[1:]); len(clauses) > 0 {
				out.clauses[name] = clauses
			}
		}
	}
	return out, nil
}

// splitElements splits the tokens after the opening parenthesis of the element
// list at the commas that separate elements, up to the parenthesis that closes
// the list.
func splitElements(tokens []lexer.Token) ([][]lexer.Token, error) {
	var elements [][]lexer.Token
	depth, from := 0, 0
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")") && depth > 0:
			depth--
		case token.MatchOperatorValue(")"), token.MatchOperatorValue(",") && depth == 0:
			if i == from {
				return nil, fmt.Errorf("%w: an element is empty", errTableElements)
			}
			elements = append(elements, tokens[from:i])
			if token.MatchOperatorValue(")") {
				return elements, nil
			}
			from = i + 1
		}
	}
	return nil, fmt.Errorf("%w: the element list does not close", errTableElements)
}

// readConstraint reads CONSTRAINT <name> CHECK|ASSUME <expression>, keeping
// the expression as the server spells it.
func (e *tableElements) readConstraint(createQuery string, element []lexer.Token) error {
	if len(element) < 4 || element[1].Type != lexer.TokenIdentifier || element[2].Type != lexer.TokenIdentifier {
		return fmt.Errorf("%w: a constraint has no name, kind and expression", errTableElements)
	}
	name, ok := chcolumn.UnquoteIdentifier(element[1].Value)
	if !ok {
		return fmt.Errorf("%w: constraint name %s", errTableElements, element[1].Value)
	}
	expression := createQuery[element[3].Start:element[len(element)-1].End]
	switch element[2].Value {
	case "CHECK":
		e.checks = append(e.checks, catalog.Constraint{Name: name, Type: "CHECK", CheckClause: new(expression)})
	case "ASSUME":
		e.assumptions = append(e.assumptions, catalog.Assumption{Name: name, Expression: expression})
	default:
		return fmt.Errorf("%w: constraint %s is %s, neither CHECK nor ASSUME", errTableElements, name, element[2].Value)
	}
	return nil
}

// readProjection reads PROJECTION <name> <definition>, keeping the definition
// as the server spells it.
func readProjection(createQuery string, element []lexer.Token) (catalog.Projection, error) {
	if len(element) < 3 || element[1].Type != lexer.TokenIdentifier {
		return catalog.Projection{}, fmt.Errorf("%w: a projection has no name and definition", errTableElements)
	}
	name, ok := chcolumn.UnquoteIdentifier(element[1].Value)
	if !ok {
		return catalog.Projection{}, fmt.Errorf("%w: projection name %s", errTableElements, element[1].Value)
	}
	return catalog.Projection{Name: name, Definition: createQuery[element[2].Start:element[len(element)-1].End]}, nil
}

// storageClauses returns the storage clauses of one column, from the tokens
// after its name, each as the server spells it.
func storageClauses(createQuery string, tokens []lexer.Token) []string {
	var clauses []string
	depth, start := 0, -1
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
			continue
		case token.MatchOperatorValue(")"):
			depth--
			continue
		}
		if depth != 0 || token.Type != lexer.TokenIdentifier || !slices.Contains(storageClauseKeywords, token.Value) {
			continue
		}
		if token.Value != "TTL" && (i+1 >= len(tokens) || !tokens[i+1].MatchOperatorValue("(")) {
			continue
		}
		if start >= 0 {
			clauses = append(clauses, createQuery[tokens[start].Start:tokens[i-1].End])
		}
		start = i
	}
	if start >= 0 {
		clauses = append(clauses, createQuery[tokens[start].Start:tokens[len(tokens)-1].End])
	}
	return clauses
}

// withTableElements gives the table the projections, the ASSUME constraints
// and the column storage clauses its CREATE TABLE lists, and returns its CHECK
// constraints, which the schema lists beside every other table's.
func withTableElements(table *catalog.Table, createQuery string) ([]catalog.Constraint, error) {
	elements, err := parseTableElements(createQuery)
	if err != nil {
		return nil, err
	}
	table.Projections = elements.projections
	table.Assumptions = elements.assumptions
	for i := range table.Columns {
		table.Columns[i].StorageClauses = elements.clauses[table.Columns[i].Name]
	}
	for i := range elements.checks {
		elements.checks[i].TableName = table.Name
	}
	return elements.checks, nil
}
