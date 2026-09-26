package pgname

import (
	"slices"
	"strconv"
	"strings"
)

// excludeLabel is the label PostgreSQL ends the name of an unnamed EXCLUDE
// constraint with.
const excludeLabel = "excl"

// Exclude answers the name PostgreSQL gives an unnamed EXCLUDE constraint on
// table, as ChooseIndexName chooses it for an exclusion constraint:
// `<table>_<element names>_excl`, fitted into 63 bytes and numbered past the
// names taken answers true for, as [Constraint] does. elements is the element
// list as written between the parentheses, such as `room_id WITH =, during
// WITH &&`.
//
// The server builds an index for the constraint, so a caller's taken answers
// for the relations of the schema -- tables, views, sequences, indexes -- as
// well as for its constraints.
//
// Each element is named by ChooseIndexColumnNames: a column by its name, and
// an expression by the name FigureColname gives it, `expr` when there is none.
// A name an earlier element of the list already took is numbered. Measured on
// PostgreSQL 18.6:
//
//	EXCLUDE USING gist (r WITH =, s WITH <>) on b            b_r_s_excl
//	EXCLUDE USING btree (lower(t) WITH =) on d                d_lower_excl
//	EXCLUDE USING btree ((r + 1) WITH =) on f                 f_expr_excl
//	EXCLUDE USING gist (r WITH =, r WITH <>) on h             h_r_r1_excl
//	EXCLUDE USING btree (r WITH =) on kk, beside an index kk_r_excl   kk_r_excl1
func Exclude(table, elements string, taken func(string) bool) string {
	return Constraint(table, excludeElementNames(elements), excludeLabel, taken)
}

// excludeElementNames names the elements of an EXCLUDE constraint as
// ChooseIndexColumnNames does: each by [elementName], and a name an earlier
// element holds numbered 1, 2 and on, the name cut so the number fits in 63
// bytes.
func excludeElementNames(elements string) []string {
	var names []string
	for _, element := range splitTopLevel(significantTokens(elements)) {
		original := elementName(element)
		name := original
		for suffix := 1; slices.Contains(names, name); suffix++ {
			number := strconv.Itoa(suffix)
			name = clip(original, maxIdentifierBytes-len(number)) + number
		}
		names = append(names, name)
	}
	return names
}

// elementName names one index element: the column it is, or the expression's
// name. The element's opclass, collation, ordering and `WITH operator` follow
// its first term and do not change the name.
func elementName(element tokenList) string {
	switch {
	case element.operatorAt(0, "("):
		end := element.skipBalanced(0, "(", ")")
		if name, strength := figureName(element[1 : end-1]); strength > 0 {
			return name
		}
		return "expr"
	default:
		end := element.skipQualifiedName(0)
		if end == 0 {
			return "expr"
		}
		name, _ := element.nameAt(end - 1)
		return name
	}
}

// figureName names an expression as FigureColname does, with the strength it
// reports: 2 for a column or a function, 1 for a name taken from a type or a
// CASE, 0 for none.
func figureName(expression tokenList) (string, int) {
	expression = stripParentheses(expression)
	if len(expression) == 0 {
		return "", 0
	}
	if cast := lastTopLevelCast(expression); cast > 0 {
		return castName(expression[:cast], expression[cast+2:])
	}
	if collate := topLevelKeyword(expression, "collate"); collate > 0 {
		return figureName(expression[:collate])
	}
	switch {
	case expression.keywordAt(0, "cast") && expression.operatorAt(1, "(") &&
		expression.skipBalanced(1, "(", ")") == len(expression):
		inner := expression[2 : len(expression)-1]
		if as := topLevelKeyword(inner, "as"); as > 0 {
			return castName(inner[:as], inner[as+1:])
		}
		return "", 0
	case expression.keywordAt(0, "case"):
		return caseName(expression)
	case expression.keywordAt(0, "array") && expression.operatorAt(1, "["):
		return "array", 2
	case expression.keywordAt(0, "row") && expression.operatorAt(1, "("):
		return "row", 2
	}
	end := expression.skipQualifiedName(0)
	if end == 0 {
		return "", 0
	}
	name, _ := expression.nameAt(end - 1)
	switch {
	case end == len(expression):
		// A column, or a function written without parentheses such as
		// current_date: either way FigureColname answers the word itself.
		return name, 2
	case expression.operatorAt(end, "(") && expression.skipBalanced(end, "(", ")") == len(expression):
		return name, 2
	case expression.operatorAt(end, "["):
		// A subscript names what it subscripts.
		return name, 2
	default:
		return "", 0
	}
}

// castName names `arg::type`: the argument's name when it has a strong one,
// and the type's otherwise.
func castName(argument, typeName tokenList) (string, int) {
	if name, strength := figureName(argument); strength > 1 {
		return name, strength
	}
	return internalTypeName(typeName), 1
}

// caseName names a CASE expression by its ELSE result when that has a strong
// name, and `case` otherwise. The ELSE is the one of this CASE, not of a CASE
// nested inside it.
func caseName(expression tokenList) (string, int) {
	depth, nesting := 0, 0
	elseAt := -1
	for position, token := range expression {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case depth == 0 && expression.keywordAt(position, "case"):
			nesting++
		case depth == 0 && expression.keywordAt(position, "end"):
			nesting--
		case depth == 0 && nesting == 1 && expression.keywordAt(position, "else"):
			elseAt = position
		}
	}
	if elseAt > 0 && expression.keywordAt(len(expression)-1, "end") {
		if name, strength := figureName(expression[elseAt+1 : len(expression)-1]); strength > 1 {
			return name, strength
		}
	}
	return "case", 1
}

// systemTypeNames are the names the grammar gives the SQL-standard spellings
// of a type, which is the name a cast is named after. A type spelled with its
// own name keeps it.
var systemTypeNames = map[string]string{
	"int":                         "int4",
	"integer":                     "int4",
	"smallint":                    "int2",
	"bigint":                      "int8",
	"real":                        "float4",
	"float":                       "float8",
	"double precision":            "float8",
	"decimal":                     "numeric",
	"dec":                         "numeric",
	"boolean":                     "bool",
	"character varying":           "varchar",
	"char varying":                "varchar",
	"character":                   "bpchar",
	"char":                        "bpchar",
	"bit varying":                 "varbit",
	"timestamp with time zone":    "timestamptz",
	"timestamp without time zone": "timestamp",
	"time with time zone":         "timetz",
	"time without time zone":      "time",
}

// internalTypeName names a type as the parse tree does: the last part of a
// qualified name, or the internal name of a SQL-standard spelling. A modifier
// and array brackets do not change it.
func internalTypeName(typeName tokenList) string {
	var words []string
	for position := 0; position < len(typeName) && !typeName.operatorAt(position, "["); {
		if typeName.operatorAt(position, "(") {
			position = typeName.skipBalanced(position, "(", ")")
			continue
		}
		if name, ok := typeName.nameAt(position); ok {
			words = append(words, name)
		}
		position++
	}
	if len(words) == 0 {
		return ""
	}
	if internal, ok := systemTypeNames[strings.Join(words, " ")]; ok {
		return internal
	}
	return words[len(words)-1]
}

// stripParentheses removes the parentheses that enclose the whole expression.
func stripParentheses(expression tokenList) tokenList {
	for expression.operatorAt(0, "(") && expression.skipBalanced(0, "(", ")") == len(expression) {
		expression = expression[1 : len(expression)-1]
	}
	return expression
}

// lastTopLevelCast answers the position of the last `::` outside any
// parentheses or brackets, and -1 when there is none.
func lastTopLevelCast(expression tokenList) int {
	found := -1
	depth := 0
	for position, token := range expression {
		switch {
		case token.MatchOperatorValue("("), token.MatchOperatorValue("["):
			depth++
		case token.MatchOperatorValue(")"), token.MatchOperatorValue("]"):
			depth--
		case depth == 0 && token.MatchOperatorValue(":") && expression.operatorAt(position+1, ":"):
			found = position
		}
	}
	return found
}

// topLevelKeyword answers the position of the first unquoted word outside any
// parentheses, and -1 when there is none.
func topLevelKeyword(expression tokenList, word string) int {
	depth := 0
	for position, token := range expression {
		switch {
		case token.MatchOperatorValue("("), token.MatchOperatorValue("["):
			depth++
		case token.MatchOperatorValue(")"), token.MatchOperatorValue("]"):
			depth--
		case depth == 0 && expression.keywordAt(position, word):
			return position
		}
	}
	return -1
}

// splitTopLevel splits a list on the commas outside any parentheses or
// brackets.
func splitTopLevel(list tokenList) []tokenList {
	var parts []tokenList
	depth, start := 0, 0
	for position, token := range list {
		switch {
		case token.MatchOperatorValue("("), token.MatchOperatorValue("["):
			depth++
		case token.MatchOperatorValue(")"), token.MatchOperatorValue("]"):
			depth--
		case depth == 0 && token.MatchOperatorValue(","):
			parts = append(parts, list[start:position])
			start = position + 1
		}
	}
	if start < len(list) {
		parts = append(parts, list[start:])
	}
	return parts
}
