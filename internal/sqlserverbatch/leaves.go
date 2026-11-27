package sqlserverbatch

import "strings"

// Leaves returns the text of each statement the server runs from statements,
// in the order the text writes them, without the semicolon that ends it. The
// statements a block, an IF or a WHILE runs stand in for it, since BEGIN, END,
// a condition and an ELSE run nothing, and CREATE SCHEMA stands beside the
// elements it creates. A procedure, function, trigger or view is one leaf: the
// server stores its body rather than runs it.
//
// Every judgment of what T-SQL text does reads it through this one walk: the
// safety rating in ptah.run/migration/safety and the refusal of transaction
// control in internal/txcontrol. The migrator sends a whole block as one
// statement, so a judge that reads only the words a statement starts with sees
// BEGIN and misses the COMMIT inside it, and two walks agree when the second is
// written and stop agreeing when the first learns a new kind. The rating would
// then judge a statement the refusal lets through, or the other way round.
func Leaves(statements []Statement) []string {
	var leaves []string
	for _, statement := range statements {
		switch statement.Kind {
		case Block, If, While:
			leaves = append(leaves, Leaves(statement.Statements)...)
		case Schema:
			leaves = append(leaves, strings.TrimSuffix(statement.Head, ";"))
			leaves = append(leaves, Leaves(statement.Statements)...)
		default:
			leaves = append(leaves, strings.TrimSuffix(statement.SQL, ";"))
		}
	}
	return leaves
}
