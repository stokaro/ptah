package safety

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/sqlutil"
)

// The reasons a statement that restates a column NOT NULL is reported with.
// Measured on MySQL 26.7, MariaDB 12.3, SQL Server 2022 and Oracle 23.26, each
// over a table holding a NULL in the column.
const (
	// MySQL and MariaDB refuse under their default strict SQL mode, MySQL with
	// error 1138 and MariaDB with 1265. Without STRICT_TRANS_TABLES both
	// rewrite the NULL to the type's zero value -- 0, the empty string -- with
	// a warning, even where the MODIFY declares a DEFAULT. Oracle's MODIFY
	// answers ORA-02296.
	modifyNotNullReason = "MODIFY or CHANGE ... NOT NULL fails when a row holds NULL; " +
		"outside strict SQL mode MySQL and MariaDB rewrite the NULL to the type's zero value instead"
	// SQL Server answers Msg 515, a default constraint on the column or not.
	alterColumnNotNullReason = "ALTER COLUMN ... NOT NULL fails when a row holds NULL"
	// Oracle's NOVALIDATE leaves the rows the table holds unchecked: the NULL
	// stays, and only new rows are held to the constraint.
	novalidateNotNullReason = "NOT NULL ENABLE NOVALIDATE keeps the NULL rows the table holds and refuses new ones"
)

// restatedNotNullReason answers the reason a statement that restates a column
// NOT NULL is a warning for, or false when it restates none.
//
// A restatement repeats the column's whole definition: MySQL's and MariaDB's
// MODIFY and CHANGE, Oracle's MODIFY, and SQL Server's ALTER COLUMN followed
// by a type. PostgreSQL's ALTER COLUMN ... SET NOT NULL changes the one
// property and has a rule of its own. A NOT NULL the statement repeats for a
// column that already had it is read the same way here, because the words
// cannot tell the two apart; AssessRendered, which has the operation, can.
//
// NOT NULL is found by clause, so a CHECK testing IS NOT NULL, a NOT NULL in
// another clause of the same ALTER TABLE, and one inside a comment or a string
// literal are not mistaken for it. Oracle's DEFAULT ON NULL makes the column
// NOT NULL too, and fails on a NULL row the same way.
func restatedNotNullReason(statement string) (string, bool) {
	clauses := alterTableClauses(statement)
	if clauses == nil {
		return "", false
	}
	for _, clause := range clauses {
		keyword, definition := columnRestatement(clause)
		if keyword == "" || !declaresNotNull(definition) {
			continue
		}
		switch {
		case hasWordSequence(definition, "NOVALIDATE"):
			return novalidateNotNullReason, true
		case keyword == "ALTER":
			return alterColumnNotNullReason, true
		default:
			return modifyNotNullReason, true
		}
	}
	return "", false
}

// restatesNotNull reports whether a statement restates a column NOT NULL.
func restatesNotNull(statement string) bool {
	_, ok := restatedNotNullReason(statement)
	return ok
}

// keepsNullability reports whether node is an ALTER TABLE whose one column
// modification states its changes and nullability is not among them. The
// NOT NULL its statement repeats is then the constraint the column has.
func keepsNullability(node ast.Node) bool {
	alter, ok := node.(*ast.AlterTableNode)
	if !ok || len(alter.Operations) != 1 {
		return false
	}
	modify, ok := alter.Operations[0].(*ast.ModifyColumnOperation)
	return ok && modify.HasChanged && !modify.Changed.Nullability
}

// restatesColumn reports whether a statement restates a column's whole
// definition in any of the spellings [restatedNotNullReason] reads.
func restatesColumn(statement string) bool {
	for _, clause := range alterTableClauses(statement) {
		if keyword, _ := columnRestatement(clause); keyword != "" {
			return true
		}
	}
	return false
}

// columnRestatement answers the keyword a clause restates a column with --
// MODIFY, CHANGE, or ALTER for SQL Server's ALTER COLUMN -- and the words of
// the definition after it, or an empty keyword when the clause restates
// nothing.
//
// ALTER COLUMN restates only when a type follows the column's name. Followed
// by SET, DROP, TYPE or ADD it changes one property, which is PostgreSQL's and
// MySQL's grammar and SQL Server's for a mask.
func columnRestatement(clause []string) (keyword string, definition []string) {
	for i, word := range clause {
		switch {
		case word == "MODIFY" || word == "CHANGE":
			return word, clause[i+1:]
		case word == "ALTER" && i+3 < len(clause) && clause[i+1] == "COLUMN":
			switch clause[i+3] {
			case "SET", "DROP", "TYPE", "ADD":
				return "", nil
			default:
				return word, clause[i+2:]
			}
		}
	}
	return "", nil
}

// declaresNotNull reports whether the words of a column definition make the
// column NOT NULL: NOT NULL not preceded by IS, or DEFAULT ON NULL.
func declaresNotNull(words []string) bool {
	for i := 1; i < len(words); i++ {
		if words[i-1] == "NOT" && words[i] == "NULL" && (i < 2 || words[i-2] != "IS") {
			return true
		}
		if i >= 2 && words[i-2] == "DEFAULT" && words[i-1] == "ON" && words[i] == "NULL" {
			return true
		}
	}
	return false
}

// alterTableClauses splits an ALTER TABLE statement into its clauses, each as
// uppercased words, or answers nil for any other statement. Comments are
// removed first. A comma splits clauses only outside parentheses, quotes and
// quoted identifiers, so Oracle's MODIFY (a ..., b ...) stays one clause and
// a DEFAULT 'a,b' stays in its own.
func alterTableClauses(statement string) [][]string {
	text := sqlutil.StripComments(statement)
	var scan clauseScanner
	depth := 0
	for i := 0; i < len(text); i++ {
		character := text[i]
		switch {
		case quoteOpens(character):
			i = scan.quoted(text, i)
		case character == '(':
			scan.endWord()
			depth++
		case character == ')':
			scan.endWord()
			depth--
		case character == ',' && depth == 0:
			scan.endClause()
		case separatesWords(character):
			scan.endWord()
		default:
			scan.word.WriteByte(character)
		}
	}
	scan.endClause()
	clauses := scan.clauses
	if len(clauses[0]) < 2 || clauses[0][0] != "ALTER" || clauses[0][1] != "TABLE" {
		return nil
	}
	clauses[0] = clauses[0][2:]
	return clauses
}

// clauseScanner collects the uppercased words of the clause being read and the
// clauses read so far.
type clauseScanner struct {
	clauses [][]string
	words   []string
	word    strings.Builder
}

func (s *clauseScanner) endWord() {
	if s.word.Len() > 0 {
		s.words = append(s.words, strings.ToUpper(s.word.String()))
		s.word.Reset()
	}
}

func (s *clauseScanner) endClause() {
	s.endWord()
	s.clauses = append(s.clauses, s.words)
	s.words = nil
}

// quoted adds the quoted text that opens at text[start] to the current word,
// quotes included, and answers the index of its closing quote. Text left
// unclosed runs to the end.
func (s *clauseScanner) quoted(text string, start int) int {
	closing := text[start]
	if closing == '[' {
		closing = ']'
	}
	end := strings.IndexByte(text[start+1:], closing)
	if end < 0 {
		s.word.WriteString(text[start:])
		return len(text)
	}
	closed := start + 1 + end
	s.word.WriteString(text[start : closed+1])
	return closed
}

// quoteOpens reports whether a character opens a string literal or a quoted
// identifier in any of the dialects read here.
func quoteOpens(character byte) bool {
	return character == '\'' || character == '"' || character == '`' || character == '['
}

// separatesWords reports whether a character ends a word.
func separatesWords(character byte) bool {
	return character == ';' || character == ',' || character == ' ' || character == '\t' || character == '\n' || character == '\r'
}
