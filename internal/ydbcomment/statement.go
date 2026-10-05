package ydbcomment

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbtype"
	"ptah.run/internal/yqlservice"
)

// ErrStatement is returned for text that is one of Ptah's comment statements
// and cannot be run as written.
var ErrStatement = errors.New("invalid comment statement")

// service is the YDB service Ptah runs a comment statement through, as a
// refusal names it.
const service = "table service"

// Statement is one of Ptah's comment statements:
//
//	COMMENT ON TABLE `path` IS 'text'
//	COMMENT ON COLUMN `path`.`column` IS 'text'
//	COMMENT ON INDEX `index` ON `path` IS 'text'
//	COMMENT ON VIEW `path` IS 'text'
//
// `IS NULL` removes the comment.
type Statement struct {
	Object Object
	// Path is the table's or the view's path as the statement names it:
	// relative to the TablePathPrefix in effect, or to the database root
	// where none is, or absolute where it starts with a slash.
	Path string
	// Name is the column's or the index's name, and empty for a table or a
	// view.
	Name string
	// Comment is what the comment becomes. Empty removes it, as YDB removes
	// an attribute set to the empty string.
	Comment string
}

// Key is the attribute the statement sets.
func (s Statement) Key() string {
	return Key(s.Object, s.Name)
}

// Text writes the statement without a terminator. The comment is a YQL
// string literal, and an empty one is written NULL.
func (s Statement) Text() (string, error) {
	keyword := s.Object.Keyword()
	if keyword == "" {
		return "", fmt.Errorf("%w: unknown object %d", ErrStatement, s.Object)
	}
	if strings.TrimSpace(s.Path) == "" {
		return "", fmt.Errorf("%w: COMMENT ON %s names no path", ErrStatement, keyword)
	}
	if reason := Refusal(s.Object, s.Name, s.Comment); reason != "" {
		return "", fmt.Errorf("%w: %s", ErrStatement, reason)
	}
	path := sqlident.Quote(platform.YDB, s.Path)
	target := path
	switch s.Object {
	case Column:
		target = path + "." + sqlident.Quote(platform.YDB, s.Name)
	case Index:
		target = sqlident.Quote(platform.YDB, s.Name) + " ON " + path
	}
	value := "NULL"
	if s.Comment != "" {
		value = ydbtype.StringLiteral(s.Comment)
	}
	return "COMMENT ON " + keyword + " " + target + " IS " + value, nil
}

// Query is a query that runs one of Ptah's comment statements.
type Query struct {
	Statement
	// PathPrefix is the path the query's `PRAGMA TablePathPrefix` sets, or
	// empty where it sets none.
	PathPrefix string
}

// Absolute is the absolute path of the table or view the query names, on the
// database whose absolute path is database; see [yqlservice.Absolute].
func (q Query) Absolute(database string) (string, error) {
	absolute, err := yqlservice.Absolute(q.Path, q.PathPrefix, database)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrStatement, err)
	}
	return absolute, nil
}

// Recognize reports whether text is a query running one of Ptah's comment
// statements, and reads it.
//
// A statement opens with COMMENT ON and one of TABLE, COLUMN, INDEX and VIEW;
// where it stands in its query follows [yqlservice.Locate]. COMMENT ON any
// other kind is not recognized, so it reaches YDB and its parse error.
// Recognized text that does not read as the statement, and a comment YDB
// cannot hold ([Refusal]), are refused with [ErrStatement]. The comment is a
// single- or double-quoted string or NULL.
func Recognize(text string) (Query, bool, error) {
	if !containsFold(text, "comment") {
		return Query{}, false, nil
	}
	located, recognized, err := yqlservice.Locate(text, opens, service)
	switch {
	case err != nil:
		return Query{}, true, fmt.Errorf("%w: %w", ErrStatement, err)
	case !recognized:
		return Query{}, false, nil
	}
	statement, err := parse(located.Tokens)
	if err != nil {
		return Query{}, true, err
	}
	return Query{Statement: statement, PathPrefix: located.PathPrefix}, true, nil
}

// containsFold reports whether text contains word, ignoring ASCII case,
// without copying text: every query a YDB connection runs is asked.
func containsFold(text, word string) bool {
	for i := 0; i+len(word) <= len(text); i++ {
		if strings.EqualFold(text[i:i+len(word)], word) {
			return true
		}
	}
	return false
}

// opens reports whether tokens open one of the four statements.
func opens(tokens []lexer.Token) bool {
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("COMMENT") || !tokens[1].MatchIdentifierValue("ON") {
		return false
	}
	_, known := objectOf(tokens[2])
	return known
}

// objectOf reads the object keyword of a statement.
func objectOf(token lexer.Token) (Object, bool) {
	for _, object := range []Object{Table, Column, Index, View} {
		if token.MatchIdentifierValue(object.Keyword()) {
			return object, true
		}
	}
	return 0, false
}

// parse reads one of the four statements from its tokens.
func parse(tokens []lexer.Token) (Statement, error) {
	object, _ := objectOf(tokens[2])
	statement := Statement{Object: object}
	opening := yqlservice.FirstWords(tokens, 3)
	refuse := func(format string, args ...any) (Statement, error) {
		return Statement{}, fmt.Errorf("%w: %s: %s", ErrStatement, opening, fmt.Sprintf(format, args...))
	}
	rest := tokens[3:]
	var ok bool
	switch object {
	case Column:
		// `path`.`column`
		if len(rest) < 3 || !rest[1].MatchOperatorValue(".") {
			return refuse("name the column as `table`.`column`")
		}
		if statement.Path, ok = name(rest[0]); !ok {
			return refuse("%s does not name a table; write its path in backticks", rest[0].Value)
		}
		if statement.Name, ok = name(rest[2]); !ok {
			return refuse("%s does not name a column", rest[2].Value)
		}
		rest = rest[3:]
	case Index:
		// `index` ON `path`
		if len(rest) < 3 || !rest[1].MatchIdentifierValue("ON") {
			return refuse("name the index as `index` ON `table`")
		}
		if statement.Name, ok = name(rest[0]); !ok {
			return refuse("%s does not name an index", rest[0].Value)
		}
		if statement.Path, ok = name(rest[2]); !ok {
			return refuse("%s does not name a table; write its path in backticks", rest[2].Value)
		}
		rest = rest[3:]
	default:
		if len(rest) == 0 {
			return refuse("the statement names no %s", strings.ToLower(object.Keyword()))
		}
		if statement.Path, ok = name(rest[0]); !ok {
			return refuse("%s does not name %s; write its path in backticks", rest[0].Value, object.noun())
		}
		rest = rest[1:]
	}
	if len(rest) != 2 || !rest[0].MatchIdentifierValue("IS") {
		return refuse("the comment follows IS, as a string or NULL, and nothing follows it")
	}
	switch {
	case rest[1].MatchIdentifierValue("NULL"):
		statement.Comment = ""
	default:
		comment, isString := yqlservice.StringValue(rest[1])
		if !isString {
			return refuse("the comment %s is not a string Ptah reads; write it in quotes, with the escapes "+
				"\\\\, \\', \\\", \\n, \\r, \\t and \\xHH", rest[1].Value)
		}
		statement.Comment = comment
	}
	if reason := Refusal(statement.Object, statement.Name, statement.Comment); reason != "" {
		return refuse("%s", reason)
	}
	return statement, nil
}

// name reads a backticked or plain identifier. A named expression is not a
// name: the statement is read before YDB would evaluate one.
func name(token lexer.Token) (string, bool) {
	if token.Type != lexer.TokenIdentifier || strings.HasPrefix(token.Value, "$") {
		return "", false
	}
	value, ok := lexer.YQLIdentifierValue(token.Value)
	return value, ok && value != ""
}
