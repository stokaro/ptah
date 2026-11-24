package postgres

import (
	"context"
	"fmt"
	"strings"
)

// The ON UPDATE expression of a column is in no pg_catalog table, and
// information_schema.columns has no column_on_update on CockroachDB v25.4.21,
// though the server records what the expression names and refuses to drop a
// function it calls or a sequence it takes from. SHOW CREATE ALL TABLES prints
// the expression on the column's line, so where the column is absent the
// cleanup reads it from there.

// showCreateAllTables lists the CREATE statement of each table, view and
// sequence of the current database, and the ALTER statements that add their
// foreign keys.
const showCreateAllTables = "SHOW CREATE ALL TABLES"

// readOnUpdateFromCreateStatements reads the ON UPDATE expression of each
// column of a table dropped does not list from SHOW CREATE ALL TABLES, as
// outside texts.
func readOnUpdateFromCreateStatements(
	ctx context.Context,
	q cleanupConn,
	dropped map[cleanupKey]string,
) ([]outsideText, error) {
	rows, err := q.QueryContext(ctx, showCreateAllTables)
	if err != nil {
		return nil, fmt.Errorf("failed to read the ON UPDATE expressions outside the cleanup: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var texts []outsideText
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			return nil, fmt.Errorf("failed to read the ON UPDATE expressions outside the cleanup: %w", err)
		}
		definition, isTable := strings.CutPrefix(statement, "CREATE TABLE ")
		if !isTable {
			continue
		}
		table, err := onUpdateClauses(definition)
		if err != nil {
			return nil, fmt.Errorf("failed to read the ON UPDATE expressions outside the cleanup: %w", err)
		}
		key, spelled := dependentOf(relationFamily, table.schema, table.name, "r")
		if _, alsoDropped := dropped[key]; alsoDropped {
			continue
		}
		for _, clause := range table.clauses {
			texts = append(texts, outsideText{what: "on update", key: key, kind: spelled, text: clause.text, carrier: clause.column})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read the ON UPDATE expressions outside the cleanup: %w", err)
	}
	return texts, nil
}

// onUpdateClause is a column and the text of its line from its ON UPDATE
// keyword on.
type onUpdateClause struct {
	column, text string
}

// tableOnUpdates is a table and the ON UPDATE clauses of its columns.
type tableOnUpdates struct {
	schema, name string
	clauses      []onUpdateClause
}

// onUpdateClauses reads the definition of a CREATE TABLE statement, the text
// after CREATE TABLE, as SHOW CREATE ALL TABLES prints it: the schema and the
// name of the table, and each column whose line carries ON UPDATE. A column is
// a line that starts with a tab, and ON UPDATE is the last clause CockroachDB
// prints on it. The text runs from the first ON UPDATE on the line to its end,
// so a string in the default that holds the keyword widens the text rather
// than cutting the expression short.
//
// CockroachDB prints a quoted identifier as it is, with any newline it holds,
// so the column list is cut into lines by [definitionLines], which reads the
// quotes, and never at every newline: a newline in a column name would leave
// the column's ON UPDATE on a line without its tab.
//
// A table name, a column name or a column list it cannot read is an error,
// never a table without an ON UPDATE: the clause it would skip may name what
// the cleanup drops.
func onUpdateClauses(definition string) (tableOnUpdates, error) {
	var table tableOnUpdates
	header, _, _ := strings.Cut(definition, "\n")
	schema, rest, ok := cutIdentifier(definition)
	if ok {
		rest, ok = strings.CutPrefix(rest, ".")
	}
	var name string
	if ok {
		name, rest, ok = cutIdentifier(rest)
	}
	if ok {
		rest, ok = strings.CutPrefix(rest, " (")
	}
	if !ok {
		return tableOnUpdates{}, fmt.Errorf("the table name of %q cannot be read", "CREATE TABLE "+header)
	}
	table.schema, table.name = schema, name
	lines, ok := definitionLines(rest)
	if !ok {
		return tableOnUpdates{}, fmt.Errorf("the column list of table %q.%q cannot be read: a quote is not closed", schema, name)
	}
	for _, line := range lines {
		line, isColumn := strings.CutPrefix(line, "\t")
		if !isColumn {
			continue
		}
		_, text, carries := strings.Cut(line, " ON UPDATE ")
		if !carries {
			continue
		}
		column, _, ok := cutIdentifier(line)
		if !ok {
			return tableOnUpdates{}, fmt.Errorf("the column name of %q in table %q.%q cannot be read", line, schema, name)
		}
		table.clauses = append(table.clauses, onUpdateClause{column: column, text: strings.TrimSuffix(text, ",")})
	}
	return table, nil
}

// definitionLines cuts the column list of a CREATE TABLE statement into its
// lines, at each newline that stands outside a quote. A double quote opens an
// identifier, which CockroachDB prints with any newline it holds; a single
// quote opens a string, and one after an e opens an escape string, where a
// backslash escapes the next character, as in e'it\'s'. A doubled quote closes
// its quote and opens it again, which reads it whole. It reports false when a
// quote is still open at the end of the text.
func definitionLines(text string) ([]string, bool) {
	const (
		unquoted = iota
		identifier
		literal
		escapeLiteral
	)
	var lines []string
	state, start := unquoted, 0
	for i := 0; i < len(text); i++ {
		switch c := text[i]; state {
		case unquoted:
			switch {
			case c == '\n':
				lines = append(lines, text[start:i])
				start = i + 1
			case c == '"':
				state = identifier
			case c == '\'' && opensEscapeLiteral(text[:i]):
				state = escapeLiteral
			case c == '\'':
				state = literal
			}
		case identifier:
			if c == '"' {
				state = unquoted
			}
		case literal:
			if c == '\'' {
				state = unquoted
			}
		case escapeLiteral:
			switch c {
			case '\\':
				i++
			case '\'':
				state = unquoted
			}
		}
	}
	return append(lines, text[start:]), state == unquoted
}

// opensEscapeLiteral reports whether a quote after before opens an escape
// string: before ends in an e that is a word of its own.
func opensEscapeLiteral(before string) bool {
	rest, found := strings.CutSuffix(before, "e")
	if !found {
		rest, found = strings.CutSuffix(before, "E")
	}
	if !found {
		return false
	}
	if rest == "" {
		return true
	}
	last := rest[len(rest)-1]
	return last != '_' && last != '"' && (last < '0' || last > '9') && (last < 'a' || last > 'z') && (last < 'A' || last > 'Z')
}

// cutIdentifier cuts the identifier s starts with, as CockroachDB prints one:
// in double quotes with each quote inside doubled, or bare up to a space, a
// dot or a parenthesis.
func cutIdentifier(s string) (name, rest string, ok bool) {
	quoted, isQuoted := strings.CutPrefix(s, `"`)
	if !isQuoted {
		end := strings.IndexAny(s, " .(")
		if end <= 0 {
			return "", s, false
		}
		return s[:end], s[end:], true
	}
	var b strings.Builder
	for {
		before, after, found := strings.Cut(quoted, `"`)
		if !found {
			return "", s, false
		}
		b.WriteString(before)
		next, doubled := strings.CutPrefix(after, `"`)
		if !doubled {
			return b.String(), after, true
		}
		b.WriteByte('"')
		quoted = next
	}
}
