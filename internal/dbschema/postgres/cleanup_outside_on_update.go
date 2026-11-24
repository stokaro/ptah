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
		table, isTable := onUpdateClauses(statement)
		if !isTable {
			continue
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

// onUpdateClauses reads a CREATE TABLE statement as SHOW CREATE ALL TABLES
// prints it: the schema and the name of the table, and each column whose line
// carries ON UPDATE. A column is a line that starts with a tab, and ON UPDATE
// is the last clause CockroachDB prints on it. The text runs from the first
// ON UPDATE on the line to its end, so a string in the default that holds the
// keyword widens the text rather than cutting the expression short. Any other
// statement reports false.
func onUpdateClauses(statement string) (tableOnUpdates, bool) {
	var table tableOnUpdates
	rest, isCreateTable := strings.CutPrefix(statement, "CREATE TABLE ")
	if !isCreateTable {
		return table, false
	}
	schema, rest, ok := cutIdentifier(rest)
	if !ok {
		return table, false
	}
	rest, ok = strings.CutPrefix(rest, ".")
	if !ok {
		return table, false
	}
	name, rest, ok := cutIdentifier(rest)
	if !ok {
		return table, false
	}
	table.schema, table.name = schema, name
	for line := range strings.SplitSeq(rest, "\n") {
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
			continue
		}
		table.clauses = append(table.clauses, onUpdateClause{column: column, text: strings.TrimSuffix(text, ",")})
	}
	return table, true
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
