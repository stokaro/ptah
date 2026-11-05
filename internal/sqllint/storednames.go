package sqllint

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/internal/sqlschema"
)

const (
	// RuleIdentifierTruncated reports a name longer than the identifier limit
	// of a server that cuts long names. The server stores the object under its
	// first bytes, with a notice and no error.
	RuleIdentifierTruncated = "DDL003"
	// RuleIdentifierCollision reports a name the server cuts to a name already
	// in use where the server refuses the second declaration.
	//
	// It is a code of its own rather than an error-grade DDL003 because one
	// code carries one default severity: a cut name is a warning a project
	// may promote, and a collision fails when the statement runs.
	RuleIdentifierCollision = "DDL004"
)

// storedNameTitles are the short labels of the two rules, read by the catalog
// and by every finding they emit.
const (
	identifierTruncatedTitle = "Identifier longer than the server keeps"
	identifierCollisionTitle = "Identifiers the server stores as one"
)

// storedNameFindings follows the names the file declares, statement by
// statement, on a dialect whose server cuts long names, and returns each
// statement's findings at the position of items, and the nodes whose names
// the rules judged.
//
// The analysis is the SQL reader's (sqlschema.NameFollower), so lint reports
// the names the reader records objects under. It runs over the whole file
// rather than one statement at a time, because a collision is between two
// statements: an index cut to the name of a table the file created earlier.
// A dialect whose descriptor does not say its server cuts long names gets
// nothing, which is every dialect but the PostgreSQL family's measured ones.
func storedNameFindings(source Source, items []sourceItem, opts Options) ([][]Finding, map[ast.Node]bool) {
	follower, cuts := sqlschema.NewNameFollower(opts.Dialect)
	if !cuts {
		return nil, nil
	}
	findings := make([][]Finding, len(items))
	judged := make(map[ast.Node]bool)
	for i, item := range items {
		for _, node := range item.nodes {
			names := follower.Follow(node)
			if names.Declares {
				judged[node] = true
			}
			for _, name := range names.Cut {
				findings[i] = append(findings[i], truncatedFinding(source, item.statement, opts, name, ""))
			}
			for _, collision := range names.Collisions {
				if finding, reported := collisionFinding(source, item.statement, opts, collision); reported {
					findings[i] = append(findings[i], finding)
				}
			}
		}
	}
	return findings, judged
}

// collisionFinding is the finding for a name stored as one already in use.
//
// The server refuses the statement unless it is guarded, which is DDL004. A
// guarded statement it skips under IF NOT EXISTS, or applies to the object
// it finds under OR REPLACE: nothing fails, and one of the two objects the
// file names is not created. That is reported as the cut it is, with what
// the server does about it. A guarded name written as the server stores it
// names the object that holds the name, as SQL may, and is not reported.
func collisionFinding(source Source, statement sourceStatement, opts Options, collision sqlschema.NameCollision) (Finding, bool) {
	name := collision.Name
	if collision.Refused {
		line, column := namePosition(source, statement, opts, name.Written)
		return Finding{
			Rule:     RuleIdentifierCollision,
			Title:    identifierCollisionTitle,
			Severity: SeverityError,
			File:     source.Name,
			Line:     line,
			Column:   column,
			Dialect:  opts.Dialect,
			Message: fmt.Sprintf("%s %q and %s %q in %s are both stored as %q, and the server refuses the second",
				collision.HeldBy, collision.HeldAs, name.Kind, name.Written, name.Scope, name.Stored),
			Rationale: "The server cuts a long name to its first bytes, so two names that differ only after them " +
				"take one name, and the second statement fails when it runs.",
			SuggestedFix: fmt.Sprintf("Rename one of them so the two differ within the first %d bytes.", name.Limit),
		}, true
	}
	if name.Written == name.Stored {
		return Finding{}, false
	}
	note := fmt.Sprintf("; %s %q already holds that name, so the server keeps one of the two objects",
		collision.HeldBy, collision.HeldAs)
	return truncatedFinding(source, statement, opts, name, note), true
}

// truncatedFinding is the finding for one name the server cuts, with note
// appended to the message.
func truncatedFinding(source Source, statement sourceStatement, opts Options, name sqlschema.StoredName, note string) Finding {
	line, column := namePosition(source, statement, opts, name.Written)
	return Finding{
		Rule:     RuleIdentifierTruncated,
		Title:    identifierTruncatedTitle,
		Severity: SeverityWarning,
		File:     source.Name,
		Line:     line,
		Column:   column,
		Dialect:  opts.Dialect,
		Message: fmt.Sprintf("%s %q in %s is %d bytes, longer than the %d bytes the server keeps; it is stored as %q%s",
			name.Kind, name.Written, name.Scope, len(name.Written), name.Limit, name.Stored, note),
		Rationale: "The server cuts the name with a notice and no error, so the database holds a name the SQL " +
			"never wrote, and later SQL, a schema diff or a second name of the same first bytes meets that one.",
		SuggestedFix: fmt.Sprintf("Write a name of at most %d bytes, such as %q, so the SQL names what the database holds.",
			name.Limit, name.Stored),
	}
}

// namePosition is the line and column of the name written in statement, read
// from its tokens, or of the statement's first keyword when its tokens do not
// spell the name, as for a name only an earlier statement wrote.
func namePosition(source Source, statement sourceStatement, opts Options, written string) (line, column int) {
	offset, found := sqlschema.LocateName(opts.Dialect, statement.sql, written)
	if !found {
		_, offset = firstKeyword(statement.sql)
	}
	return lineColumn(source.SQL, statement.offset+offset)
}
