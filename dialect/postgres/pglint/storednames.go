package pglint

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// atlasCompatibility is the surface ptah-compat reports on, which neither
// identifier rule reports on: the community binary reports no such finding,
// and an error-grade one would change the exit code of `migrate lint` on a
// directory it passes.
const atlasCompatibility = "atlas"

// identifierRules are PG401P and PG402P, which report the names PostgreSQL
// cuts to its identifier limit and the cut names it refuses as already in use.
//
// They read the SQL reader's analysis (sqlschema.NameFollower), the one the
// reader records objects under, over the whole directory: an up file is judged
// from the names the up files before it declare, and a down file from the
// names the up files through its version declare, which is where a rollback
// starts. Each rule follows the directory for itself, so a run that enables
// one parses it once.
func identifierRules() []lintkit.Rule {
	return []lintkit.Rule{
		{
			Rule: lintext.Rule{
				Code:          "PG401P",
				Title:         "identifier longer than the server keeps",
				Severity:      lintext.Warning,
				Targets:       []string{"postgres"},
				AppliesToDown: true,
			},
			Directory: func(files []lintkit.File) [][]lintext.Violation {
				return storedNameViolations(files, truncationViolations)
			},
		},
		{
			Rule: lintext.Rule{
				Code:          "PG402P",
				Title:         "identifiers the server stores as one",
				Severity:      lintext.Error,
				Targets:       []string{"postgres"},
				AppliesToDown: true,
			},
			Directory: func(files []lintkit.File) [][]lintext.Violation {
				return storedNameViolations(files, collisionViolations)
			},
		},
	}
}

// storedNameViolations follows the directory and reports what judge makes of
// each statement's names, by file.
func storedNameViolations(files []lintkit.File, judge func(statement int, names sqlschema.StatementNames) []lintext.Violation) [][]lintext.Violation {
	violations := make([][]lintext.Violation, len(files))
	for index, statements := range followDirectory(files) {
		for _, followed := range statements {
			violations[index] = append(violations[index], judge(followed.statement, followed.names)...)
		}
	}
	return violations
}

// followedStatement is what one statement of a file did to the names the
// server keeps.
type followedStatement struct {
	statement int
	names     sqlschema.StatementNames
}

// followDirectory follows the names the files declare, by file: the up files
// in request order, which is the order the directory applies them in, and each
// down file from a copy of the names the up files through its version leave.
// A run on the compatibility surface, and one whose target keeps long names
// whole, follows nothing.
func followDirectory(files []lintkit.File) [][]followedStatement {
	followed := make([][]followedStatement, len(files))
	if len(files) == 0 || files[0].Compatibility == atlasCompatibility {
		return followed
	}
	dialect := files[0].Target.Name
	follower, cuts := sqlschema.NewNameFollower(dialect)
	if !cuts {
		return followed
	}
	var downs []int
	for index := range files {
		if files[index].IsDown {
			downs = append(downs, index)
		}
	}
	slices.SortStableFunc(downs, func(a, b int) int { return cmp.Compare(files[a].Version, files[b].Version) })
	next := 0
	for index := range files {
		if !files[index].IsUp {
			continue
		}
		for ; next < len(downs) && files[downs[next]].Version < files[index].Version; next++ {
			followed[downs[next]] = followFile(follower.Clone(), &files[downs[next]], dialect)
		}
		followed[index] = followFile(follower, &files[index], dialect)
	}
	for ; next < len(downs); next++ {
		followed[downs[next]] = followFile(follower.Clone(), &files[downs[next]], dialect)
	}
	return followed
}

// followFile follows the statements of one file and returns those that cut
// or collide. A statement the shared grammar does not parse declares nothing
// the rules can read, and is passed over.
func followFile(follower *sqlschema.NameFollower, file *lintkit.File, dialect string) []followedStatement {
	var followed []followedStatement
	for index := range file.Statements {
		statements, err := parser.NewParser(terminated(file.Statements[index].SQL),
			parser.WithDialect(dialect), parser.WithSQL(sqlext.None())).Parse()
		if err != nil {
			continue
		}
		for _, statement := range statements.Statements {
			names := follower.Follow(statement)
			if len(names.Cut) > 0 || len(names.Collisions) > 0 {
				followed = append(followed, followedStatement{statement: index, names: names})
			}
		}
	}
	return followed
}

// terminated is sql ending in a semicolon, which the parser reads a statement
// up to.
func terminated(sql string) string {
	if strings.HasSuffix(strings.TrimSpace(sql), ";") {
		return sql
	}
	return sql + ";"
}

// truncationViolations is PG401P for one statement: each name it declares
// that the server cuts, and each it writes past the limit under IF NOT EXISTS
// or OR REPLACE that is then stored as a name in use, where the server keeps
// one of the two objects without an error. A name written as the server
// stores it names the object that holds the name, and is not reported.
func truncationViolations(statement int, names sqlschema.StatementNames) []lintext.Violation {
	var violations []lintext.Violation
	for _, name := range names.Cut {
		violations = append(violations, lintext.Violation{Statement: statement, Message: truncationMessage(name, "")})
	}
	for _, collision := range names.Collisions {
		if collision.Refused || collision.Name.Written == collision.Name.Stored {
			continue
		}
		note := fmt.Sprintf("; %s %q already holds that name, so PostgreSQL keeps one of the two objects",
			collision.HeldBy, collision.HeldAs)
		violations = append(violations, lintext.Violation{Statement: statement, Message: truncationMessage(collision.Name, note)})
	}
	return violations
}

// truncationMessage says what the server stores name as, with note, and how
// to write the name instead.
func truncationMessage(name sqlschema.StoredName, note string) string {
	return fmt.Sprintf("%s %q in %s is %d bytes, longer than the %d bytes PostgreSQL keeps; it is stored as %q%s; "+
		"write a name of at most %d bytes, such as %q, so the migration names what the database holds",
		name.Kind, name.Written, name.Scope, len(name.Written), name.Limit, name.Stored, note, name.Limit, name.Stored)
}

// collisionViolations is PG402P for one statement: each name it writes that
// the server stores as a name already in use, where the server refuses the
// statement.
func collisionViolations(statement int, names sqlschema.StatementNames) []lintext.Violation {
	var violations []lintext.Violation
	for _, collision := range names.Collisions {
		if !collision.Refused {
			continue
		}
		name := collision.Name
		violations = append(violations, lintext.Violation{Statement: statement, Message: fmt.Sprintf(
			"%s %q and %s %q in %s are both stored as %q, and PostgreSQL refuses the second; "+
				"rename one of them so the two differ within the first %d bytes",
			collision.HeldBy, collision.HeldAs, name.Kind, name.Written, name.Scope, name.Stored, name.Limit)})
	}
	return violations
}
