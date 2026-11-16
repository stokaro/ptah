package devseed

import (
	"slices"

	"ptah.run/catalog"
)

// RolesNamed returns the roles of candidates that statements name, in the
// order candidates holds them. statements and the routines of state are read
// as [Closure] reads them: the code of each statement, what PostgreSQL
// resolves in a routine body when it is created, and the whole of a DO block.
//
// A role is named by a bare name, compared folded to lower case on both
// sides, so a quoted `"Admin"` and a role `admin` match each other. The rule
// errs toward naming: a column `reader` names a role `reader` too. A role
// named in a string, such as `pg_has_role('reader', 'USAGE')`, is not named,
// since the server resolves it only when the expression runs.
//
// Roles belong to the server rather than to one database, so [Closure] adds
// none. The caller decides whether the dev server may hold one the run
// creates, and passes the candidates it may.
func RolesNamed(state *catalog.Database, statements []string, candidates []catalog.Role, opts Options) []catalog.Role {
	if len(candidates) == 0 {
		return nil
	}
	var bodies []code
	if state != nil {
		bodies = routineBodies(state)
	}
	texts := slices.Concat(codeOf(statements), bodies, statementBodies(statements, opts.Dialect))
	words := readNames(texts, opts).words
	var named []catalog.Role
	for _, role := range candidates {
		if words[fold(role.Name)] {
			named = append(named, role)
		}
	}
	return named
}
