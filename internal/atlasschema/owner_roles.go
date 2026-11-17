package atlasschema

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/dbschema"
	"ptah.run/internal/objectowner"
	"ptah.run/internal/parser"
)

// MissingOwnerRoleError is the refusal of a plan that gives an object to a
// role the server does not hold, issued before the plan sends a statement.
// A role belongs to the server rather than to one database, and the server
// answers such a statement with SQLSTATE 42704 only once the statements before
// it have run.
type MissingOwnerRoleError struct {
	// Statement is the ownership statement, as the plan writes it.
	Statement string
	// Role is the role it names, as the server would store the name.
	Role string
	// Hint says what the caller can do about it, when the place the plan runs
	// knows more than the statement does. It may be empty.
	Hint string
}

// Error implements error.
func (e *MissingOwnerRoleError) Error() string {
	message := fmt.Sprintf(
		"%s gives the object to role %q, which the server does not hold and no statement before it creates; "+
			"a role belongs to the server, not to a database, so create the role on the server or declare it in the desired schema",
		strings.TrimSuffix(strings.TrimSpace(e.Statement), ";"), e.Role,
	)
	if e.Hint != "" {
		message += "; " + e.Hint
	}
	return message
}

// serverRoleLister is a writer that lists the roles of its server.
type serverRoleLister interface {
	ServerRoles(context.Context) ([]string, error)
}

// plannedOwner is one ownership statement of a plan and the role it names.
type plannedOwner struct {
	statement, role string
}

// refuseMissingOwners refuses statements, about to run on conn, when one gives
// a view or a materialized view to a role the server does not hold and no
// CREATE ROLE before it creates. A writer that cannot list its server's roles
// is not asked, and the server decides as it runs the statement.
func refuseMissingOwners(ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) error {
	if conn == nil {
		return nil
	}
	dialect := conn.Info().Dialect
	owners, created := plannedOwners(dialect, statements)
	if len(owners) == 0 {
		return nil
	}
	lister, ok := conn.SchemaWriter().(serverRoleLister)
	if !ok {
		return nil
	}
	held, err := lister.ServerRoles(ctx)
	if err != nil {
		return err
	}
	for i, owner := range owners {
		exists := func(role string) bool { return objectowner.SameRole(dialect, role, owner.role) }
		if slices.ContainsFunc(held, exists) || slices.ContainsFunc(created[i], exists) {
			continue
		}
		return &MissingOwnerRoleError{Statement: owner.statement, Role: owner.role}
	}
	return nil
}

// plannedOwners reads the ownership statements of statements, in order, and
// for each one the roles the statements before it create. A statement the
// parser of dialect does not read is not an ownership statement Ptah plans,
// and is left to the server.
func plannedOwners(dialect string, statements []string) (owners []plannedOwner, createdBefore [][]string) {
	var created []string
	for _, statement := range statements {
		upper := strings.ToUpper(statement)
		if !strings.Contains(upper, "OWNER") && !strings.Contains(upper, "ROLE") {
			continue
		}
		parsed, err := parser.NewParser(statement, parser.WithDialect(dialect)).Parse()
		if err != nil {
			continue
		}
		for _, node := range parsed.Statements {
			switch n := node.(type) {
			case *ast.CreateRoleNode:
				created = append(created, objectowner.StoredRole(dialect, n.Name))
			case *ast.AlterOwnerNode:
				owners = append(owners, plannedOwner{statement: statement, role: objectowner.StoredRole(dialect, n.Owner)})
				createdBefore = append(createdBefore, slices.Clone(created))
			}
		}
	}
	return owners, createdBefore
}
