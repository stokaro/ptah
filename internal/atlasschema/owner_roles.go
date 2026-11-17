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
	// Kind is the kind of object, as ALTER spells it.
	Kind string
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

// OwnerTransferError is the refusal of a plan that gives an object to a role
// the connection that runs it may not give an object to, issued before the
// plan sends a statement. The server checks the same privilege at the ALTER
// ... OWNER TO and refuses there with SQLSTATE 42501 only once the statements
// before it have run; a view the plan drops and creates again has been dropped
// by then, and the plan rolls back.
type OwnerTransferError struct {
	// Statement is the ownership statement, as the plan writes it.
	Statement string
	// Kind is the kind of object, as ALTER spells it.
	Kind string
	// Role is the role the statement gives the object to, as the server
	// would store the name.
	Role string
	// Connected is the role the connection runs as.
	Connected string
	// Privilege is the role privilege the server requires of Connected on
	// Role: SET, or MEMBER where the server checks membership.
	Privilege string
	// Holds reports that Role owns the object now, so the statement gives it
	// back after the plan creates it again.
	Holds bool
	// Hint says what the caller can do about it, when the place the plan runs
	// knows more than the statement does. It may be empty.
	Hint string
}

// Error implements error.
func (e *OwnerTransferError) Error() string {
	kind := strings.ToLower(e.Kind)
	grant := "GRANT " + quoteOwnerName(e.Role) + " TO " + quoteOwnerName(e.Connected)
	requires := fmt.Sprintf("the server requires membership in the new owner, and %q is not a member", e.Connected)
	if e.Privilege == "SET" {
		grant += " WITH SET TRUE"
		requires = fmt.Sprintf("the server requires the SET privilege on the new owner, and %q lacks it", e.Connected)
	}
	message := fmt.Sprintf("%s gives the %s to role %q, which connected role %q may not give an object to: %s",
		strings.TrimSuffix(strings.TrimSpace(e.Statement), ";"), kind, e.Role, e.Connected, requires)
	return message + ownerWaysOut{kind: kind, role: e.Role, holds: e.Holds, grant: grant,
		other: fmt.Sprintf("one %q may give it to", e.Connected), hint: e.Hint}.String()
}

// OwnerSchemaError is the refusal of a plan that gives an object to a role
// that holds no CREATE privilege on the object's schema, which the server
// requires of a new owner unless the connection is a PostgreSQL superuser. Like
// [OwnerTransferError], it is issued before the plan sends a statement, where
// the server would refuse the ALTER ... OWNER TO with SQLSTATE 42501 after
// the statements before it ran.
type OwnerSchemaError struct {
	// Statement is the ownership statement, as the plan writes it.
	Statement string
	// Kind is the kind of object, as ALTER spells it.
	Kind string
	// Role is the role the statement gives the object to, as the server
	// would store the name.
	Role string
	// Schema is the schema the object is in.
	Schema string
	// Holds reports that Role owns the object now, so the statement gives it
	// back after the plan creates it again.
	Holds bool
	// Hint says what the caller can do about it, when the place the plan runs
	// knows more than the statement does. It may be empty.
	Hint string
}

// Error implements error.
func (e *OwnerSchemaError) Error() string {
	kind := strings.ToLower(e.Kind)
	message := fmt.Sprintf("%s gives the %s to role %q, which holds no CREATE privilege on schema %q: "+
		"the server requires it of the new owner",
		strings.TrimSuffix(strings.TrimSpace(e.Statement), ";"), kind, e.Role, e.Schema)
	grant := "GRANT CREATE ON SCHEMA " + quoteOwnerName(e.Schema) + " TO " + quoteOwnerName(e.Role)
	return message + ownerWaysOut{kind: kind, role: e.Role, holds: e.Holds, grant: grant, other: "one that holds it", hint: e.Hint}.String()
}

// ownerWaysOut is the tail both owner refusals share.
type ownerWaysOut struct {
	// kind is the kind of object in lower case, and role the role the
	// statement gives it to.
	kind, role string
	// holds reports that role owns the object now.
	holds bool
	// grant is the statement that lets the plan run, and other the owner a
	// declaration may name instead.
	grant, other string
	// hint is the refusal's hint, or empty.
	hint string
}

// String is that role owns the object now, where it does, then the grant and
// the declaration, and the hint.
func (w ownerWaysOut) String() string {
	var tail string
	if w.holds {
		tail = fmt.Sprintf("; %q owns the %s now, and the plan gives it back after it creates the %s again", w.role, w.kind, w.kind)
	}
	tail += fmt.Sprintf("; run %s, or declare the owner the %s should have, %s", w.grant, w.kind, w.other)
	if w.hint != "" {
		tail += "; " + w.hint
	}
	return tail
}

// quoteOwnerName writes a role's or a schema's stored name as a quoted
// identifier.
func quoteOwnerName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// serverRoleLister is a writer that lists the roles of its server.
type serverRoleLister interface {
	ServerRoles(context.Context) ([]string, error)
}

// ownerTransferChecker is a writer that can ask its server, before a plan
// runs, which roles its connection may not give an object to, and what it
// holds of a relation: its owner, and whether a role may own an object in
// its schema.
type ownerTransferChecker interface {
	OwnerTransfers(context.Context) (objectowner.Transfers, error)
	Relation(ctx context.Context, written, role string) (objectowner.Relation, error)
}

// plannedOwner is one ownership statement of a plan: the statement, the kind
// of object and its name as the statement writes them, and the role it names.
type plannedOwner struct {
	statement, kind, name, role string
}

// refuseMissingOwners refuses statements, about to run on conn, when one gives
// a view or a materialized view to a role the server does not hold and no
// CREATE ROLE before it creates, or to a role the server would refuse as the
// new owner when the statement runs; see [refuseOwnerTransfers]. A writer
// that cannot list its server's roles, or cannot ask what its connection may
// give, is not asked, and the server decides as it runs the statement; so is
// a role a statement before it creates, which the server cannot answer about
// yet.
func refuseMissingOwners(ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) error {
	if conn == nil {
		return nil
	}
	dialect := conn.Info().Dialect
	owners, created := plannedOwners(dialect, statements)
	if len(owners) == 0 {
		return nil
	}
	writer := conn.SchemaWriter()
	if lister, ok := writer.(serverRoleLister); ok {
		held, err := lister.ServerRoles(ctx)
		if err != nil {
			return err
		}
		for i, owner := range owners {
			exists := func(role string) bool { return objectowner.SameRole(dialect, role, owner.role) }
			if slices.ContainsFunc(held, exists) || slices.ContainsFunc(created[i], exists) {
				continue
			}
			return &MissingOwnerRoleError{Statement: owner.statement, Kind: owner.kind, Role: owner.role}
		}
	}
	checker, ok := writer.(ownerTransferChecker)
	if !ok {
		return nil
	}
	return refuseOwnerTransfers(ctx, dialect, checker, owners, created)
}

// refuseOwnerTransfers refuses the first of owners the server would refuse to
// give its object to: a role the connection may not give an object to, or,
// for an object the server holds and a connection the server does not exempt,
// a role that holds no CREATE privilege on the object's schema. It asks the
// server about the roles once, and about each object only where the answer
// can refuse it. The schema of an object the server does not hold yet is not
// asked about: the server refuses that owner as the statement runs, and the
// plan rolls back with nothing the server held dropped.
func refuseOwnerTransfers(ctx context.Context, dialect string, checker ownerTransferChecker, owners []plannedOwner, created [][]string) error {
	answer, err := checker.OwnerTransfers(ctx)
	if err != nil || answer.Privilege == "" {
		return err
	}
	for i, owner := range owners {
		same := func(role string) bool { return objectowner.SameRole(dialect, role, owner.role) }
		refused := answer.Refuses(dialect, owner.role)
		if slices.ContainsFunc(created[i], same) || (!refused && answer.SchemaExempt) {
			continue
		}
		relation, err := checker.Relation(ctx, owner.name, owner.role)
		if err != nil {
			return err
		}
		holds := relation.Owner != "" && same(relation.Owner)
		switch {
		case refused:
			return &OwnerTransferError{Statement: owner.statement, Kind: owner.kind, Role: owner.role,
				Connected: answer.Connected, Privilege: answer.Privilege, Holds: holds}
		case relation.Owner != "" && !relation.Creates:
			return &OwnerSchemaError{Statement: owner.statement, Kind: owner.kind, Role: owner.role, Schema: relation.Schema, Holds: holds}
		}
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
				owners = append(owners, plannedOwner{statement: statement, kind: n.Kind, name: n.Name, role: objectowner.StoredRole(dialect, n.Owner)})
				createdBefore = append(createdBefore, slices.Clone(created))
			}
		}
	}
	return owners, createdBefore
}
