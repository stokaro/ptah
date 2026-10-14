// Package privilegerender renders owner-validated default-privilege operations
// for the PostgreSQL family. It consumes captured state without database I/O.
package privilegerender

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/sqlident"
)

// Handlers returns a fresh handler for the owner's statement payload.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&pgdefaultprivilege.Operation{}, ast.StatementExtension, validate, render)}
}

// Registry returns an independent statement registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validate(ctx renderer.ExtensionContext, operation *pgdefaultprivilege.Operation) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgdefaultprivilege.OperationKind, ast.StatementExtension)
	}
	return operation.Validate()
}

func render(_ renderer.ExtensionContext, operation *pgdefaultprivilege.Operation) ([]string, error) {
	names := append([]string(nil), operation.Privileges...)
	for i := range names {
		names[i] = strings.ToUpper(strings.TrimSpace(names[i]))
	}
	parts := []string{"ALTER DEFAULT PRIVILEGES FOR ROLE", role(operation.Identity.Grantor)}
	if operation.Identity.Schema != "" {
		parts = append(parts, "IN SCHEMA", quote(operation.Identity.Schema))
	}
	verb, recipient := "GRANT", "TO"
	if operation.Action != pgdefaultprivilege.GrantRights {
		verb, recipient = "REVOKE", "FROM"
	}
	if operation.Action == pgdefaultprivilege.RevokeGrantOptions {
		verb = "REVOKE GRANT OPTION FOR"
	}
	parts = append(parts, verb, strings.Join(names, ", "), "ON", operation.Identity.ObjectType, recipient, role(operation.Identity.Grantee))
	if operation.WithOption {
		parts = append(parts, "WITH GRANT OPTION")
	}
	return []string{strings.Join(parts, " ") + ";"}, nil
}

func role(name string) string {
	switch strings.ToUpper(name) {
	case "PUBLIC", "CURRENT_ROLE", "CURRENT_USER", "SESSION_USER":
		return strings.ToUpper(name)
	default:
		return quote(name)
	}
}

func quote(name string) string {
	return sqlident.Quote(platform.Postgres, name)
}
