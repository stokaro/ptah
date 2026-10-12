// Package extensionrender validates and renders the PostgreSQL extension
// operation payloads of package pgextension for the PostgreSQL family. It
// reads captured target facts only and never opens a database connection.
//
// Capability gating of a comment stays with the PostgreSQL-family renderer:
// the comment payload names the capability it needs and the object a skip
// line names, and a target without the key writes the skip and records the
// omission, as it does for its own capability-gated objects. CockroachDB and
// Spanner accept CREATE EXTENSION and nothing else about one, so a creation
// that names a schema and every alteration are refused there.
package extensionrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/sqlident"
)

// Handlers returns independent statement handlers for both payloads, for a
// composition that joins them with other owners' handlers.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&pgextension.Operation{}, ast.StatementExtension, validateOperation, renderOperation),
		renderer.TypedHandler(&pgextension.CommentOperation{}, ast.StatementExtension, validateComment, renderComment),
	}
}

// Registry returns the statement handlers for both payloads. Each call
// returns an independent registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

// HostsAlterations reports whether a target speaks ALTER EXTENSION and
// CREATE EXTENSION ... WITH SCHEMA. PostgreSQL and YugabyteDB do; CockroachDB
// and Spanner create an extension and do nothing else with one.
func HostsAlterations(target string) bool {
	switch platform.NormalizeDialect(target) {
	case platform.Postgres, platform.YugabyteDB:
		return true
	default:
		return false
	}
}

func validateOperation(ctx renderer.ExtensionContext, value *pgextension.Operation) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgextension.OperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	if HostsAlterations(ctx.Target) {
		return nil
	}
	switch {
	case value.Action == pgextension.Create && value.Schema != "":
		return UnsupportedInstallationSchema(ctx.Target, value.Name, value.Schema)
	case value.Action == pgextension.UpdateVersion || value.Action == pgextension.SetSchema:
		target := platform.NormalizeDialect(ctx.Target)
		return &ptaherr.CapabilityError{Dialect: target, Feature: "ALTER EXTENSION", Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s cannot alter PostgreSQL extension %q; this target has no ALTER EXTENSION", target, value.Name)}
	default:
		return nil
	}
}

// UnsupportedInstallationSchema is the refusal of an extension installed
// into a named schema on a target that has no such placement.
func UnsupportedInstallationSchema(target, name, schema string) error {
	normalized := platform.NormalizeDialect(target)
	return &ptaherr.CapabilityError{Dialect: normalized, Feature: "PostgreSQL extension installation schemas", Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s does not support PostgreSQL extension installation schema %q for extension %q", normalized, schema, name)}
}

func validateComment(ctx renderer.ExtensionContext, value *pgextension.CommentOperation) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgextension.CommentOperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

// renderOperation writes one statement about one extension. Each spelling was
// measured on PostgreSQL 18: `ALTER EXTENSION pg_trgm UPDATE TO '1.6'` is
// idempotent, answering a NOTICE when the version is installed already, and
// so is `ALTER EXTENSION pg_trgm SET SCHEMA public` on the schema the
// extension occupies (stokaro/ptah#1718).
func renderOperation(_ renderer.ExtensionContext, value *pgextension.Operation) ([]string, error) {
	name := quote(value.Name)
	switch value.Action {
	case pgextension.UpdateVersion:
		return []string{"ALTER EXTENSION " + name + " UPDATE TO " + sqlident.StringLiteral(value.Version) + ";"}, nil
	case pgextension.SetSchema:
		return []string{"ALTER EXTENSION " + name + " SET SCHEMA " + quote(value.Schema) + ";"}, nil
	case pgextension.Drop:
		return []string{dropStatement(value)}, nil
	default:
		return []string{CreateStatement(value)}, nil
	}
}

// dropStatement writes DROP EXTENSION for a drop, with IF EXISTS and CASCADE
// where the operation names them.
func dropStatement(value *pgextension.Operation) string {
	parts := []string{"DROP EXTENSION"}
	if value.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, quote(value.Name))
	if value.Cascade {
		parts = append(parts, "CASCADE")
	}
	return strings.Join(parts, " ") + ";"
}

// CreateStatement writes CREATE EXTENSION for a creation. An extension's name
// is one database-wide identifier: a dot in it separates nothing, and its
// placement is the separate WITH SCHEMA clause.
func CreateStatement(value *pgextension.Operation) string {
	parts := []string{"CREATE EXTENSION"}
	if value.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	parts = append(parts, quote(value.Name))
	if value.Schema != "" {
		parts = append(parts, "WITH SCHEMA", quote(value.Schema))
	}
	if value.Version != "" {
		parts = append(parts, "VERSION", sqlident.StringLiteral(value.Version))
	}
	return strings.Join(parts, " ") + ";"
}

func renderComment(_ renderer.ExtensionContext, value *pgextension.CommentOperation) ([]string, error) {
	literal := "NULL"
	if value.Comment != "" {
		literal = sqlident.StringLiteral(value.Comment)
	}
	return []string{"COMMENT ON EXTENSION " + quote(value.Name) + " IS " + literal + ";"}, nil
}

func quote(name string) string {
	return sqlident.Quote(platform.Postgres, sqlident.UnquoteDoubleQuoted(name))
}
