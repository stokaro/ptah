// Package typerender validates and renders the PostgreSQL user-type operation
// payloads of package pgtype for the PostgreSQL family. It reads captured
// target facts only and never opens a database connection.
//
// Capability gating stays with the PostgreSQL-family renderer: a creation, an
// alteration and a comment each name the capability they need and the object
// a skip line names, and a target without the key writes the skip and records
// the omission, as it does for its own capability-gated objects. A drop needs
// no capability, so a type a target holds can always be dropped.
//
// Each statement is the one the PostgreSQL-family renderer wrote for the
// common user types: the same quoting, the same clause order, and a literal
// default written in the form the base type takes.
package typerender

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/defaultlit"
	"ptah.run/internal/sqlident"
)

// Handlers returns independent statement handlers for the seven payloads, for
// a composition that joins them with other owners' handlers.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&pgtype.CreateDomain{}, ast.StatementExtension, validator[*pgtype.CreateDomain](pgtype.CreateDomainKind), renderCreateDomain),
		renderer.TypedHandler(&pgtype.AlterDomain{}, ast.StatementExtension, validator[*pgtype.AlterDomain](pgtype.AlterDomainKind), renderAlterDomain),
		renderer.TypedHandler(&pgtype.CreateComposite{}, ast.StatementExtension, validator[*pgtype.CreateComposite](pgtype.CreateCompositeKind),
			renderCreateComposite),
		renderer.TypedHandler(&pgtype.AlterComposite{}, ast.StatementExtension, validator[*pgtype.AlterComposite](pgtype.AlterCompositeKind),
			renderAlterComposite),
		renderer.TypedHandler(&pgtype.CreateRange{}, ast.StatementExtension, validator[*pgtype.CreateRange](pgtype.CreateRangeKind), renderCreateRange),
		renderer.TypedHandler(&pgtype.Drop{}, ast.StatementExtension, validator[*pgtype.Drop](pgtype.DropKind), renderDrop),
		renderer.TypedHandler(&pgtype.Comment{}, ast.StatementExtension, validator[*pgtype.Comment](pgtype.CommentKind), renderComment),
	}
}

// Registry returns the statement handlers for the seven payloads. Each call
// returns an independent registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

// validator refuses a payload outside the PostgreSQL family and a payload
// its own validation refuses.
func validator[P interface{ Validate() error }](kind schemaext.Kind) func(renderer.ExtensionContext, P) error {
	return func(ctx renderer.ExtensionContext, value P) error {
		if !platform.IsPostgresFamily(ctx.Target) {
			return renderer.UnsupportedExtension(ctx.Target, kind, ast.StatementExtension)
		}
		if err := value.Validate(); err != nil {
			return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
		}
		return nil
	}
}

// name is a type's qualified name, each part quoted.
func name(target pgtype.Target) string {
	return sqlident.QuotePostgresQualified(target.QualifiedName())
}

// identifier is one attribute's or constraint's name, quoted for target.
func identifier(target, value string) string {
	return sqlident.Quote(target, value)
}

// renderCreateDomain writes CREATE DOMAIN with NOT NULL, the default and the
// CHECK in that order. A default expression is written as it is, and a
// literal one in the form the base type takes: bare for a number or a
// boolean, quoted otherwise.
func renderCreateDomain(_ renderer.ExtensionContext, value *pgtype.CreateDomain) ([]string, error) {
	statement := "CREATE DOMAIN " + name(value.Target) + " AS " + value.BaseType
	if value.NotNull {
		statement += " NOT NULL"
	}
	switch {
	case value.DefaultExpr != "":
		statement += " DEFAULT " + value.DefaultExpr
	case value.Default != "":
		statement += " DEFAULT " + defaultlit.Typed(value.BaseType, value.Default, sqlident.StringLiteral)
	}
	if value.Check != "" {
		statement += " CHECK (" + value.Check + ")"
	}
	return []string{statement + ";"}, nil
}

// renderAlterDomain writes one ALTER DOMAIN per change, in the order the
// payload documents. A replaced CHECK drops the old constraint before adding
// the new one, and NOT NULL is set after the CHECK it may depend on.
func renderAlterDomain(ctx renderer.ExtensionContext, value *pgtype.AlterDomain) ([]string, error) {
	domain := name(value.Target)
	statements := make([]string, 0, len(value.DropChecks)+3)
	for _, constraint := range value.DropChecks {
		statements = append(statements, "ALTER DOMAIN "+domain+" DROP CONSTRAINT "+identifier(ctx.Target, constraint)+";")
	}
	if value.AddCheck != "" {
		constraint := ""
		if value.CheckName != "" {
			constraint = "CONSTRAINT " + identifier(ctx.Target, value.CheckName) + " "
		}
		statements = append(statements, "ALTER DOMAIN "+domain+" ADD "+constraint+"CHECK ("+value.AddCheck+");")
	}
	if value.DropDefault {
		statements = append(statements, "ALTER DOMAIN "+domain+" DROP DEFAULT;")
	}
	if value.SetDefault != "" {
		statements = append(statements, "ALTER DOMAIN "+domain+" SET DEFAULT "+value.SetDefault+";")
	}
	if value.NotNull != nil {
		action := "DROP"
		if *value.NotNull {
			action = "SET"
		}
		statements = append(statements, "ALTER DOMAIN "+domain+" "+action+" NOT NULL;")
	}
	return statements, nil
}

// renderCreateComposite writes CREATE TYPE ... AS with the attributes in
// order, each name quoted and each type as written.
func renderCreateComposite(ctx renderer.ExtensionContext, value *pgtype.CreateComposite) ([]string, error) {
	attributes := make([]string, len(value.Attributes))
	for i, attribute := range value.Attributes {
		attributes[i] = identifier(ctx.Target, attribute.Name) + " " + attribute.Type
	}
	return []string{"CREATE TYPE " + name(value.Target) + " AS (" + strings.Join(attributes, ", ") + ");"}, nil
}

// renderAlterComposite writes one ALTER TYPE per attribute: the drops first,
// so a reused name does not collide with the one still there, then the
// additions.
func renderAlterComposite(ctx renderer.ExtensionContext, value *pgtype.AlterComposite) ([]string, error) {
	typeName := name(value.Target)
	statements := make([]string, 0, len(value.Drop)+len(value.Add))
	for _, attribute := range value.Drop {
		statements = append(statements, "ALTER TYPE "+typeName+" DROP ATTRIBUTE "+identifier(ctx.Target, attribute)+";")
	}
	for _, attribute := range value.Add {
		statements = append(statements, "ALTER TYPE "+typeName+" ADD ATTRIBUTE "+identifier(ctx.Target, attribute.Name)+" "+attribute.Type+";")
	}
	return statements, nil
}

// renderCreateRange writes CREATE TYPE ... AS RANGE with the subtype and the
// optional attributes the payload names, in the order PostgreSQL documents
// them.
func renderCreateRange(_ renderer.ExtensionContext, value *pgtype.CreateRange) ([]string, error) {
	options := []string{"SUBTYPE = " + value.Subtype}
	for _, option := range []struct{ keyword, value string }{
		{"SUBTYPE_OPCLASS", value.SubtypeOpClass}, {"COLLATION", value.Collation},
		{"CANONICAL", value.Canonical}, {"SUBTYPE_DIFF", value.SubtypeDiff},
	} {
		if option.value != "" {
			options = append(options, option.keyword+" = "+option.value)
		}
	}
	return []string{"CREATE TYPE " + name(value.Target) + " AS RANGE (" + strings.Join(options, ", ") + ");"}, nil
}

// renderDrop writes DROP DOMAIN for a domain and DROP TYPE otherwise, IF
// EXISTS, with CASCADE where the payload asks for it.
func renderDrop(_ renderer.ExtensionContext, value *pgtype.Drop) ([]string, error) {
	statement := "DROP " + objectKeyword(value.Target) + " IF EXISTS " + name(value.Target)
	if value.Cascade {
		statement += " CASCADE"
	}
	return []string{statement + ";"}, nil
}

// renderComment writes COMMENT ON DOMAIN or COMMENT ON TYPE, with NULL for an
// empty comment.
func renderComment(_ renderer.ExtensionContext, value *pgtype.Comment) ([]string, error) {
	literal := "NULL"
	if value.Comment != "" {
		literal = sqlident.StringLiteral(value.Comment)
	}
	return []string{"COMMENT ON " + objectKeyword(value.Target) + " " + name(value.Target) + " IS " + literal + ";"}, nil
}

// objectKeyword is the keyword a statement names the type's kind with.
func objectKeyword(target pgtype.Target) string {
	if target.Kind == pgtype.DomainKind {
		return "DOMAIN"
	}
	return "TYPE"
}
