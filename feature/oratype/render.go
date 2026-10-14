package oratype

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/internal/defaultlit"
)

// Handlers returns the statement handlers of the three operations, for the
// Oracle target, to join a composition with other owners' handlers.
//
// A statement the target's capabilities do not hold is refused rather than
// skipped: a comment in its place would let a render succeed on a schema whose
// tables then name a type the server has no definition of. Oracle 21 has no
// CREATE DOMAIN and answers ORA-00901.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&CreateDomain{}, ast.StatementExtension, validator[*CreateDomain](CreateDomainKind), renderCreateDomain),
		renderer.TypedHandler(&ReplaceObjectType{}, ast.StatementExtension, validator[*ReplaceObjectType](ReplaceObjectTypeKind),
			renderReplaceObjectType),
		renderer.TypedHandler(&Drop{}, ast.StatementExtension, validator[*Drop](DropKind), renderDrop),
	}
}

// validator refuses a payload on another target than Oracle, one its own
// validation refuses, and one the target's capabilities do not hold.
func validator[P interface {
	Validate() error
	RequiredCapability() capability.Capability
	statement() string
}](kind schemaext.Kind) func(renderer.ExtensionContext, P) error {
	return func(ctx renderer.ExtensionContext, value P) error {
		if !supported(ctx.Target) {
			return renderer.UnsupportedExtension(ctx.Target, kind, ast.StatementExtension)
		}
		if err := value.Validate(); err != nil {
			return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
		}
		if !ctx.Capabilities.Has(value.RequiredCapability()) {
			return fmt.Errorf("%w: oracle: %s: user types are not rendered for Oracle", ptaherr.ErrUnsupportedFeature,
				value.statement())
		}
		return nil
	}
}

// renderCreateDomain writes CREATE DOMAIN with NOT NULL, the default and the
// CHECK in that order, which Oracle 23 accepts as PostgreSQL writes it. The
// creation carries IF NOT EXISTS where the target has existence guards.
func renderCreateDomain(ctx renderer.ExtensionContext, value *CreateDomain) ([]string, error) {
	var lines []string
	if value.Comment != "" {
		lines = append(lines, "-- "+value.Comment)
	}
	statement := "CREATE DOMAIN" + createGuard(ctx) + " " + statementName(value.Target.Schema, value.Target.Name) + " AS " + value.BaseType
	if value.NotNull {
		statement += " NOT NULL"
	}
	switch {
	case value.DefaultExpr != "":
		statement += " DEFAULT " + value.DefaultExpr
	case value.Default != "":
		statement += " DEFAULT " + defaultlit.Render(value.Default, stringLiteral)
	}
	if strings.TrimSpace(value.Check) != "" {
		statement += " CHECK (" + value.Check + ")"
	}
	return append(lines, statement+";"), nil
}

// renderReplaceObjectType writes CREATE OR REPLACE TYPE ... AS OBJECT with the
// attributes in order. The OBJECT keyword is the whole difference from
// PostgreSQL's spelling: Oracle accepts `CREATE TYPE t AS (a NUMBER)` too and
// creates an incomplete, invalid type without reporting an error.
func renderReplaceObjectType(_ renderer.ExtensionContext, value *ReplaceObjectType) ([]string, error) {
	var lines []string
	if value.Comment != "" {
		lines = append(lines, "-- "+value.Comment)
	}
	attributes := make([]string, len(value.Attributes))
	for i, attribute := range value.Attributes {
		attributes[i] = identifierName(attribute.Name) + " " + strings.TrimSpace(attribute.Type)
	}
	return append(lines, "CREATE OR REPLACE TYPE "+statementName(value.Target.Schema, value.Target.Name)+
		" AS OBJECT ("+strings.Join(attributes, ", ")+");"), nil
}

// renderDrop writes DROP DOMAIN for a domain and DROP TYPE otherwise, with no
// guard and no FORCE.
func renderDrop(_ renderer.ExtensionContext, value *Drop) ([]string, error) {
	keyword := "TYPE"
	if value.Target.Kind == DomainKind {
		keyword = "DOMAIN"
	}
	return []string{"DROP " + keyword + " " + statementName(value.Target.Schema, value.Target.Name) + ";"}, nil
}

// statement names a creation's statement and its type in a refusal.
func (v *CreateDomain) statement() string { return "CREATE DOMAIN " + v.Target.QualifiedName() }

// statement names a creation's statement and its type in a refusal.
func (v *ReplaceObjectType) statement() string { return "CREATE TYPE " + v.Target.QualifiedName() }

// statement names a drop's statement and its type in a refusal.
func (v *Drop) statement() string {
	if v.Target.Kind == DomainKind {
		return "DROP DOMAIN " + v.Target.QualifiedName()
	}
	return "DROP TYPE " + v.Target.QualifiedName()
}

// createGuard is the IF NOT EXISTS a creation carries where the target has
// existence guards.
func createGuard(ctx renderer.ExtensionContext) string {
	if ctx.Capabilities.Has(capability.ObjectExistenceGuards) {
		return " IF NOT EXISTS"
	}
	return ""
}

// stringLiteral quotes a value as an Oracle string literal.
func stringLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
