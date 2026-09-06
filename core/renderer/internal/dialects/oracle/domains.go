package oracle

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/internal/defaultlit"
)

// visitCreateDomain renders Oracle 23's CREATE DOMAIN.
//
// The clause order is PostgreSQL's, and that is a measurement rather than a
// hope. On 23.26.2.0.0,
//
//	CREATE DOMAIN pgshape_d AS VARCHAR2(50) NOT NULL DEFAULT 'x' CHECK (VALUE <> 'zzz')
//
// is accepted verbatim, so the shape ast.DomainTypeDef already carries needs no
// rearranging for this target. What differs is the identifier quoting and the
// guard, both of which this renderer already owns.
//
// The base type is written as the declaration spelled it. A domain over a type
// Oracle does not have fails at the server with ORA-00902, which is the same
// answer a column of that type gets, and translating it here would be this
// renderer inventing a type map the column path does not use.
func (r *Renderer) visitCreateDomain(node *ast.CreateTypeNode, typeDef *ast.DomainTypeDef) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	statement := fmt.Sprintf("CREATE DOMAIN%s %s AS %s",
		r.createGuard(), escapeQualifiedIdentifier(node.Name), typeDef.BaseType)
	if !typeDef.Nullable {
		statement += " NOT NULL"
	}
	if typeDef.Default != nil {
		switch {
		case typeDef.Default.HasLiteral():
			statement += " DEFAULT " + defaultlit.Render(typeDef.Default.Value, escapeStringLiteral)
		case typeDef.Default.Expression != "":
			statement += " DEFAULT " + typeDef.Default.Expression
		}
	}
	if strings.TrimSpace(typeDef.Check) != "" {
		statement += fmt.Sprintf(" CHECK (%s)", typeDef.Check)
	}
	r.w.WriteLinef("%s;", statement)
	return nil
}

// visitDropDomain renders DROP DOMAIN.
//
// Measured on 23.26.2.0.0: dropping a domain a table still uses answers
// ORA-11502, `The domain EMAIL_D to be dropped has dependent objects`. That is
// left as the server's refusal rather than turned into FORCE: dropping a
// domain out from under the columns typed by it is not what a plan that no
// longer declares the domain asked for, and the columns are the planner's own
// business.
func (r *Renderer) visitDropDomain(node *ast.DropTypeNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	guard := ""
	if node.IfExists {
		guard = r.dropGuard()
	}
	r.w.WriteLinef("DROP DOMAIN%s %s;", guard, escapeQualifiedIdentifier(node.Name))
	return nil
}

// domainsRendered reports whether this target's preset says Ptah manages
// domains on it.
//
// The key is version-dependent here in a way it is not elsewhere: 23 has
// CREATE DOMAIN and 21 answers ORA-00901, so Oracle21 turns it off and the
// same renderer refuses on that line. That is why the check is the capability
// rather than the dialect.
func (r *Renderer) domainsRendered() bool {
	return r.capabilities().Has(capability.DomainTypes)
}
