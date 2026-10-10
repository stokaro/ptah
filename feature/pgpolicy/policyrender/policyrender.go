// Package policyrender validates and renders the PostgreSQL row-security
// operation payloads of package pgpolicy for the PostgreSQL family, and lowers
// a new table's row-security switches into the statements that follow its
// CREATE TABLE. It reads captured target facts only and never opens a database
// connection.
//
// Capability gating stays with the PostgreSQL-family renderer: each payload
// names the capability it needs and the object a skip line names, and a target
// without the key writes the skip and records the omission, as it does for its
// own capability-gated objects.
package policyrender

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/sqlident"
)

// Handlers returns independent statement handlers for both payloads, for a
// composition that joins them with other owners' handlers.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&pgpolicy.PolicyOperation{}, ast.StatementExtension, validatePolicy, renderPolicy),
		renderer.TypedHandler(&pgpolicy.PolicyCommentOperation{}, ast.StatementExtension, validateComment, renderComment),
		renderer.TypedHandler(&pgpolicy.TableStateOperation{}, ast.StatementExtension, validateTableState, renderTableState),
	}
}

// Registry returns the statement handlers for both payloads. Each call returns
// an independent registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

// ValidateTableFacets checks the row-security switches among a table's
// facets. Other kinds belong to their own owners; the composition that selected
// this one decides whether they are accepted.
func ValidateTableFacets(facets schemaext.Facets) error {
	declared, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](facets, pgpolicy.TableStateKind)
	if err != nil || !found {
		return err
	}
	return pgpolicy.ValidateDesiredTableState(declared)
}

// LowerTableFacets returns the statements that give a new table its declared
// row-security switches, to run after its CREATE TABLE, and the facets the
// statement itself still carries. table is the name the CREATE TABLE writes. A
// table created by the plan had no rows anyone could read before it, so the
// statements leave access unchanged.
func LowerTableFacets(table string, facets schemaext.Facets) ([]ast.ExtensionPayload, schemaext.Facets, error) {
	declared, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](facets, pgpolicy.TableStateKind)
	if err != nil {
		return nil, schemaext.Facets{}, err
	}
	if !found {
		return nil, facets, nil
	}
	if err := pgpolicy.ValidateDesiredTableState(declared); err != nil {
		return nil, schemaext.Facets{}, err
	}
	rest := facets.Without(pgpolicy.TableStateKind)
	if !declared.Enabled && !declared.Forced {
		return nil, rest, nil
	}
	operation := &pgpolicy.TableStateOperation{Table: table, Change: pgpolicy.TableStateChange{
		Before: &pgpolicy.ObservedTableState{}, After: new(*declared),
		Access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "the table is created by this plan, so no role could read it before"},
	}}
	return []ast.ExtensionPayload{operation}, rest, nil
}

func validatePolicy(ctx renderer.ExtensionContext, value *pgpolicy.PolicyOperation) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgpolicy.PolicyOperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

func validateTableState(ctx renderer.ExtensionContext, value *pgpolicy.TableStateOperation) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgpolicy.TableStateOperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

func validateComment(ctx renderer.ExtensionContext, value *pgpolicy.PolicyCommentOperation) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, pgpolicy.PolicyCommentOperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

// renderPolicy writes a policy transition: CREATE POLICY, DROP POLICY, or for
// a change the drop and the create. ALTER POLICY changes neither the command
// nor the composition and cannot remove a clause, so a change is a
// replacement; the plan runs both statements in its transaction, so no other
// session sees the table without the policy.
func renderPolicy(_ renderer.ExtensionContext, value *pgpolicy.PolicyOperation) ([]string, error) {
	name := quotePart(value.Name)
	table := qualifiedIdentifier(value.Schema, value.Table)
	var statements []string
	if value.Change.Before != nil {
		statements = append(statements, "DROP POLICY "+name+" ON "+table+";")
	}
	if value.Change.After != nil {
		statements = append(statements, createStatement(name, table, value.Change.After))
	}
	return statements, nil
}

// createStatement writes CREATE POLICY as PostgreSQL documents it. What the
// declaration left out is left out of the statement, so the server applies its
// own default: PERMISSIVE, FOR ALL and TO PUBLIC.
func createStatement(name, table string, policy *pgpolicy.DesiredPolicy) string {
	head := []string{"CREATE POLICY", name, "ON", table}
	if policy.Composition == pgpolicy.Restrictive {
		head = append(head, "AS RESTRICTIVE")
	}
	if policy.Command != "" {
		head = append(head, "FOR", string(policy.Command))
	}
	if policy.Roles != nil {
		head = append(head, "TO", roleList(policy.Roles))
	}
	lines := []string{strings.Join(head, " ")}
	if policy.Using != nil {
		lines = append(lines, "    USING ("+*policy.Using+")")
	}
	if policy.WithCheck != nil {
		lines = append(lines, "    WITH CHECK ("+*policy.WithCheck+")")
	}
	return strings.Join(lines, "\n") + "\n;"
}

// renderComment writes COMMENT ON POLICY, with NULL for an empty comment.
func renderComment(_ renderer.ExtensionContext, value *pgpolicy.PolicyCommentOperation) ([]string, error) {
	literal := "NULL"
	if value.Comment != "" {
		literal = sqlident.StringLiteral(value.Comment)
	}
	return []string{"COMMENT ON POLICY " + quotePart(value.Name) + " ON " + qualifiedIdentifier(value.Schema, value.Table) + " IS " + literal + ";"}, nil
}

// roleList writes the TO list in canonical order: a keyword bare and a role
// name quoted, its bytes as they are.
func roleList(roles []pgpolicy.RoleSelector) string {
	parts := make([]string, len(roles))
	for i, role := range pgpolicy.CanonicalRoles(roles) {
		if role.Keyword != "" {
			parts[i] = string(role.Keyword)
			continue
		}
		parts[i] = sqlident.Quote(platform.Postgres, role.Name)
	}
	return strings.Join(parts, ", ")
}

// renderTableState writes the ALTER TABLE statements that set each switch the
// change moves. ENABLE comes before FORCE, so a table both enabled and forced
// is never forced while row security is off.
func renderTableState(_ renderer.ExtensionContext, value *pgpolicy.TableStateOperation) ([]string, error) {
	table := qualifiedIdentifier(value.Schema, value.Table)
	before, after := value.Change.Before, value.Change.After
	var statements []string
	if before.Enabled != after.Enabled {
		keyword := "DISABLE"
		if after.Enabled {
			keyword = "ENABLE"
		}
		statements = append(statements, "ALTER TABLE "+table+" "+keyword+" ROW LEVEL SECURITY;")
	}
	if before.Forced != after.Forced {
		keyword := "NO FORCE"
		if after.Forced {
			keyword = "FORCE"
		}
		statements = append(statements, "ALTER TABLE "+table+" "+keyword+" ROW LEVEL SECURITY;")
	}
	if after.Comment != "" {
		statements = append([]string{"-- " + strings.Join(strings.Fields(after.Comment), " ")}, statements...)
	}
	return statements, nil
}

// qualifiedIdentifier quotes each part separately, so a dot inside a name
// stays part of that name. A part that arrives double-quoted is not quoted
// twice.
func qualifiedIdentifier(schema, name string) string {
	if strings.TrimSpace(schema) == "" {
		return quotePart(name)
	}
	return quotePart(schema) + "." + quotePart(name)
}

func quotePart(part string) string {
	return sqlident.Quote(platform.Postgres, sqlident.UnquoteDoubleQuoted(part))
}
