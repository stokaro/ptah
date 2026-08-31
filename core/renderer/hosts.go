package renderer

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
)

// hostedDeclaration is one table-owned declaration and the two spellings it can
// name its host with: StructName, the Go struct the declaration was read from,
// and the database name of the relation.
//
// The two are not redundant and neither is required on its own -- a Go
// annotation names the struct, a YAML or SQL source names the table, and a
// declaration that names both is ordinary. What is not ordinary is naming
// neither, and that is what this type is collected for.
type hostedDeclaration struct {
	kind       string
	name       string
	structName string
	table      string
}

// validateDeclaredHosts refuses a table-owned declaration that names no host.
//
// Six families reach a target through a host they name, and measured on
// PostgreSQL at a80cb5652 every one of them answered a declaration naming
// neither spelling without a word (stokaro/ptah#2612):
//
//	constraint     dropped from the render entirely
//	index          CREATE INDEX IF NOT EXISTS "idx" ON "" ("id")
//	RLS enable     ALTER TABLE "" ENABLE ROW LEVEL SECURITY
//	RLS policy     CREATE POLICY "p" ON "" FOR SELECT
//	trigger        CREATE TRIGGER "tr" BEFORE UPDATE ON ""
//	hypertable     SELECT create_hypertable('', by_range('id'))
//
// All six at exit 0. An empty identifier is not a relation name, no server
// accepts one, and a constraint that vanishes is the loss this refusal exists
// to report -- the same shape stokaro/ptah#2608 fixed one level down, for a
// column.
//
// The check is on the DECLARATION rather than on a resolver's answer, and
// deliberately so. The index and row-level-security resolvers disagree on
// purpose about a host that matches nothing declared: one answers "" and the
// other keeps the author's spelling so the server can report the name they
// wrote (stokaro/ptah#1311). Refusing on either answer would have made this a
// third opinion about resolution. Naming no host at all is the one question
// both resolvers were never asked, and it has one answer for every family.
//
// An extended property is deliberately absent: SQL Server takes one at database
// scope, and `EXEC sp_addextendedproperty @name = N'MS_Description'` naming no
// table is a complete statement rather than a loss.
func validateDeclaredHosts(dialect string, database *schemamodel.Database) error {
	for _, declaration := range hostedDeclarations(database) {
		if hostNamed(declaration) {
			continue
		}
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf(
				"%s names no table: a table-owned declaration has to name its host, "+
					"through the Go struct it belongs to or through the table name itself",
				declarationSubject(declaration),
			),
		}
	}
	return nil
}

// hostNamed reports whether the declaration names a host at all.
func hostNamed(declaration hostedDeclaration) bool {
	return strings.TrimSpace(declaration.structName) != "" ||
		strings.TrimSpace(declaration.table) != ""
}

// declarationSubject names the object in a refusal, falling back to its kind
// where the declaration has no name of its own either.
func declarationSubject(declaration hostedDeclaration) string {
	if strings.TrimSpace(declaration.name) == "" {
		return "a declared " + declaration.kind
	}
	return fmt.Sprintf("%s %q", declaration.kind, declaration.name)
}

// hostedDeclarations collects every declaration that reaches a target through a
// host, in the order a reader of the schema meets them.
func hostedDeclarations(database *schemamodel.Database) []hostedDeclaration {
	declarations := make([]hostedDeclaration, 0,
		len(database.Constraints)+len(database.Indexes)+len(database.RLSEnabledTables)+
			len(database.RLSPolicies)+len(database.Triggers)+len(database.Hypertables))

	for _, constraint := range database.Constraints {
		declarations = append(declarations, hostedDeclaration{
			kind: "constraint", name: constraint.Name,
			structName: constraint.StructName, table: constraint.Table,
		})
	}
	for _, index := range database.Indexes {
		declarations = append(declarations, hostedDeclaration{
			kind: "index", name: index.Name,
			structName: index.StructName, table: index.TableName,
		})
	}
	for _, enabled := range database.RLSEnabledTables {
		declarations = append(declarations, hostedDeclaration{
			kind: "row-level security enablement", name: enabled.Table,
			structName: enabled.StructName, table: enabled.Table,
		})
	}
	for _, policy := range database.RLSPolicies {
		declarations = append(declarations, hostedDeclaration{
			kind: "policy", name: policy.Name,
			structName: policy.StructName, table: policy.Table,
		})
	}
	for _, trigger := range database.Triggers {
		declarations = append(declarations, hostedDeclaration{
			kind: "trigger", name: trigger.Name,
			structName: trigger.StructName, table: trigger.Table,
		})
	}
	for _, hypertable := range database.Hypertables {
		declarations = append(declarations, hostedDeclaration{
			kind: "hypertable", name: hypertable.Table,
			structName: hypertable.StructName, table: hypertable.Table,
		})
	}
	return slices.Clip(declarations)
}
