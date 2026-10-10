// Package crdbrender validates and renders CockroachDB-owned row-level TTL:
// the storage parameters a CREATE TABLE carries and the ALTER TABLE statements
// an owned change lowers to. Callers select these handlers explicitly; no
// global registry is installed.
package crdbrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/internal/ttlsql"
	"ptah.run/internal/sqlident"
)

// Handlers returns independent descriptors for the supported operation roles.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&crdbast.AlterRowTTL{}, ast.AlterExtension, validateRowTTL, renderRowTTL),
	}
}

// Registry creates a local handler registry. Unknown kinds and roles fail
// closed; an ALTER payload requires its parent before SQL can be rendered.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validateRowTTL(ctx renderer.ExtensionContext, op *crdbast.AlterRowTTL) error {
	if err := requireRowTTL(ctx.Target, ctx.Capabilities, tableName(ctx.Parent)); err != nil {
		return err
	}
	return ttlsql.ValidateChange(&op.Change)
}

func renderRowTTL(ctx renderer.ExtensionContext, op *crdbast.AlterRowTTL) ([]string, error) {
	return ttlsql.Statements(sqlident.QuotePostgresQualified(ctx.Parent.Name), &op.Change), nil
}

func tableName(parent *ast.AlterTableNode) string {
	if parent == nil {
		return ""
	}
	return parent.Name
}

// ValidateTableFacets checks the table values a CockroachDB CREATE TABLE
// consumes. An empty collection, including retained source exclusions, is
// valid. Unknown active kinds wrap ptaherr.ErrUnsupportedFeature; observations
// and malformed declarations wrap schemaext.ErrInvalidValue. Target selection
// happens before this call.
func ValidateTableFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != crdbschema.RowTTLKind {
			return fmt.Errorf("%w: CockroachDB table facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, found, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](facets, crdbschema.RowTTLKind)
	if err != nil || !found {
		return err
	}
	return crdbschema.ValidateDesired(value)
}

// CreateTableClause returns the ` WITH (...)` clause a CREATE TABLE carries for
// its row-level TTL, and the empty string for a table without one.
//
// A target without capability.RowLevelTTL is refused rather than given a
// statement without the clause. Row-level TTL deletes rows; omitting it would
// create a table the server accepts while the declared retention policy does
// not exist (stokaro/ptah#1027).
func CreateTableClause(target string, caps capability.Capabilities, table string, facets schemaext.Facets) (string, error) {
	if err := ValidateTableFacets(facets); err != nil {
		return "", err
	}
	value, found, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](facets, crdbschema.RowTTLKind)
	if err != nil || !found {
		return "", err
	}
	if err := requireRowTTL(target, caps, table); err != nil {
		return "", err
	}
	return " WITH (" + strings.Join(ttlsql.Options(value.Policy), ", ") + ")", nil
}

// requireRowTTL names the dialect and the reason rather than only saying no:
// on a PostgreSQL-wire engine that is not CockroachDB there is no other
// spelling. PostgreSQL refuses the parameter itself, but YugabyteDB first warns
// that it is ignoring it, which is why the refusal is Ptah's.
func requireRowTTL(target string, caps capability.Capabilities, table string) error {
	if platform.NormalizeDialect(target) != platform.CockroachDB {
		return &ptaherr.CapabilityError{Dialect: target, Feature: string(crdbschema.RowTTLKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("CockroachDB row-level TTL cannot be rendered for %q", target)}
	}
	if caps.Has(capability.RowLevelTTL) {
		return nil
	}
	subject := "a table"
	if table != "" {
		subject = fmt.Sprintf("table %q", table)
	}
	return &ptaherr.CapabilityError{Dialect: platform.CockroachDB, Feature: string(capability.RowLevelTTL), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s declares row-level TTL, which requires target capability %s; Ptah refuses the "+
			"declaration rather than emitting a statement whose retention policy the server may drop",
			platform.CockroachDB, subject, capability.RowLevelTTL)}
}
