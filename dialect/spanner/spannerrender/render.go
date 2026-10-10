// Package spannerrender validates and renders Spanner-owned row deletion
// policies: the TTL clause a CREATE TABLE carries and the ALTER TABLE statement
// an owned change lowers to. Callers select these handlers explicitly; no
// global registry is installed.
package spannerrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/internal/spannerttl"
	"ptah.run/internal/sqlident"
)

// Handlers returns independent descriptors for the supported operation roles.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&spannerast.AlterRowDeletion{}, ast.AlterExtension, validateRowDeletion, renderRowDeletion),
	}
}

// Registry creates a local handler registry. Unknown kinds and roles fail
// closed; an ALTER payload requires its parent before SQL can be rendered.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validateRowDeletion(ctx renderer.ExtensionContext, op *spannerast.AlterRowDeletion) error {
	if err := requireRowDeletion(ctx.Target, ctx.Capabilities, tableName(ctx.Parent)); err != nil {
		return err
	}
	return spannerast.ValidateChange(&op.Change)
}

// renderRowDeletion lowers a change to its one statement. Measured against the
// Cloud Spanner emulator behind PGAdapter, reading
// information_schema.tables.row_deletion_policy_expression back after each
// (stokaro/ptah#2236):
//
//	no policy -> a policy   ALTER TABLE t ADD TTL INTERVAL '10 days' ON ts
//	a policy  -> a policy   ALTER TABLE t ALTER TTL INTERVAL '20 days' ON ts
//	a policy  -> no policy  ALTER TABLE t DROP TTL
//
// The removal names no column: the clause goes and the column stays.
func renderRowDeletion(ctx renderer.ExtensionContext, op *spannerast.AlterRowDeletion) ([]string, error) {
	table := sqlident.QuotePostgresQualified(ctx.Parent.Name)
	change := op.Change
	if change.After == nil {
		return []string{fmt.Sprintf("ALTER TABLE %s DROP TTL;", table)}, nil
	}
	verb := "ADD"
	if change.Before != nil {
		verb = "ALTER"
	}
	return []string{fmt.Sprintf("ALTER TABLE %s %s %s;", table, verb, clause(change.After.Policy))}, nil
}

// clause is the TTL clause with the interval as the author wrote it. Rendering
// a normalized value instead would write Ptah's spelling into the operator's
// DDL, which the comparison exists to avoid having to do.
func clause(policy spannerschema.Policy) string {
	return spannerttl.Clause(policy.Column, policy.Interval, func(column string) string {
		return sqlident.Quote(platform.Spanner, sqlident.UnquoteDoubleQuoted(column))
	})
}

func tableName(parent *ast.AlterTableNode) string {
	if parent == nil {
		return ""
	}
	return parent.Name
}

// ValidateTableFacets checks the table values a Spanner CREATE TABLE consumes.
// An empty collection, including retained source exclusions, is valid. Unknown
// active kinds wrap ptaherr.ErrUnsupportedFeature; observations and malformed
// declarations wrap schemaext.ErrInvalidValue. Target selection happens before
// this call.
func ValidateTableFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != spannerschema.RowDeletionKind {
			return fmt.Errorf("%w: Spanner table facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, found, err := schemaext.FacetAs[*spannerschema.DesiredRowDeletion](facets, spannerschema.RowDeletionKind)
	if err != nil || !found {
		return err
	}
	return spannerschema.ValidateDesired(value)
}

// CreateTableClause returns the ` TTL INTERVAL '…' ON …` clause a CREATE TABLE
// carries for its row deletion policy, and the empty string for a table
// without one.
//
// A target without capability.RowDeletionPolicy is refused rather than given a
// statement without the clause. The clause deletes rows; omitting it would
// create a table the server accepts while the declared retention does not
// exist.
func CreateTableClause(target string, caps capability.Capabilities, table string, facets schemaext.Facets) (string, error) {
	if err := ValidateTableFacets(facets); err != nil {
		return "", err
	}
	value, found, err := schemaext.FacetAs[*spannerschema.DesiredRowDeletion](facets, spannerschema.RowDeletionKind)
	if err != nil || !found {
		return "", err
	}
	if err := requireRowDeletion(target, caps, table); err != nil {
		return "", err
	}
	return " " + clause(value.Policy), nil
}

// requireRowDeletion names the dialect and the reason rather than only saying
// no. The clause is Spanner's: on another PostgreSQL-wire engine the row
// expiry spelling is a different owner's.
func requireRowDeletion(target string, caps capability.Capabilities, table string) error {
	if platform.NormalizeDialect(target) != platform.Spanner {
		return &ptaherr.CapabilityError{Dialect: target, Feature: string(spannerschema.RowDeletionKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("a Spanner row deletion policy cannot be rendered for %q", target)}
	}
	if caps.Has(capability.RowDeletionPolicy) {
		return nil
	}
	subject := "a table"
	if table != "" {
		subject = fmt.Sprintf("table %q", table)
	}
	return &ptaherr.CapabilityError{Dialect: platform.Spanner, Feature: string(capability.RowDeletionPolicy), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s declares a row deletion policy, which requires target capability %s; Ptah refuses the "+
			"declaration rather than emitting a statement that silently keeps every row",
			platform.Spanner, subject, capability.RowDeletionPolicy)}
}
