// Package tsrender validates and renders TimescaleDB operation payloads for
// the PostgreSQL family, and lowers the table facets a CREATE TABLE cannot
// carry into those payloads. It reads captured target facts only and never
// opens a database connection.
//
// Capability gating stays with the PostgreSQL-family renderer: each payload
// names the capability it needs and the object a skip line names, and a target
// without the key writes the skip and records the omission, as it does for its
// own capability-gated objects. Validation here checks the target family and
// the payload's shape.
package tsrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/sqlident"
)

// Handlers returns independent statement handlers for every TimescaleDB
// payload, for a composition that joins them with other owners' handlers.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&tsast.CreateHypertable{}, ast.StatementExtension, validateHypertable, renderHypertable),
		renderer.TypedHandler(&tsast.ContinuousAggregate{}, ast.StatementExtension, validateAggregate, renderAggregate),
	}
}

// Registry returns the statement handlers for every TimescaleDB payload. Each
// call returns an independent registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

// ValidateTableFacets checks the TimescaleDB settings among a table's facets.
// Other kinds belong to their own owners; the composition that selected this
// one decides whether they are accepted.
func ValidateTableFacets(facets schemaext.Facets) error {
	declared, found, err := schemaext.FacetAs[*tsschema.DesiredHypertable](facets, tsschema.HypertableKind)
	if err != nil || !found {
		return err
	}
	return tsschema.ValidateDesiredHypertable(declared)
}

// LowerTableFacets returns the statements that give a new table its
// TimescaleDB settings, in the order they run after its CREATE TABLE, and the
// facets the statement itself still has to carry. table is the name the
// CREATE TABLE writes. The call needs the table to exist and must run before
// any row is written: measured on 2.29.2, it answers
// `relation "conditions" does not exist` before the table and
// `table "loaded" is not empty` once it holds a row.
func LowerTableFacets(table string, facets schemaext.Facets) ([]ast.ExtensionPayload, schemaext.Facets, error) {
	declared, found, err := schemaext.FacetAs[*tsschema.DesiredHypertable](facets, tsschema.HypertableKind)
	if err != nil {
		return nil, schemaext.Facets{}, err
	}
	if !found {
		return nil, facets, nil
	}
	if err := tsschema.ValidateDesiredHypertable(declared); err != nil {
		return nil, schemaext.Facets{}, err
	}
	return []ast.ExtensionPayload{&tsast.CreateHypertable{Table: table, Hypertable: *declared}}, facets.Without(tsschema.HypertableKind), nil
}

func validateTarget(ctx renderer.ExtensionContext) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, tsast.CreateHypertableKind, ast.StatementExtension)
	}
	return nil
}

func validateHypertable(ctx renderer.ExtensionContext, value *tsast.CreateHypertable) error {
	if err := validateTarget(ctx); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

func validateAggregate(ctx renderer.ExtensionContext, value *tsast.ContinuousAggregate) error {
	if !platform.IsPostgresFamily(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, tsast.ContinuousAggregateKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

// renderHypertable writes the call that turns an ordinary table into a
// hypertable.
//
// It is a function call rather than DDL, because TimescaleDB has no CREATE
// HYPERTABLE grammar. Measured on 2.29.2 / PostgreSQL 17:
//
//	SELECT create_hypertable('conditions', by_range('time'));                  -> (1,t)
//	SELECT create_hypertable('conditions', by_range('time'));                  -> ERROR: already a hypertable
//	SELECT create_hypertable('conditions', by_range('time'), if_not_exists => TRUE); -> (1,f), NOTICE
//
// The table is a REGCLASS literal, which the server parses as a name: an
// unquoted spelling folds to lower case. The CREATE TABLE before it quotes
// the name, so the literal carries the same quoted spelling -- `'"Readings"'`
// -- or a mixed-case table would answer `relation "readings" does not exist`.
// The column inside `by_range` is a name the server compares as written, so
// it is passed as it is.
//
// The call creates an index on the dimension unless told not to, and nothing
// declared that index. Measured on 2.29.2: after
// `create_hypertable('readings', by_range('time'))` the table carries
// `readings_time_idx`, and the next comparison planned
// `DROP INDEX IF EXISTS "readings_time_idx"`. `create_default_indexes => FALSE`
// keeps the description the whole truth about which indexes exist.
func renderHypertable(_ renderer.ExtensionContext, value *tsast.CreateHypertable) ([]string, error) {
	declared := value.Hypertable
	dimension := "by_range(" + sqlident.StringLiteral(declared.Column)
	if strings.TrimSpace(declared.ChunkInterval) != "" {
		dimension += ", INTERVAL " + sqlident.StringLiteral(declared.ChunkInterval)
	}
	dimension += ")"
	arguments := []string{sqlident.StringLiteral(sqlident.QuotePostgresQualified(value.Table)), dimension}
	if declared.IfNotExists {
		arguments = append(arguments, "if_not_exists => TRUE")
	}
	arguments = append(arguments, "create_default_indexes => FALSE")
	statement := fmt.Sprintf("SELECT create_hypertable(%s);", strings.Join(arguments, ", "))
	return withComment(declared.Comment, statement), nil
}

// renderAggregate writes the statements for one aggregate transition.
//
// A creation is a CREATE MATERIALIZED VIEW carrying `WITH
// (timescaledb.continuous)`, which is what makes the extension own it. `WITH NO
// DATA` is not optional: creating one WITH DATA materializes the whole history
// the hypertable holds, which is work an operator schedules rather than a side
// effect of a schema change.
//
// A removal is DROP MATERIALIZED VIEW on the server's own instruction:
// `cannot drop continuous aggregate using DROP VIEW. HINT: Use DROP
// MATERIALIZED VIEW to drop a continuous aggregate.` A replacement is the drop
// followed by the creation, because `CREATE OR REPLACE MATERIALIZED VIEW` is
// `syntax error at or near "MATERIALIZED"` (all measured on 2.29.2).
func renderAggregate(_ renderer.ExtensionContext, value *tsast.ContinuousAggregate) ([]string, error) {
	name := qualifiedIdentifier(value.Schema, value.Name)
	var statements []string
	if value.Change.Before != nil {
		statements = append(statements, "DROP MATERIALIZED VIEW IF EXISTS "+name+";")
	}
	if value.Change.After == nil {
		return statements, nil
	}
	declared := value.Change.After
	options := []string{"timescaledb.continuous"}
	if declared.MaterializedOnly != nil {
		options = append(options, fmt.Sprintf("timescaledb.materialized_only = %t", *declared.MaterializedOnly))
	}
	create := fmt.Sprintf("CREATE MATERIALIZED VIEW %s WITH (%s) AS\n%s\nWITH NO DATA\n;",
		name, strings.Join(options, ", "), tsschema.FoldBody(declared.Body))
	return append(statements, withComment(declared.Comment, create)...), nil
}

func withComment(comment, statement string) []string {
	if comment == "" {
		return []string{statement}
	}
	return []string{"-- " + strings.Join(strings.Fields(comment), " "), statement}
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
