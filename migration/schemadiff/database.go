package schemadiff

import (
	"context"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// CompareWithDatabase resolves live catalog identifier equivalence and compares
// the target schema with the connected database schema.
func CompareWithDatabase(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, error) {
	diff, _, err := CompareWithDatabaseReportingUndecidedAdditions(
		ctx, conn, generated, database, opts,
	)
	return diff, err
}

// CompareWithDatabaseReportingUndecidedAdditions performs the same
// database-aware comparison as [CompareWithDatabase] and also reports desired
// additions that the current state's coverage record makes undecidable.
//
// The comparison resolves live catalog identifier semantics before comparing
// and applies [config.DefaultCompareOptions] when opts is nil, just as
// [CompareWithDatabase] does. See [CompareReportingUndecidedAdditions] for the
// meaning and ordering of the second return.
func CompareWithDatabaseReportingUndecidedAdditions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object, error) {
	if conn == nil {
		return nil, nil, fmt.Errorf("compare schemas: database connection is nil")
	}
	info := conn.Info()
	// Resolve the comparison-owned toggle before catalog queries. Direct
	// library callers must not run identifier-semantics SQL before reporting a
	// malformed setting; command adapters perform the same check before they
	// load desired sources or connect.
	if err := sqlitevirtual.ValidateToggle(info.Dialect); err != nil {
		return nil, nil, err
	}
	names := collectIdentifierNames(generated, database, info.Schema)
	semantics, err := conn.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return nil, nil, fmt.Errorf("compare schemas: %w", err)
	}
	info.IdentifierSemantics = semantics

	// Resolved here rather than inside the comparison for the same reason the
	// identifier semantics are: a live fact belongs to the side that holds a
	// connection, and the comparison itself must stay a pure function of the
	// two states it is given.
	expressions, err := resolveDomainExpressions(ctx, conn, generated, database)
	if err != nil {
		return nil, nil, err
	}
	bodies, err := resolveContinuousAggregateBodies(ctx, conn, generated, database)
	if err != nil {
		return nil, nil, err
	}
	checks, err := resolveCheckExpressions(ctx, conn, generated, database)
	if err != nil {
		return nil, nil, err
	}
	if len(expressions) > 0 || len(bodies) > 0 || len(checks) > 0 {
		merged := config.DefaultCompareOptions()
		if opts != nil {
			*merged = *opts
		}
		if len(expressions) > 0 {
			merged.DomainExpressions = expressions
		}
		if len(bodies) > 0 {
			merged.ContinuousAggregateBodies = bodies
		}
		if len(checks) > 0 {
			merged.CheckExpressions = checks
		}
		opts = merged
	}

	return compareWithDatabaseInfoReportingUndecidedAdditions(
		generated, database, info, opts,
	)
}

// resolveDomainExpressions normalizes the declared CHECK and DEFAULT of every
// domain the database also holds.
//
// Only those: a domain being created carries its declaration into the CREATE
// statement unchanged, so nothing about it needs the server's spelling, and a
// domain being dropped has no declaration left to normalize. The ones in the
// middle are the ones a string comparison cannot decide (stokaro/ptah#1717).
func resolveDomainExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
) (map[string]config.DomainExpression, error) {
	if generated == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Domains))
	for _, domain := range database.Domains {
		held[strings.ToLower(domain.QualifiedName())] = struct{}{}
	}

	probes := make([]dbschema.DomainExpressionProbe, 0, len(generated.Domains))
	for _, domain := range generated.Domains {
		if _, exists := held[strings.ToLower(domain.QualifiedName())]; !exists {
			continue
		}
		defaultExpression := domain.DefaultExpr
		if defaultExpression == "" && domain.Default != "" {
			defaultExpression = quoteDomainDefaultLiteral(domain.Default)
		}
		probes = append(probes, dbschema.DomainExpressionProbe{
			Key:      domain.QualifiedName(),
			BaseType: domain.BaseType,
			Check:    domain.Check,
			Default:  defaultExpression,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	expressions, err := conn.ResolveDomainExpressions(ctx, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return expressions, nil
}

// resolveCheckExpressions normalizes the declared expression of every table
// CHECK the database also holds.
//
// Only those, for the reason [resolveDomainExpressions] gives: a constraint
// being created carries its declaration into the ADD statement unchanged, and
// one being dropped has no declaration left to normalize. The ones in the
// middle are the ones a string comparison cannot decide.
//
// The probe table is built from the LIVE table's columns rather than the
// declared ones, because the rewrite depends on the types the expression is
// parsed against: `price >= 0` normalizes to `(0)::numeric` over numeric and
// to `(0)` over integer, and it is the live table the constraint is compared
// against.
func resolveCheckExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
) (map[string]config.CheckExpression, error) {
	if generated == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "CHECK") {
			held[checkExpressionKey(constraint.TableName, constraint.Name)] = struct{}{}
		}
	}
	columns := liveTableColumns(database)

	probes := make([]dbschema.CheckExpressionProbe, 0, len(generated.Constraints))
	for _, constraint := range generated.Constraints {
		if !strings.EqualFold(constraint.Type, "CHECK") {
			continue
		}
		key := checkExpressionKey(constraint.Table, constraint.Name)
		if _, exists := held[key]; !exists {
			continue
		}
		probeColumns, known := columns[foldCheckTableName(constraint.Table)]
		if !known {
			continue
		}
		probes = append(probes, dbschema.CheckExpressionProbe{
			Key:        key,
			Columns:    probeColumns,
			Expression: constraint.CheckExpression,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	checks, err := conn.ResolveCheckExpressions(ctx, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return checks, nil
}

// liveTableColumns projects every live table's columns into the shape a probe
// needs, keyed by the table's bare name.
func liveTableColumns(database *dbschematypes.DBSchema) map[string][]dbschema.CheckProbeColumn {
	columns := make(map[string][]dbschema.CheckProbeColumn, len(database.Tables))
	for _, table := range database.Tables {
		probeColumns := make([]dbschema.CheckProbeColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			probeColumns = append(probeColumns, dbschema.CheckProbeColumn{
				Name: column.Name,
				Type: column.RawType(),
			})
		}
		columns[foldCheckTableName(table.Name)] = probeColumns
	}
	return columns
}

// checkExpressionKey is the map key both halves use: the table and the
// constraint, folded, because a constraint name is unique within its table and
// not across the schema.
func checkExpressionKey(table, name string) string {
	return foldCheckTableName(table) + "." + strings.ToLower(strings.TrimSpace(name))
}

// foldCheckTableName reduces a table reference to its bare name, folded. The
// declaration may qualify it and the read may not, and the probe only needs to
// find the columns.
func foldCheckTableName(table string) string {
	name := strings.TrimSpace(table)
	if ref, ok := tableref.Parse(name); ok {
		name = ref.Name
	}
	return strings.ToLower(name)
}

// resolveContinuousAggregateBodies normalizes the declared SELECT of every
// continuous aggregate the database also holds.
//
// Only those, for the reason [resolveDomainExpressions] gives: an aggregate
// being created carries its declaration into the CREATE statement unchanged,
// and one being dropped has no declaration left to normalize. The ones in the
// middle are the ones a string comparison cannot decide.
func resolveContinuousAggregateBodies(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
) (map[string]config.ContinuousAggregateBody, error) {
	if generated == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.ContinuousAggregates))
	for _, aggregate := range database.ContinuousAggregates {
		held[strings.ToLower(aggregate.QualifiedName())] = struct{}{}
		held[strings.ToLower(aggregate.Name)] = struct{}{}
	}

	probes := make([]dbschema.ContinuousAggregateProbe, 0, len(generated.ContinuousAggregates))
	for _, aggregate := range generated.ContinuousAggregates {
		if _, exists := held[strings.ToLower(aggregate.QualifiedName())]; !exists {
			continue
		}
		probes = append(probes, dbschema.ContinuousAggregateProbe{
			Key:              aggregate.QualifiedName(),
			Schema:           aggregate.Schema,
			Body:             aggregate.Body,
			MaterializedOnly: aggregate.MaterializedOnly,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	bodies, err := conn.ResolveContinuousAggregateBodies(ctx, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return bodies, nil
}

// quoteDomainDefaultLiteral renders a declared literal default as SQL.
//
// goschema.Domain keeps a literal and an expression apart, and only the
// expression is already SQL. A literal reaches here as the value itself, so
// `abc` has to become `'abc'` before a server can parse the statement carrying
// it.
func quoteDomainDefaultLiteral(literal string) string {
	return "'" + strings.ReplaceAll(literal, "'", "''") + "'"
}

func collectIdentifierNames(
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
	defaultSchema string,
) []string {
	names := []string{defaultSchema}
	names = appendGeneratedIdentifierNames(names, generated)
	return appendDatabaseIdentifierNames(names, database)
}

func appendGeneratedIdentifierNames(
	names []string,
	generated *goschema.Database,
) []string {
	if generated == nil {
		return names
	}
	for _, field := range generated.Fields {
		names = append(names, field.Name)
	}
	for _, table := range generated.Tables {
		names = append(names, table.Schema, table.Name)
		for _, field := range generatedschema.FieldsForTable(generated, table) {
			names = append(names, field.Name)
		}
	}
	for _, index := range generated.Indexes {
		names = append(names, index.Name)
		names = appendQualifiedIdentifier(names, index.TableName)
		names = append(names, index.Fields...)
		names = append(names, index.IncludeColumns...)
		for _, part := range index.Parts {
			names = append(names, part.Name)
		}
	}
	return names
}

func appendDatabaseIdentifierNames(
	names []string,
	database *dbschematypes.DBSchema,
) []string {
	if database == nil {
		return names
	}
	for _, table := range database.Tables {
		names = append(names, table.Schema, table.Name)
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
	}
	for _, index := range database.Indexes {
		names = append(names, index.Schema, index.TableName, index.Name)
		names = append(names, index.Columns...)
		for _, part := range index.Parts {
			names = append(names, part.Name)
		}
	}
	return names
}

func appendQualifiedIdentifier(names []string, value string) []string {
	ref, ok := tableref.Parse(value)
	if !ok {
		return append(names, value)
	}
	if !ref.Qualified {
		return append(names, ref.Name)
	}
	return append(names, ref.Schema, ref.Name)
}
