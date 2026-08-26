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
	"go.5x5.cz/ptah/internal/dbexprprobe"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
	policies, err := resolvePolicyExpressions(ctx, conn, generated, database)
	if err != nil {
		return nil, nil, err
	}
	indexes, err := resolveIndexExpressions(ctx, conn, generated, database)
	if err != nil {
		return nil, nil, err
	}
	// Every resolver's answer reaches the comparison the same way: a copy of
	// the options carrying the maps that have something in them. The copy is
	// what keeps the caller's options untouched, which matters because a
	// caller may compare twice with one value.
	opts = withResolvedExpressions(opts, resolvedExpressions{
		domains:    expressions,
		aggregates: bodies,
		checks:     checks,
		policies:   policies,
		indexes:    indexes,
	})

	return compareWithDatabaseInfoReportingUndecidedAdditions(
		generated, database, info, opts,
	)
}

// resolvedExpressions collects what the resolvers above answered, so the
// options are copied once rather than once per family.
type resolvedExpressions struct {
	domains    map[string]config.DomainExpression
	aggregates map[string]config.ContinuousAggregateBody
	checks     map[string]config.CheckExpression
	policies   map[string]config.PolicyExpression
	indexes    map[string]config.IndexExpression
}

// empty reports that no server answered for anything, which is every offline
// comparison and every target whose engine rewrites nothing.
func (r resolvedExpressions) empty() bool {
	return len(r.domains) == 0 && len(r.aggregates) == 0 && len(r.checks) == 0 &&
		len(r.policies) == 0 && len(r.indexes) == 0
}

// withResolvedExpressions returns the options the comparison should run under.
//
// The caller's own value is returned unchanged when nothing was resolved, so a
// comparison that asked no server is byte-for-byte the one that ran before any
// of these resolvers existed.
func withResolvedExpressions(
	opts *config.CompareOptions,
	resolved resolvedExpressions,
) *config.CompareOptions {
	if resolved.empty() {
		return opts
	}
	merged := config.DefaultCompareOptions()
	if opts != nil {
		*merged = *opts
	}
	if len(resolved.domains) > 0 {
		merged.DomainExpressions = resolved.domains
	}
	if len(resolved.aggregates) > 0 {
		merged.ContinuousAggregateBodies = resolved.aggregates
	}
	if len(resolved.checks) > 0 {
		merged.CheckExpressions = resolved.checks
	}
	if len(resolved.policies) > 0 {
		merged.PolicyExpressions = resolved.policies
	}
	if len(resolved.indexes) > 0 {
		merged.IndexExpressions = resolved.indexes
	}
	return merged
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

	probes := make([]dbexprprobe.DomainExpressionProbe, 0, len(generated.Domains))
	for _, domain := range generated.Domains {
		if _, exists := held[strings.ToLower(domain.QualifiedName())]; !exists {
			continue
		}
		defaultExpression := domain.DefaultExpr
		if defaultExpression == "" && domain.Default != "" {
			defaultExpression = quoteDomainDefaultLiteral(domain.Default)
		}
		probes = append(probes, dbexprprobe.DomainExpressionProbe{
			Key:      domain.QualifiedName(),
			BaseType: domain.BaseType,
			Check:    domain.Check,
			Default:  defaultExpression,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	expressions, err := dbexprprobe.ResolveDomainExpressions(ctx, conn, probes)
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

	probes := make([]dbexprprobe.CheckExpressionProbe, 0, len(generated.Constraints))
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
		probes = append(probes, dbexprprobe.CheckExpressionProbe{
			Key:        key,
			Columns:    probeColumns,
			Expression: constraint.CheckExpression,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	checks, err := dbexprprobe.ResolveCheckExpressions(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return checks, nil
}

// resolvePolicyExpressions normalizes the declared clauses of every RLS policy
// the database also holds.
//
// Only those, for the reason [resolveDomainExpressions] gives: a policy being
// created carries its declaration into the CREATE statement unchanged, and one
// being dropped has no declaration left to normalize.
func resolvePolicyExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
) (map[string]config.PolicyExpression, error) {
	if generated == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.RLSPolicies))
	for _, policy := range database.RLSPolicies {
		held[checkExpressionKey(policy.Table, policy.Name)] = struct{}{}
	}
	columns := liveTableColumns(database)

	probes := make([]dbexprprobe.PolicyExpressionProbe, 0, len(generated.RLSPolicies))
	for _, policy := range generated.RLSPolicies {
		key := checkExpressionKey(policy.Table, policy.Name)
		if _, exists := held[key]; !exists {
			continue
		}
		probeColumns, known := columns[foldCheckTableName(policy.Table)]
		if !known {
			continue
		}
		probes = append(probes, dbexprprobe.PolicyExpressionProbe{
			Key:       key,
			Columns:   probeColumns,
			Using:     policy.UsingExpression,
			WithCheck: policy.WithCheckExpression,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	policies, err := dbexprprobe.ResolvePolicyExpressions(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return policies, nil
}

// resolveIndexExpressions normalizes the declared expression and predicate of
// every index the database also holds.
//
// An index over plain columns with no predicate is skipped: there is nothing
// for the server to rewrite, and probing one would cost a statement to learn
// what the declaration already says.
func resolveIndexExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
) (map[string]config.IndexExpression, error) {
	if generated == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Indexes))
	for _, index := range database.Indexes {
		held[strings.ToLower(strings.TrimSpace(index.Name))] = struct{}{}
	}
	columns := liveTableColumns(database)

	// The owner is resolved the way the comparator resolves it, because an
	// index declaration does not always carry its table: `TableName` is the
	// cross-table override and is empty for the ordinary case, where the owner
	// comes from the struct or block the index was declared inside.
	owners := goschema.ResolveIndexOwners(generated.Indexes, generated.Tables, generated.MaterializedViews)

	probes := make([]dbexprprobe.IndexExpressionProbe, 0, len(generated.Indexes))
	for position, index := range generated.Indexes {
		expression, parts := declaredIndexExpression(index)
		if expression == "" && strings.TrimSpace(index.Condition) == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(index.Name))
		if _, exists := held[key]; !exists {
			continue
		}
		probeColumns, known := columns[foldCheckTableName(owners[position])]
		if !known {
			continue
		}
		probes = append(probes, dbexprprobe.IndexExpressionProbe{
			Key:        key,
			Columns:    probeColumns,
			Expression: expression,
			Parts:      parts,
			Predicate:  index.Condition,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	indexes, err := dbexprprobe.ResolveIndexExpressions(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return indexes, nil
}

// declaredIndexExpression separates an index over an EXPRESSION from one over
// plain columns, which is what decides how the probe writes its CREATE INDEX.
//
// Parts is preferred over Fields where it is filled, for the reason
// [go.5x5.cz/ptah/core/goschema.Index] gives: the two spellings duplicate each
// other and only Parts distinguishes an expression from a column.
func declaredIndexExpression(index goschema.Index) (expression string, parts []string) {
	for _, part := range index.Parts {
		if strings.TrimSpace(part.Expr) != "" {
			return part.Expr, nil
		}
		if strings.TrimSpace(part.Name) != "" {
			parts = append(parts, part.Name)
		}
	}
	if len(parts) == 0 {
		parts = index.Fields
	}
	return "", parts
}

// liveTableColumns projects every live table's columns into the shape a probe
// needs, keyed by the table's bare name.
func liveTableColumns(database *dbschematypes.DBSchema) map[string][]dbexprprobe.CheckProbeColumn {
	columns := make(map[string][]dbexprprobe.CheckProbeColumn, len(database.Tables))
	for _, table := range database.Tables {
		probeColumns := make([]dbexprprobe.CheckProbeColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			probeColumns = append(probeColumns, dbexprprobe.CheckProbeColumn{
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

	probes := make([]dbexprprobe.ContinuousAggregateProbe, 0, len(generated.ContinuousAggregates))
	for _, aggregate := range generated.ContinuousAggregates {
		if _, exists := held[strings.ToLower(aggregate.QualifiedName())]; !exists {
			continue
		}
		probes = append(probes, dbexprprobe.ContinuousAggregateProbe{
			Key:              aggregate.QualifiedName(),
			Schema:           aggregate.Schema,
			Body:             aggregate.Body,
			MaterializedOnly: aggregate.MaterializedOnly,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	bodies, err := dbexprprobe.ResolveContinuousAggregateBodies(ctx, conn, probes)
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
