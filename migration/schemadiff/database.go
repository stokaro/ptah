package schemadiff

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/modelast"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// CompareWithDatabase resolves live catalog identifier equivalence and compares
// the target schema with the connected database schema.
func CompareWithDatabase(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
	runtime DatabaseRuntime,
) (*difftypes.SchemaDiff, error) {
	return completeComparison(CompareWithDatabaseReportingUndecidedAdditions(ctx, conn, desired, database, opts, runtime))
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
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
	runtime DatabaseRuntime,
) (*difftypes.SchemaDiff, Diagnostics, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, Diagnostics{}, err
	}
	if desired == nil || database == nil {
		return nil, Diagnostics{}, fmt.Errorf("%w: comparison requires desired and observed schemas", ptaherr.ErrInvalidSchemaDiff)
	}
	if conn == nil {
		return nil, Diagnostics{}, fmt.Errorf("compare schemas: database connection is nil")
	}
	info := conn.Info()
	// Resolve the comparison-owned toggle before catalog queries. Direct
	// library callers must not run identifier-semantics SQL before reporting a
	// malformed setting; command adapters perform the same check before they
	// load desired sources or connect.
	if err := sqlitevirtual.ValidateToggle(info.Dialect); err != nil {
		return nil, Diagnostics{}, err
	}
	names := collectIdentifierNames(desired, database, info.Schema)
	semantics, err := conn.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return nil, Diagnostics{}, fmt.Errorf("compare schemas: %w", err)
	}
	info.IdentifierSemantics = semantics

	// Resolved here rather than inside the comparison for the same reason the
	// identifier semantics are: a live fact belongs to the side that holds a
	// connection, and the comparison itself must stay a pure function of the
	// two states it is given.
	expressions, err := resolveDomainExpressions(ctx, conn, desired, database)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	// Feature owners attach the same kind of live fact to the objects they
	// own, through their own probes in rolled-back transactions.
	desired, err = normalizeFeatureObjects(ctx, conn, desired, database, info, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	checks, err := resolveCheckExpressions(ctx, conn, desired, database, info.Dialect, semantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	policies, err := resolvePolicyExpressions(ctx, conn, desired, database, semantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	indexes, err := resolveIndexExpressions(ctx, conn, desired, database, semantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	excludes, err := resolveExcludeExpressions(ctx, conn, desired, database, semantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	columns, err := resolveColumnSpellings(ctx, conn, desired, database, semantics, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	triggers, err := resolveTriggerConditions(ctx, conn, desired, database, semantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	arguments, err := resolveRoutineArguments(ctx, conn, desired, database, semantics, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	views, err := resolveViewBodies(ctx, conn, desired, database)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	// Every resolver's answer reaches the comparison the same way: a copy of
	// the options carrying the maps that have something in them. The copy is
	// what keeps the caller's options untouched, which matters because a
	// caller may compare twice with one value.
	opts = withResolvedExpressions(opts, resolvedExpressions{
		domains:   expressions,
		checks:    checks,
		policies:  policies,
		indexes:   indexes,
		excludes:  excludes,
		columns:   columns,
		triggers:  triggers,
		arguments: arguments,
		views:     views,
	})

	return compareWithDatabaseInfoReportingUndecidedAdditions(
		ctx, desired, database, info, opts, runtime,
	)
}

// resolvedExpressions collects what the resolvers above answered, so the
// options are copied once rather than once per family.
type resolvedExpressions struct {
	domains   map[string]config.DomainExpression
	checks    map[string]config.CheckExpression
	policies  map[string]config.PolicyExpression
	indexes   map[string]config.IndexExpression
	excludes  map[string]config.ExcludeExpression
	columns   map[string]config.ColumnSpelling
	triggers  map[string]config.TriggerCondition
	arguments map[string]config.RoutineArguments
	views     map[string]config.ViewBody
}

// empty reports that no server answered for anything, which is every offline
// comparison and every target whose engine rewrites nothing.
func (r resolvedExpressions) empty() bool {
	return len(r.domains) == 0 && len(r.checks) == 0 &&
		len(r.policies) == 0 && len(r.indexes) == 0 && len(r.excludes) == 0 && len(r.columns) == 0 &&
		len(r.triggers) == 0 && len(r.arguments) == 0 && len(r.views) == 0
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
	if len(resolved.checks) > 0 {
		merged.CheckExpressions = resolved.checks
	}
	if len(resolved.policies) > 0 {
		merged.PolicyExpressions = resolved.policies
	}
	if len(resolved.indexes) > 0 {
		merged.IndexExpressions = resolved.indexes
	}
	if len(resolved.excludes) > 0 {
		merged.ExcludeExpressions = resolved.excludes
	}
	if len(resolved.columns) > 0 {
		merged.ColumnSpellings = resolved.columns
	}
	if len(resolved.triggers) > 0 {
		merged.TriggerConditions = resolved.triggers
	}
	if len(resolved.arguments) > 0 {
		merged.RoutineArguments = resolved.arguments
	}
	if len(resolved.views) > 0 {
		merged.ViewBodies = resolved.views
	}
	return merged
}

// resolveTriggerConditions asks the server to print the WHEN condition of
// every declared trigger whose table the database holds.
//
// Only those: a trigger on a table the plan creates is created from its
// declaration unchanged, and its condition has no live table to parse against.
func resolveTriggerConditions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) (map[string]config.TriggerCondition, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	columns := liveTableColumns(database, semantics)
	var probes []dbexprprobe.TriggerConditionProbe
	for _, trigger := range desired.Triggers {
		if strings.TrimSpace(trigger.When) == "" {
			continue
		}
		live, known := columns[exprkey.Table(semantics, trigger.Table)]
		if !known {
			continue
		}
		canonical := trigger
		canonical.Canonicalize()
		probes = append(probes, dbexprprobe.TriggerConditionProbe{
			Key:     exprkey.Trigger(semantics, trigger.Table, trigger.Name),
			Table:   live.name,
			Columns: live.columns,
			Timing:  canonical.Timing,
			Event:   canonical.Event,
			ForEach: canonical.ForEach,
			When:    canonical.When,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}
	conditions, err := dbexprprobe.ResolveTriggerConditions(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return conditions, nil
}

// resolveRoutineArguments asks the server to spell the argument list and the
// result of every declared routine whose name the database also holds.
//
// Only those, for the reason [resolveDomainExpressions] gives: a routine being
// created carries its declaration into the CREATE unchanged. The name is
// matched loosely, on the routine's own name in any schema and case, because a
// probe too many costs one CREATE in a rolled-back transaction and a probe too
// few is a routine dropped and created on every plan.
//
// Each probe is the CREATE the renderer writes for the declared kind,
// arguments and return type, in pg_temp and with a body nobody reads: the
// server spells both the way it would for the plan's own statement. A routine
// that declares neither, a procedure without arguments, has nothing to spell.
//
// CockroachDB refuses a routine in pg_temp and rewrites a body when it stores
// one, so there the probe is the declared routine itself, language and body
// included, in the session's current schema as its view probe is; see
// [routineArgumentsProbe]. Its body is the declaration's stored form, so on
// CockroachDB every declared routine the database holds is probed, whatever it
// declares.
//
// A body that selects `*`, see [dbexprprobe.SelectsStar], is stored with the
// star expanded against the columns its tables hold when the routine is
// created. The probe expands it against the columns they hold now, so its body
// is used only while no table the database shares with the declaration gains
// or loses a column: an answer from today's columns would hide a column the
// plan adds, as it would for a view. Its signature is used either way.
func resolveRoutineArguments(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
	service renderer.Service,
) (map[string]config.RoutineArguments, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	starsAnswerable := !columnListsChange(desired, database, semantics)
	held := make(map[string]bool, len(database.Functions))
	for _, function := range database.Functions {
		held[strings.ToLower(function.Name)] = true
	}
	dialect := conn.Info().Dialect
	seen := make(map[string]bool, len(desired.Functions))
	var probes []dbexprprobe.RoutineArgumentsProbe
	for _, function := range desired.Functions {
		key := exprkey.RoutineArguments(function)
		spellsNothing := strings.TrimSpace(function.Parameters+function.Returns) == "" && !rewritesStoredBodies(dialect)
		if spellsNothing || seen[key] || !held[strings.ToLower(bareName(function.Name))] {
			continue
		}
		seen[key] = true
		probe, ok, err := routineArgumentsProbe(ctx, service, function, key, len(probes), conn.Info(), starsAnswerable)
		if err != nil {
			return nil, err
		}
		if ok {
			probes = append(probes, probe)
		}
	}
	if len(probes) == 0 {
		return nil, nil
	}
	arguments, err := dbexprprobe.ResolveRoutineArguments(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return arguments, nil
}

// resolveViewBodies asks the server to spell the body of every declared view
// and materialized view whose name the database also holds.
//
// Only those, for the reason [resolveDomainExpressions] gives: a view being
// created carries its declaration into the CREATE unchanged. The name is
// matched loosely, on the view's own name in any schema and case, as
// [resolveRoutineArguments] matches a routine, because a probe too many costs
// one CREATE in a rolled-back transaction and a probe too few is a view dropped
// and created on every plan. A view and a materialized view share the
// relation namespace, so either kind in the database makes a declaration of
// either kind worth asking about.
func resolveViewBodies(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
) (map[string]config.ViewBody, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]bool, len(database.Views)+len(database.MatViews))
	for _, view := range database.Views {
		held[strings.ToLower(view.Name)] = true
	}
	for _, view := range database.MatViews {
		held[strings.ToLower(view.Name)] = true
	}
	seen := make(map[string]bool)
	var probes []dbexprprobe.ViewBodyProbe
	ask := func(name, body string) {
		key := exprkey.ViewBody(body)
		if seen[key] || !held[strings.ToLower(bareName(name))] {
			return
		}
		seen[key] = true
		probes = append(probes, dbexprprobe.ViewBodyProbe{Key: key, Body: body})
	}
	for _, view := range desired.Views {
		ask(view.Name, view.Body)
	}
	for _, view := range desired.MaterializedViews {
		ask(view.Name, view.Body)
	}
	if len(probes) == 0 {
		return nil, nil
	}
	bodies, err := dbexprprobe.ResolveViewBodies(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return bodies, nil
}

// bareName is a declared object's own name, without the schema a declaration
// may qualify it with.
func bareName(name string) string {
	if ref, ok := tableref.Parse(name); ok {
		return ref.Name
	}
	return name
}

// routineArgumentsProbe renders the probe for one declared routine, or reports
// that the renderer refused it, in which case the arguments stay unresolved.
// starsAnswerable says whether a body selecting `*` may be answered; see
// [resolveRoutineArguments].
func routineArgumentsProbe(
	ctx context.Context,
	service renderer.Service,
	function schemamodel.Function,
	key string,
	index int,
	info catalog.ServerInfo,
	starsAnswerable bool,
) (dbexprprobe.RoutineArgumentsProbe, bool, error) {
	dialect := info.Dialect
	name := fmt.Sprintf("ptah_routine_probe_%d", index)
	probe := dbexprprobe.RoutineArgumentsProbe{Key: key, Name: name}
	declared := schemamodel.Function{
		Name:       "pg_temp." + name,
		Kind:       function.Kind,
		Parameters: function.Parameters,
		Returns:    function.Returns,
		Language:   "sql",
		Body:       "SELECT NULL",
	}
	if rewritesStoredBodies(dialect) {
		probe.InCurrentSchema = true
		probe.ReadsBody = starsAnswerable || !dbexprprobe.SelectsStar(function.Body)
		declared.Name = name
		declared.Language = function.Language
		declared.Body = function.Body
	}
	result, usable, err := renderProbe(ctx, service, info,
		modelast.FromFunction(declared), ast.NewDropFunction(declared.Name).SetKind(function.Kind))
	if err != nil || !usable {
		return dbexprprobe.RoutineArgumentsProbe{}, false, err
	}
	probe.Statement, probe.Drop = result.Fragments[0], result.Fragments[1]
	return probe, true, nil
}

// columnListsChange reports whether a table the database shares with the
// declaration gains or loses a column under it, which is when a stored star
// expansion and the declared one can differ. Columns are matched by identity
// key; a type change keeps the list, and the star with it.
func columnListsChange(
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) bool {
	live := make(map[string]map[string]bool, len(database.Tables))
	for _, table := range database.Tables {
		columns := make(map[string]bool, len(table.Columns))
		for _, column := range table.Columns {
			columns[semantics.ColumnIdentityKey(column.Name)] = true
		}
		live[exprkey.TableParts(semantics, table.Schema, table.Name)] = columns
	}
	for _, table := range desired.Tables {
		columns, held := live[exprkey.TableParts(semantics, table.Schema, table.Name)]
		if !held {
			continue
		}
		declared := make(map[string]bool, len(columns))
		for _, field := range generatedschema.FieldsForTable(desired, table) {
			declared[semantics.ColumnIdentityKey(field.Name)] = true
		}
		if !maps.Equal(declared, columns) {
			return true
		}
	}
	return false
}

// rewritesStoredBodies reports whether the dialect stores a routine body in a
// form of its own rather than as written, so that only the server can say
// what a declared body becomes. Measured on CockroachDB v26.3.2, `SELECT *
// FROM public.items` is stored as `SELECT public.items.id,
// public.items.title FROM f1.public.items;`, and a PL/pgSQL body is reflowed
// and qualified the same way. PostgreSQL stores prosrc as written
// (stokaro/ptah#4058).
func rewritesStoredBodies(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.CockroachDB
}

// resolveColumnSpellings asks the server to spell the type and default of
// every declared column the database also holds.
//
// Only those, for the reason [resolveDomainExpressions] gives: a column being
// added carries its declaration into the ALTER statement unchanged.
//
// Each probe is the CREATE TABLE the renderer writes for the declared columns,
// on a pg_temp table, so the server reads exactly the text a plan would send.
// Everything that is not the type or the default -- keys, CHECKs, foreign keys,
// generation, comments -- is left out, because the probe table stands alone
// and none of it changes how a type or a default is spelled.
func resolveColumnSpellings(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
	service renderer.Service,
) (map[string]config.ColumnSpelling, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]map[string]string, len(database.Tables))
	for _, table := range database.Tables {
		columns := make(map[string]string, len(table.Columns))
		for _, column := range table.Columns {
			columns[semantics.ColumnIdentityKey(column.Name)] = column.RawType()
		}
		held[exprkey.TableParts(semantics, table.Schema, table.Name)] = columns
	}

	var probes []dbexprprobe.ColumnSpellingProbe
	for _, table := range desired.Tables {
		liveColumns, exists := held[exprkey.TableParts(semantics, table.Schema, table.Name)]
		if !exists {
			continue
		}
		var fields []schemamodel.Field
		for _, field := range generatedschema.FieldsForTable(desired, table) {
			liveType, live := liveColumns[semantics.ColumnIdentityKey(field.Name)]
			if live && needsColumnSpelling(field, liveType) {
				fields = append(fields, field)
			}
		}
		probe, ok, err := columnSpellingProbe(ctx, service, desired, table, fields, len(probes), conn.Info())
		if err != nil {
			return nil, err
		}
		if ok {
			probes = append(probes, probe)
		}
	}
	if len(probes) == 0 {
		return nil, nil
	}
	spellings, err := dbexprprobe.ResolveColumnSpellings(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return spellings, nil
}

// needsColumnSpelling reports whether a column's comparison can depend on the
// server's spelling: it declares a default, or its type is not written the way
// the catalog reports it. Every other column is compared as before, which keeps
// the probe to the columns that need it -- on a large schema, most do not.
func needsColumnSpelling(field schemamodel.Field, liveType string) bool {
	return field.Default != "" || field.DefaultExpr != "" ||
		!strings.EqualFold(strings.TrimSpace(field.Type), strings.TrimSpace(liveType))
}

// columnSpellingProbe renders the probe for one table's columns, or reports
// that the renderer refused them, in which case the columns stay unresolved.
func columnSpellingProbe(
	ctx context.Context,
	service renderer.Service,
	desired *schemamodel.Database,
	table schemamodel.Table,
	fields []schemamodel.Field,
	index int,
	info catalog.ServerInfo,
) (dbexprprobe.ColumnSpellingProbe, bool, error) {
	dialect := info.Dialect
	if len(fields) == 0 {
		return dbexprprobe.ColumnSpellingProbe{}, false, nil
	}
	probe := dbexprprobe.ColumnSpellingProbe{Table: fmt.Sprintf("pg_temp.ptah_column_probe_%d", index)}
	tableNode := ast.NewCreateTable(probe.Table)
	nodes := []ast.Node{tableNode}
	for position, field := range fields {
		column := modelast.FromFieldWithoutForeignKeys(typeAndDefaultOnly(field), desired.Enums, dialect)
		tableNode.AddColumn(column)
		columnTable := fmt.Sprintf("%s_%d", probe.Table, position)
		nodes = append(nodes, &ast.CreateTableNode{Name: columnTable, Columns: []*ast.ColumnNode{column}})
		probe.Columns = append(probe.Columns, dbexprprobe.ColumnSpellingColumn{
			Key:   exprkey.Column(dialect, table.Schema, table.Name, field.Name),
			Name:  column.Name,
			Table: columnTable,
		})
	}
	result, usable, err := renderProbe(ctx, service, info, nodes...)
	if err != nil || !usable {
		return dbexprprobe.ColumnSpellingProbe{}, false, err
	}
	probe.Statement = result.Fragments[0]
	for position := range probe.Columns {
		probe.Columns[position].Statement = result.Fragments[position+1]
	}
	return probe, true, nil
}

// typeAndDefaultOnly strips a field to what its probe column needs.
func typeAndDefaultOnly(field schemamodel.Field) schemamodel.Field {
	field.Nullable = true
	field.NotNullConstraintName = ""
	field.Primary, field.Unique = false, false
	field.Check, field.CheckName, field.CheckNotEnforced = "", "", false
	field.Foreign, field.ForeignKeyName = "", ""
	field.ForeignKeyMatch, field.ForeignKeyNotEnforced = "", false
	field.GeneratedExpression, field.GeneratedKind = "", ""
	field.IdentityGeneration = ""
	field.Comment = ""
	return field
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
	desired *schemamodel.Database,
	database *catalog.Database,
) (map[string]config.DomainExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Domains))
	for _, domain := range database.Domains {
		held[strings.ToLower(domain.QualifiedName())] = struct{}{}
	}

	probes := make([]dbexprprobe.DomainExpressionProbe, 0, len(desired.Domains))
	for _, domain := range desired.Domains {
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

// resolveCheckExpressions normalizes the expression of every CHECK the
// comparison compares -- declared on the table, or synthesized from a column's
// check or a table's checks list -- that the database also holds.
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
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) (map[string]config.CheckExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "CHECK") {
			held[exprkey.CheckParts(semantics, constraint.Schema, constraint.TableName, constraint.Name)] = struct{}{}
		}
	}
	columns := liveTableColumns(database, semantics)

	// The checks the comparison compares, not only the declared ones: a
	// column's check is compared as a synthesized constraint, and a check the
	// resolver never saw is compared by text.
	compared := compare.ComparedCheckConstraints(desired, database, dialect, semantics)
	probes := make([]dbexprprobe.CheckExpressionProbe, 0, len(compared))
	for _, constraint := range compared {
		key := exprkey.Check(semantics, constraint.Table, constraint.Name)
		if _, exists := held[key]; !exists {
			continue
		}
		live, known := columns[exprkey.Table(semantics, constraint.Table)]
		if !known {
			continue
		}
		probes = append(probes, dbexprprobe.CheckExpressionProbe{
			Key:        key,
			Table:      live.name,
			Columns:    live.columns,
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
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) (map[string]config.PolicyExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.RLSPolicies))
	for _, policy := range database.RLSPolicies {
		held[exprkey.Policy(semantics, policy.Table, policy.Name)] = struct{}{}
	}
	columns := liveTableColumns(database, semantics)

	probes := make([]dbexprprobe.PolicyExpressionProbe, 0, len(desired.RLSPolicies))
	for _, policy := range desired.RLSPolicies {
		key := exprkey.Policy(semantics, policy.Table, policy.Name)
		if _, exists := held[key]; !exists {
			continue
		}
		live, known := columns[exprkey.Table(semantics, policy.Table)]
		if !known {
			continue
		}
		probes = append(probes, dbexprprobe.PolicyExpressionProbe{
			Key:       key,
			Table:     live.name,
			Columns:   live.columns,
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

// resolveExcludeExpressions normalizes the elements and WHERE clause of every
// declared EXCLUDE constraint the database holds under the same name.
//
// Only those: a constraint the plan adds is created from its declaration, and
// one the database lacks has nothing to be compared with.
func resolveExcludeExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) (map[string]config.ExcludeExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "EXCLUDE") {
			held[exprkey.ExcludeParts(semantics, constraint.Schema, constraint.TableName, constraint.Name)] = struct{}{}
		}
	}
	columns := liveTableColumns(database, semantics)

	var probes []dbexprprobe.ExcludeExpressionProbe
	for _, constraint := range compare.ComparedExcludeConstraints(desired, semantics) {
		key := exprkey.Exclude(semantics, constraint.Table, constraint.Name)
		if _, exists := held[key]; !exists {
			continue
		}
		live, known := columns[exprkey.Table(semantics, constraint.Table)]
		if !known {
			continue
		}
		probes = append(probes, dbexprprobe.ExcludeExpressionProbe{
			Key:         key,
			Table:       live.name,
			Columns:     live.columns,
			UsingMethod: constraint.UsingMethod,
			Elements:    constraint.ExcludeElements,
			Where:       constraint.WhereCondition,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	excludes, err := dbexprprobe.ResolveExcludeExpressions(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	return excludes, nil
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
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) (map[string]config.IndexExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]struct{}, len(database.Indexes))
	for _, index := range database.Indexes {
		held[exprkey.IndexParts(semantics, index.Schema, index.TableName, index.Name)] = struct{}{}
	}
	columns := liveTableColumns(database, semantics)

	// The owner is resolved the way the comparator resolves it, because an
	// index declaration does not always carry its table: `TableName` is the
	// cross-table override and is empty for the ordinary case, where the owner
	// comes from the struct or block the index was declared inside.
	owners := schemamodel.ResolveIndexOwners(desired.Indexes, desired.Tables, desired.MaterializedViews)

	probes := make([]dbexprprobe.IndexExpressionProbe, 0, len(desired.Indexes))
	for position, index := range desired.Indexes {
		expression, parts := declaredIndexExpression(index)
		if expression == "" && strings.TrimSpace(index.Condition) == "" {
			continue
		}
		key := exprkey.Index(semantics, owners[position], index.Name)
		if _, exists := held[key]; !exists {
			continue
		}
		live, known := columns[exprkey.Table(semantics, owners[position])]
		if !known {
			continue
		}
		probes = append(probes, dbexprprobe.IndexExpressionProbe{
			Key:        key,
			Table:      live.name,
			Columns:    live.columns,
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
// [ptah.run/core/schemamodel.Index] gives: the two spellings duplicate each
// other and only Parts distinguishes an expression from a column.
func declaredIndexExpression(index schemamodel.Index) (expression string, parts []string) {
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

// liveProbeTable is one live table in the shape a probe needs: its name as
// the server stores it, which the probe table takes, and its columns.
type liveProbeTable struct {
	name    string
	columns []dbexprprobe.CheckProbeColumn
}

// liveTableColumns projects every live table into the shape a probe needs,
// keyed by the table's identity.
func liveTableColumns(
	current *catalog.Database,
	semantics identifier.Semantics,
) map[string]liveProbeTable {
	tables := make(map[string]liveProbeTable, len(current.Tables))
	for _, table := range current.Tables {
		probeColumns := make([]dbexprprobe.CheckProbeColumn, 0, len(table.Columns))
		for _, column := range table.Columns {
			probeColumns = append(probeColumns, dbexprprobe.CheckProbeColumn{
				Name: column.Name,
				Type: column.RawType(),
			})
		}
		tables[exprkey.TableParts(semantics, table.Schema, table.Name)] = liveProbeTable{
			name:    table.Name,
			columns: probeColumns,
		}
	}
	return tables
}

// normalizeFeatureObjects asks the owners of declared feature objects to
// attach the connected server's own spelling of what they declare, the way the
// resolvers above do for the common families. The desired schema is copied,
// never changed in place; with no declared object it is returned as it is.
func normalizeFeatureObjects(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	runtime DatabaseRuntime,
) (*schemamodel.Database, error) {
	if desired.FeatureObjects.Len() == 0 || info.Dialect == "" {
		return desired, nil
	}
	result, err := runtime.NormalizeObjects(ctx, schemaext.NormalizationRequest{
		Target: info.Dialect, Identifiers: info.IdentifierSemantics, Capabilities: info.Capabilities,
		Desired: schemaext.ObjectState{Objects: desired.FeatureObjects, Coverage: desired.FeatureCoverage},
		Current: schemaext.ObjectState{Objects: database.FeatureObjects, Coverage: database.FeatureCoverage},
		Session: conn,
	})
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if !result.Complete {
		return nil, fmt.Errorf("%w: feature normalization did not complete", schemaext.ErrInvalidValue)
	}
	normalized := *desired
	normalized.FeatureObjects = result.Desired.Objects
	return &normalized, nil
}

// quoteDomainDefaultLiteral renders a declared literal default as SQL.
//
// schemamodel.Domain keeps a literal and an expression apart, and only the
// expression is already SQL. A literal reaches here as the value itself, so
// `abc` has to become `'abc'` before a server can parse the statement carrying
// it.
func quoteDomainDefaultLiteral(literal string) string {
	return "'" + strings.ReplaceAll(literal, "'", "''") + "'"
}

func collectIdentifierNames(
	desired *schemamodel.Database,
	database *catalog.Database,
	defaultSchema string,
) []string {
	names := []string{defaultSchema}
	names = appendGeneratedIdentifierNames(names, desired)
	return appendDatabaseIdentifierNames(names, database)
}

func appendGeneratedIdentifierNames(
	names []string,
	desired *schemamodel.Database,
) []string {
	if desired == nil {
		return names
	}
	for _, field := range desired.Fields {
		names = append(names, field.Name)
	}
	for _, table := range desired.Tables {
		names = append(names, table.Schema, table.Name)
		for _, field := range generatedschema.FieldsForTable(desired, table) {
			names = append(names, field.Name)
		}
	}
	for _, index := range desired.Indexes {
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
	database *catalog.Database,
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
