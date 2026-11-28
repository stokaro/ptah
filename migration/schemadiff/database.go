package schemadiff

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/modelast"
	"ptah.run/internal/offlinenames"
	"ptah.run/internal/routinebody"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/tableidentity"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/schemadiff/internal/compare"
)

// CompareWithDatabase resolves live catalog identifier equivalence and compares
// the target schema with the connected database schema.
func CompareWithDatabase(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
	runtime DatabaseRuntime,
) (*difftypes.SchemaDiff, error) {
	return completeComparison(CompareWithDatabaseReportingUndecidedAdditions(ctx, conn, desired, database, opts, runtime))
}

// IdentifierResolver is a connected server that says how it compares names:
// its description, and the identifier rules it resolves for a set of names.
// The DatabaseConnection type of ptah.run/dbschema implements it, and so does
// every [Connection].
type IdentifierResolver interface {
	Info() catalog.ServerInfo
	ResolveIdentifierSemantics(ctx context.Context, names []string) (identifier.Semantics, error)
}

// ResolveIdentifierSemantics asks resolver how its server compares every name
// a comparison of desired with database meets, the names [CompareWithDatabase]
// asks its own connection about, and returns the server's rules.
//
// It is for a caller that compares two states offline while it holds a
// connection to a database of the target's engine, such as a dev database
// both states are planned for. Passed as
// [config.CompareOptions.IdentifierSemantics], the answer pairs names the way
// that server does, and it covers every name the comparison checks. On SQL
// Server that answer comes from the database collation, which no offline rule
// knows.
//
// Either state nil, or a nil resolver, including an interface holding a nil
// pointer, is refused with an error satisfying
// errors.Is(err, ptaherr.ErrInvalidSchemaDiff). An error from the target
// runtime or from the server is returned as it is. The dialect is the one
// resolver's Info names.
func ResolveIdentifierSemantics(
	ctx context.Context,
	resolver IdentifierResolver,
	desired *schemamodel.Database,
	database *catalog.Database,
	runtime schemaext.TargetResolver,
) (identifier.Semantics, error) {
	if desired == nil || database == nil || noResolver(resolver) {
		return identifier.Semantics{}, fmt.Errorf("%w: resolving identifiers requires a server and both schemas", ptaherr.ErrInvalidSchemaDiff)
	}
	if runtime == nil {
		return identifier.Semantics{}, fmt.Errorf("%w: resolving identifiers requires a target runtime", ptaherr.ErrInvalidSchemaDiff)
	}
	target, err := runtime.ResolveTarget(resolver.Info().Dialect)
	if err != nil {
		return identifier.Semantics{}, err
	}
	return resolveComparisonIdentifiers(ctx, resolver, desired, database, target)
}

// noResolver reports whether resolver is nil or holds a nil pointer, such as
// a nil *dbschema.DatabaseConnection, which has no Info to read.
func noResolver(resolver IdentifierResolver) bool {
	if resolver == nil {
		return true
	}
	value := reflect.ValueOf(resolver)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

// resolveComparisonIdentifiers asks resolver about every name a comparison of
// desired with database meets under target: the names the comparison keys,
// and the names the target's facets carry.
func resolveComparisonIdentifiers(
	ctx context.Context,
	resolver IdentifierResolver,
	desired *schemamodel.Database,
	database *catalog.Database,
	target schemaext.TargetSelection,
) (identifier.Semantics, error) {
	info := resolver.Info()
	names := collectIdentifierNames(desired, database, info.Schema, info.Dialect)
	slots := append(desired.FacetSlots(), database.FacetSlots()...)
	names, err := appendFacetIdentifierNames(names, slots, target)
	if err != nil {
		return identifier.Semantics{}, err
	}
	return resolver.ResolveIdentifierSemantics(ctx, names)
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
	conn Connection,
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
	target, err := runtime.ResolveTarget(info.Dialect)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	semantics, err := resolveComparisonIdentifiers(ctx, conn, desired, database, target)
	if err != nil {
		return nil, Diagnostics{}, fmt.Errorf("compare schemas: %w", err)
	}
	info.IdentifierSemantics = semantics

	// Feature owners attach live facts to the objects they own, through their
	// own probes in rolled-back transactions. They are resolved here rather
	// than inside the comparison for the same reason the identifier semantics
	// are: a live fact belongs to the side that holds a connection, and the
	// comparison itself must stay a pure function of the two states it is
	// given.
	desired, err = normalizeFeatureObjects(ctx, conn, desired, database, info, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired, err = normalizeFeatureFacets(ctx, conn, desired, database, info, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	rules, err := runtime.ComparisonRules(target.Name())
	if err != nil {
		return nil, Diagnostics{}, err
	}
	resolved, err := resolveExpressions(ctx, conn, desired, database, info, rules, heldGeneratedExpressions(opts), runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	// Every resolver's answer reaches the comparison the same way: a copy of
	// the options carrying the maps that have something in them. The copy is
	// what keeps the caller's options untouched, which matters because a
	// caller may compare twice with one value.
	opts = withResolvedExpressions(opts, resolved)

	diff, diagnostics, err := compareWithDatabaseInfoReportingUndecidedAdditions(
		ctx, desired, database, info, opts, runtime, conn,
	)
	if err == nil {
		warnUnspelledColumns(ctx, resolved.unspelledColumns, desired, diff, info.Dialect)
		// The plan this comparison feeds may replace a CHECK only because
		// its conditions were compared as written, so the warning precedes
		// it.
		rundiag.Settle(ctx, rundiag.UnformattedChecks)
	}
	return diff, diagnostics, err
}

// resolveExpressions asks the server how it spells each declared expression
// the comparison reads, one resolver per family.
func resolveExpressions(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	rules schemacomparison.Rules,
	heldGenerated map[string]config.GeneratedExpression,
	runtime DatabaseRuntime,
) (resolvedExpressions, error) {
	semantics := info.IdentifierSemantics
	var resolved resolvedExpressions
	var err error
	// A target that decides CHECK equivalence itself is asked during the
	// comparison, after its lowering; see withTargetCheckDecisions. Its server
	// spells no CHECK here.
	if rules.Checks == nil {
		if resolved.checks, err = resolveCheckExpressions(ctx, conn, desired, database, info.Dialect, semantics); err != nil {
			return resolvedExpressions{}, err
		}
	}
	if resolved.indexes, err = resolveIndexExpressions(ctx, conn, desired, database, semantics); err != nil {
		return resolvedExpressions{}, err
	}
	if resolved.generated, err = resolveGeneratedExpressions(ctx, conn, desired, database, semantics, heldGenerated); err != nil {
		return resolvedExpressions{}, err
	}
	// A column batch owner supplies the offline type and default facts. Domain
	// identities retain common comparison ownership and need no spelling probe.
	if rules.ColumnService == nil {
		resolved.columns, resolved.unspelledColumns, err = resolveColumnSpellings(ctx, conn, desired, database, semantics, runtime)
		if err != nil {
			return resolvedExpressions{}, err
		}
	}
	if resolved.triggers, err = resolveTriggerConditions(ctx, conn, desired, database, semantics); err != nil {
		return resolvedExpressions{}, err
	}
	if resolved.arguments, err = resolveRoutineArguments(ctx, conn, desired, database, semantics, runtime); err != nil {
		return resolvedExpressions{}, err
	}
	if resolved.views, err = resolveViewBodies(ctx, conn, desired, database, info.Dialect); err != nil {
		return resolvedExpressions{}, err
	}
	return resolved, nil
}

// resolvedExpressions collects what the resolvers above answered, so the
// options are copied once rather than once per family.
type resolvedExpressions struct {
	checks    map[string]config.CheckExpression
	indexes   map[string]config.IndexExpression
	generated map[string]config.GeneratedExpression
	columns   map[string]config.ColumnSpelling
	triggers  map[string]config.TriggerCondition
	arguments map[string]config.RoutineArguments
	views     map[string]config.ViewBody

	// unspelledColumns are the columns the server refused to spell. They are
	// reported once the comparison has said which of them it changes.
	unspelledColumns []dbexprprobe.ColumnSpellingRefusal
}

// empty reports that no server answered for anything, which is every offline
// comparison and every target whose engine rewrites nothing.
func (r resolvedExpressions) empty() bool {
	return len(r.checks) == 0 && len(r.indexes) == 0 &&
		len(r.generated) == 0 && len(r.columns) == 0 &&
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
	if len(resolved.checks) > 0 {
		merged.CheckExpressions = resolved.checks
	}
	if len(resolved.indexes) > 0 {
		merged.IndexExpressions = resolved.indexes
	}
	if len(resolved.generated) > 0 {
		// A caller may hold answers from another server already: `ptah
		// compare` asks a dev database about Oracle's columns. Those stay.
		generated := maps.Clone(merged.GeneratedExpressions)
		if generated == nil {
			generated = make(map[string]config.GeneratedExpression, len(resolved.generated))
		}
		for key, answer := range resolved.generated {
			if _, held := generated[key]; !held {
				generated[key] = answer
			}
		}
		merged.GeneratedExpressions = generated
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
	conn Connection,
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
			Object:  fmt.Sprintf("trigger %s on %s", trigger.Name, trigger.Table),
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
// Only those, for the reason [resolveCheckExpressions] gives: a routine being
// created carries its declaration into the CREATE unchanged. The name is
// matched loosely, on the routine's own name in any schema and case, because a
// probe too many costs one CREATE in a rolled-back transaction and a probe too
// few is a routine dropped and created on every plan.
//
// Each probe is the CREATE the renderer writes for the declared kind,
// arguments and return type, in pg_temp and with a body nobody reads: the
// server spells both the way it would for the plan's own statement. A routine
// that declares neither, a procedure without arguments, has nothing to spell,
// unless its body is SQL-standard.
//
// On PostgreSQL a SQL-standard body, `BEGIN ATOMIC ... END` or `RETURN
// expression`, is printed back in the server's own spelling, so its probe
// carries the declared body under the routine's own name and reads the stored
// body back; see [routineArgumentsProbe]. A body the server refuses there
// leaves the signature to a second CREATE with a body nobody reads, and the
// body compares as written.
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
// created, on CockroachDB and in a PostgreSQL SQL-standard body alike. The
// probe expands it against the columns they hold now, so its body is used only
// while no table the database shares with the declaration gains or loses a
// column: an answer from today's columns would hide a column the plan adds, as
// it would for a view. Its signature is used either way.
func resolveRoutineArguments(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
	service renderer.Service,
) (map[string]config.RoutineArguments, error) {
	if desired == nil || database == nil || !dbexprprobe.SupportsRoutineArguments(conn.Info().Dialect) {
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
		spellsNothing := strings.TrimSpace(function.Parameters+function.Returns) == "" && !rewritesStoredBodies(dialect) &&
			!probesSQLStandardBody(dialect, function)
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
// Only those, for the reason [resolveCheckExpressions] gives: a view being
// created carries its declaration into the CREATE unchanged. The name is
// matched loosely, on the view's own name in any schema and case, as
// [resolveRoutineArguments] matches a routine, because a probe too many costs
// one CREATE in a rolled-back transaction and a probe too few is a view dropped
// and created on every plan. A view and a materialized view share the
// relation namespace, so either kind in the database makes a declaration of
// either kind worth asking about. A materialized view is asked about its query,
// [compare.MaterializedViewQuery], which is the body its comparison looks up: a
// plain view does not take WITH NO DATA, and the server refuses the probe.
func resolveViewBodies(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
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
	ask := func(kind, name, body string) {
		key := exprkey.ViewBody(body)
		if seen[key] || !held[strings.ToLower(bareName(name))] {
			return
		}
		seen[key] = true
		probes = append(probes, dbexprprobe.ViewBodyProbe{Key: key, Body: body, Object: kind + " " + name})
	}
	for _, view := range desired.Views {
		ask("view", view.Name, view.Body)
	}
	for _, view := range desired.MaterializedViews {
		ask("materialized view", view.Name, compare.MaterializedViewQuery(view.Body, dialect))
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
	probe := dbexprprobe.RoutineArgumentsProbe{Key: key, Name: name, Object: "routine " + function.Name}
	declared := schemamodel.Function{
		Name:       "pg_temp." + name,
		Kind:       function.Kind,
		Parameters: function.Parameters,
		Returns:    function.Returns,
		Language:   "sql",
		Body:       "SELECT NULL",
	}
	switch {
	case rewritesStoredBodies(dialect):
		probe.InCurrentSchema = true
		probe.ReadsBody = starsAnswerable || !dbexprprobe.SelectsStar(function.Body)
		declared.Name = name
		declared.Language = function.Language
		declared.Body = function.Body
	case probesSQLStandardBody(dialect, function) && (starsAnswerable || !dbexprprobe.SelectsStar(function.Body)):
		// PostgreSQL keeps a SQL-standard body parsed and prints it back in
		// its own spelling: `BEGIN ATOMIC SELECT count(*) FROM v WHERE a > x;
		// END` reads back as `BEGIN ATOMIC\n SELECT count(*) AS count\n
		// FROM v\n  WHERE (v.a > f.x);\nEND` for a routine f
		// (stokaro/ptah#5021). The probe creates the declared body, which
		// reads what the target holds, so its stored form compares with the
		// one the target holds. The copy takes the routine's own name in
		// pg_temp, because the server qualifies a parameter with it; an
		// unqualified name never finds a routine in pg_temp, and the probe is
		// rolled back. A body that reads something the target does not hold
		// yet refuses the copy, and Fallback answers for the signature.
		probe.ReadsBody = true
		probe.SQLStandardBody = true
		probe.Name = bareName(function.Name)
		declared.Name = "pg_temp." + probe.Name
		declared.Language = function.Language
		declared.Body = function.Body
	}
	result, usable, err := renderProbe(ctx, service, info,
		modelast.FromFunctionFor(declared, dialect), ast.NewDropFunction(declared.Name).SetKind(function.Kind))
	if err != nil || !usable {
		return dbexprprobe.RoutineArgumentsProbe{}, false, err
	}
	probe.Statement, probe.Drop = result.Fragments[0].SQL, result.Fragments[1].SQL
	probe.BareBody = routinebody.SQLStandard(declared.Language, declared.Body)
	if probe.SQLStandardBody {
		signature := declared
		signature.Language = "sql"
		signature.Body = "SELECT NULL"
		fallback, usable, err := renderProbe(ctx, service, info,
			modelast.FromFunctionFor(signature, dialect), ast.NewDropFunction(signature.Name).SetKind(function.Kind))
		if err != nil {
			return dbexprprobe.RoutineArgumentsProbe{}, false, err
		}
		if usable {
			probe.Fallback = fallback.Fragments[0].SQL
		}
	}
	return probe, true, nil
}

// probesSQLStandardBody reports whether the probe for function carries its
// body: a SQL-standard body on an engine whose descriptor says a probe may,
// see [descriptor.Comparison.ProbesSQLStandardBodies]. YugabyteDB keeps a
// routine created in pg_temp past the rollback, see
// [dbexprprobe.RoutineArgumentsProbe.Drop], and a copy whose body reads the
// target's tables would then hold them, so its probe stays on the signature.
func probesSQLStandardBody(dialect string, function schemamodel.Function) bool {
	return comparisons.Get(dialect).ProbesSQLStandardBodies &&
		routinebody.SQLStandard(function.Language, function.Body)
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
	return comparisons.Get(dialect).RewritesRoutineBodies
}

// resolveColumnSpellings asks the server to spell the type and default of
// every declared column the database also holds.
//
// Only those, for the reason [resolveCheckExpressions] gives: a column being
// added carries its declaration into the ALTER statement unchanged.
//
// Each probe is the CREATE TABLE the renderer writes for the declared columns,
// on a pg_temp table, so the server reads exactly the text a plan would send.
// Everything that is not the type or the default -- keys, CHECKs, foreign keys,
// generation, comments -- is left out, because the probe table stands alone
// and none of it changes how a type or a default is spelled.
func resolveColumnSpellings(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
	service renderer.Service,
) (map[string]config.ColumnSpelling, []dbexprprobe.ColumnSpellingRefusal, error) {
	if desired == nil || database == nil || !dbexprprobe.SupportsColumnSpellings(conn.Info().Dialect) {
		return nil, nil, nil
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
			return nil, nil, err
		}
		if ok {
			probes = append(probes, probe)
		}
	}
	if len(probes) == 0 {
		return nil, nil, nil
	}
	spellings, refusals, err := dbexprprobe.ResolveColumnSpellings(ctx, conn, probes)
	if err != nil {
		return nil, nil, fmt.Errorf("compare schemas: %w", err)
	}
	return spellings, refusals, nil
}

// warnUnspelledColumns raises one warning for the columns the server refused
// to spell whose type or default diff changes, and writes it now, for the
// reason [warnUnspelledGeneratedColumns] gives.
//
// Only a changed column is named. Compared as written, a type or a default can
// read as a change it is not, and the warning says that the change may be
// one; a column the comparison leaves alone is compared correctly either way,
// and naming it would repeat the same lines on every run.
func warnUnspelledColumns(
	ctx context.Context,
	refusals []dbexprprobe.ColumnSpellingRefusal,
	desired *schemamodel.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	if len(refusals) == 0 || diff == nil {
		return
	}
	changed := changedColumnKeys(desired, diff, dialect)
	stream := rundiag.Stream(ctx)
	warned := false
	for _, refusal := range refusals {
		if !changed[refusal.Key] {
			continue
		}
		warned = true
		column := qualifiedColumnName(refusal.Schema, refusal.Table, refusal.Column)
		_ = rundiag.Warn(ctx, stream, rundiag.UnspelledColumns, rundiag.Record{
			Key: refusal.Key,
			Line: fmt.Sprintf("column %s has its type and default compared as written, not in the form the server stores: "+
				"the server refused the temporary table that spells them: %v", column, refusal.Reason),
			Attrs: []slog.Attr{slog.String("column", column), slog.String("reason", refusal.Reason.Error())},
		})
	}
	if warned {
		rundiag.Settle(ctx, rundiag.UnspelledColumns)
	}
}

// changedColumnKeys is the probe key of each declared column whose type or
// default diff changes.
func changedColumnKeys(desired *schemamodel.Database, diff *difftypes.SchemaDiff, dialect string) map[string]bool {
	tables := make(map[string]schemamodel.Table, len(desired.Tables))
	for _, table := range desired.Tables {
		tables[table.QualifiedName()] = table
	}
	keys := make(map[string]bool)
	for _, tableDiff := range diff.TablesModified {
		table, declared := tables[tableDiff.TableName]
		if !declared {
			continue
		}
		for _, column := range tableDiff.ColumnsModified {
			if slices.ContainsFunc([]string{"type", "default", "default_expr"}, func(change string) bool {
				_, found := column.Changes[change]
				return found
			}) {
				keys[exprkey.Column(dialect, table.Schema, table.Name, column.ColumnName)] = true
			}
		}
	}
	return keys
}

// qualifiedColumnName joins the parts of a column's name that are not empty.
func qualifiedColumnName(parts ...string) string {
	return strings.Join(slices.DeleteFunc(parts, func(part string) bool { return part == "" }), ".")
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
	probe := dbexprprobe.ColumnSpellingProbe{
		Table:    fmt.Sprintf("pg_temp.ptah_column_probe_%d", index),
		Schema:   cmp.Or(table.Schema, info.Schema),
		Declared: table.Name,
	}
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
	probe.Statement = result.Fragments[0].SQL
	for position := range probe.Columns {
		probe.Columns[position].Statement = result.Fragments[position+1].SQL
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

// resolveCheckExpressions normalizes the expression of every CHECK the
// comparison compares -- declared on the table, or synthesized from a column's
// check or a table's checks list -- that the database also holds.
//
// Only those: a constraint being created carries its declaration into the ADD
// statement unchanged, and one being dropped has no declaration left to
// normalize. The ones in the middle are the ones a string comparison cannot
// decide (stokaro/ptah#1717).
//
// The probe table is built from the LIVE table's columns rather than the
// declared ones, because the rewrite depends on the types the expression is
// parsed against: `price >= 0` normalizes to `(0)::numeric` over numeric and
// to `(0)` over integer, and it is the live table the constraint is compared
// against.
func resolveCheckExpressions(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) (map[string]config.CheckExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]string, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "CHECK") {
			key := exprkey.CheckParts(semantics, constraint.Schema, constraint.TableName, constraint.Name)
			held[key] = ""
			if constraint.CheckClause != nil {
				held[key] = strings.TrimSpace(*constraint.CheckClause)
			}
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
			Schema:     cmp.Or(live.schema, conn.Info().Schema),
			Columns:    live.columns,
			Expression: constraint.CheckExpression,
			Object:     fmt.Sprintf("CHECK %s on %s", constraint.Name, constraint.Table),
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}

	checks, err := dbexprprobe.ResolveCheckExpressions(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if err := reconcileCheckSpellings(ctx, conn, probes, held, checks); err != nil {
		return nil, err
	}
	return checks, nil
}

// The second pass asks about two expressions per constraint, under the
// constraint's key and one of these. A key is never sent to the server.
const (
	checkDeclaredSuffix = "\x00declared"
	checkStoredSuffix   = "\x00stored"
)

// reconcileCheckSpellings asks the server a second time about each resolved
// CHECK that still differs from the one the database holds, and records the
// held spelling where the two are one expression.
//
// One pass through the server is not always enough, because PostgreSQL does not
// print a CHECK in a form that reads back to itself. Measured on 15.19:
//
//	declared  kind IN ('a', 'b')
//	stored    ((kind)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))
//	that text written back is stored as
//	          ((kind)::text = ANY (ARRAY[('a'::character varying)::text, ('b'::character varying)::text]))
//
// A history whose migrations were written from an inspected database holds the
// last form, and the declaration resolves to the middle one, so the check was
// dropped and added on every run. Put through the server once more, both
// spellings print the same text; a check whose values changed still does not.
func reconcileCheckSpellings(
	ctx context.Context,
	conn Connection,
	probes []dbexprprobe.CheckExpressionProbe,
	held map[string]string,
	checks map[string]config.CheckExpression,
) error {
	var again []dbexprprobe.CheckExpressionProbe
	for _, probe := range probes {
		resolved, live := checks[probe.Key], held[probe.Key]
		if !resolved.Resolved || live == "" || resolved.Expression == live {
			continue
		}
		declared, stored := probe, probe
		declared.Key, declared.Expression = probe.Key+checkDeclaredSuffix, resolved.Expression
		stored.Key, stored.Expression = probe.Key+checkStoredSuffix, live
		again = append(again, declared, stored)
	}
	if len(again) == 0 {
		return nil
	}
	answers, err := dbexprprobe.ResolveCheckExpressions(ctx, conn, again)
	if err != nil {
		return fmt.Errorf("compare schemas: %w", err)
	}
	for i := 0; i < len(again); i += 2 {
		key := strings.TrimSuffix(again[i].Key, checkDeclaredSuffix)
		declared, stored := answers[again[i].Key], answers[again[i+1].Key]
		if declared.Resolved && stored.Resolved && declared.Expression == stored.Expression {
			checks[key] = config.CheckExpression{Expression: held[key], Resolved: true}
		}
	}
	return nil
}

// heldGeneratedExpressions is the generated expressions the caller's options
// already answer, which the comparison does not ask the target about again.
func heldGeneratedExpressions(opts *config.CompareOptions) map[string]config.GeneratedExpression {
	if opts == nil {
		return nil
	}
	return opts.GeneratedExpressions
}

// GeneratedExpressionRefusal is one declared generated column that a server
// refused to spell, as [ResolveGeneratedExpressions] reports it.
type GeneratedExpressionRefusal struct {
	// Key is the column's key in the map ResolveGeneratedExpressions returns.
	Key string
	// Schema, Table and Column name the column. Schema is the connection's
	// schema where the table names none.
	Schema, Table, Column string
	// Reason is the server's error for the statement it refused, such as a
	// role that may not create a temporary table.
	Reason error
}

// ResolveGeneratedExpressions asks conn how it stores the expression of every
// declared generated column that database also holds as a generated column.
// The answers are keyed by the strings [config.CompareOptions.GeneratedExpressions]
// is read with, one per column, the form the comparison builds from the
// dialect and the column's schema, table and name.
//
// It is for a caller that holds a dev database beside the target. Put in the
// options, the answers spell the declarations there, and
// [CompareWithDatabase] asks the target only about the columns they leave out
// or the dev server refused, so a target role without the TEMP privilege
// needs it only for those. The probe is a temporary table that carries the
// columns database reports, in a transaction that is rolled back, so conn
// needs those column types and nothing else.
//
// A nil desired or a nil database returns nil, nil, and a nil conn returns an
// error. Only a server whose descriptor names the pg-catalog probes is asked;
// on every other dialect the result is nil. A column the server refused to
// spell has an answer with Resolved false and one refusal, in probe order,
// that carries the server's reason.
func ResolveGeneratedExpressions(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
) (map[string]config.GeneratedExpression, []GeneratedExpressionRefusal, error) {
	if desired == nil || database == nil {
		return nil, nil, nil
	}
	if conn == nil {
		return nil, nil, fmt.Errorf("resolve generated expressions: database connection is nil")
	}
	info := conn.Info()
	semantics, err := conn.ResolveIdentifierSemantics(ctx, collectIdentifierNames(desired, database, info.Schema, info.Dialect))
	if err != nil {
		return nil, nil, fmt.Errorf("resolve generated expressions: %w", err)
	}
	probes := generatedColumnProbes(info, desired, database, semantics)
	if len(probes) == 0 {
		return nil, nil, nil
	}
	resolved, refused, err := dbexprprobe.ResolveGeneratedColumns(ctx, conn, probes)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve generated expressions: %w", err)
	}
	var refusals []GeneratedExpressionRefusal
	for _, refusal := range refused {
		refusals = append(refusals, GeneratedExpressionRefusal{
			Key: refusal.Key, Schema: refusal.Schema, Table: refusal.Table, Column: refusal.Column, Reason: refusal.Reason,
		})
	}
	return resolved, refusals, nil
}

// resolveGeneratedExpressions asks the server how it stores the expression of
// every declared generated column that the database holds as a generated
// column too, apart from those held already answers.
//
// Only those, for the reason [resolveCheckExpressions] gives: a column being
// added carries its declaration into the ALTER statement unchanged, and a
// column that stops or starts being generated is a change whatever the
// expression says.
//
// The probe table carries the LIVE table's other columns, for the reason
// [resolveCheckExpressions] gives: the stored form depends on the types the
// expression is parsed against. A plan that changes the type of a column the
// expression reads rebuilds the generated column as declared whatever this
// answers.
//
// A column the server refuses to spell is compared as written, and the run
// says so once, before the plan it informs: the refusal is usually a role
// without the TEMP privilege, and a comparison of the declaration against the
// stored form reports a change nobody made.
func resolveGeneratedExpressions(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
	held map[string]config.GeneratedExpression,
) (map[string]config.GeneratedExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	probes := slices.DeleteFunc(generatedColumnProbes(conn.Info(), desired, database, semantics),
		func(probe dbexprprobe.GeneratedColumnProbe) bool {
			_, answered := held[probe.Key]
			return answered
		})
	if len(probes) == 0 {
		return nil, nil
	}
	resolved, refusals, err := dbexprprobe.ResolveGeneratedColumns(ctx, conn, probes)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	warnUnspelledGeneratedColumns(ctx, refusals)
	return resolved, nil
}

// warnUnspelledGeneratedColumns raises one warning for the generated columns
// the server refused to spell, and writes it now rather than when the run
// ends, since the plan that follows depends on it.
func warnUnspelledGeneratedColumns(ctx context.Context, refusals []dbexprprobe.GeneratedColumnRefusal) {
	if len(refusals) == 0 {
		return
	}
	stream := rundiag.Stream(ctx)
	for _, refusal := range refusals {
		column := qualifiedColumnName(refusal.Schema, refusal.Table, refusal.Column)
		_ = rundiag.Warn(ctx, stream, rundiag.UnspelledGeneratedColumns, rundiag.Record{
			Key: refusal.Key,
			Line: fmt.Sprintf("generated column %s is compared as written, not in the form the server stores: "+
				"the server refused the temporary table that spells it: %v", column, refusal.Reason),
			Attrs: []slog.Attr{slog.String("column", column), slog.String("reason", refusal.Reason.Error())},
		})
	}
	rundiag.Settle(ctx, rundiag.UnspelledGeneratedColumns)
}

// generatedColumnProbes builds a probe for every declared generated column
// that database holds as a generated column too, keyed for info's dialect.
func generatedColumnProbes(
	info catalog.ServerInfo,
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) []dbexprprobe.GeneratedColumnProbe {
	held := make(map[string]catalog.Table, len(database.Tables))
	for _, table := range database.Tables {
		held[exprkey.TableParts(semantics, table.Schema, table.Name)] = table
	}
	var probes []dbexprprobe.GeneratedColumnProbe
	for _, table := range desired.Tables {
		live, exists := held[exprkey.TableParts(semantics, table.Schema, table.Name)]
		if !exists {
			continue
		}
		for _, field := range generatedschema.FieldsForTable(desired, table) {
			probe, ok := generatedColumnProbe(live, field, semantics)
			if !ok {
				continue
			}
			probe.Key = exprkey.Generated(info.Dialect, table.Schema, table.Name, field.Name)
			probe.Schema = cmp.Or(live.Schema, info.Schema)
			probes = append(probes, probe)
		}
	}
	return probes
}

// generatedColumnProbe builds the probe for one declared generated column
// from the live table that holds it, or reports that the live column is
// absent or not generated.
func generatedColumnProbe(
	live catalog.Table,
	field schemamodel.Field,
	semantics identifier.Semantics,
) (dbexprprobe.GeneratedColumnProbe, bool) {
	if strings.TrimSpace(field.GeneratedExpression) == "" {
		return dbexprprobe.GeneratedColumnProbe{}, false
	}
	identity := semantics.ColumnIdentityKey(field.Name)
	probe := dbexprprobe.GeneratedColumnProbe{Table: live.Name, Expression: field.GeneratedExpression}
	for _, column := range live.Columns {
		if semantics.ColumnIdentityKey(column.Name) != identity {
			probe.Columns = append(probe.Columns, dbexprprobe.CheckProbeColumn{Name: column.Name, Type: column.RawType()})
			continue
		}
		if strings.TrimSpace(column.GeneratedKind) == "" {
			return dbexprprobe.GeneratedColumnProbe{}, false
		}
		probe.Column, probe.Type = column.Name, column.RawType()
	}
	return probe, probe.Column != ""
}

// resolveIndexExpressions normalizes the declared expression and predicate of
// every index the database also holds.
//
// An index over plain columns with no predicate is skipped: there is nothing
// for the server to rewrite, and probing one would cost a statement to learn
// what the declaration already says.
func resolveIndexExpressions(
	ctx context.Context,
	conn Connection,
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
			Schema:     cmp.Or(live.schema, conn.Info().Schema),
			Columns:    live.columns,
			Expression: expression,
			Parts:      parts,
			Predicate:  index.Condition,
			Object:     fmt.Sprintf("index %s on %s", index.Name, owners[position]),
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
	name string
	// schema is the schema the read recorded, empty for the connection's own;
	// see [catalog.ServerInfo.Schema].
	schema  string
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
			schema:  table.Schema,
			columns: probeColumns,
		}
	}
	return tables
}

// normalizeFeatureObjects asks the owners of declared feature objects to
// attach the connected server's own spelling of what they declare, the way the
// resolvers above do for the common families. The desired schema is copied,
// never changed in place; with no declared object it is returned as it is.
//
// An owner probes only the declarations the database holds, and finds them by
// identity, so both sides are bound to the connection first, as the comparison
// binds them. A reader and a source fill a schema they did not write with the
// offline default: under `search_path=reporting,public` a policy read on
// `reporting.audit` arrives as `public.audit`, and without the binding it
// never meets the declaration on `reporting.audit`, so the owner compares the
// declared text with the server's and plans a change on every run. The bound
// states are a working copy, as in [normalizeFeatureFacets]: each answer goes
// back under the identity the caller gave it.
func normalizeFeatureObjects(
	ctx context.Context,
	conn Connection,
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	runtime DatabaseRuntime,
) (*schemamodel.Database, error) {
	if desired.FeatureObjects.Len() == 0 || info.Dialect == "" {
		return desired, nil
	}
	selected, err := runtime.ResolveTarget(info.Dialect)
	if err != nil {
		return nil, err
	}
	identity, err := comparisonTableIdentities(desired, database, &config.CompareOptions{Dialect: info.Dialect, IdentifierSemantics: &info.IdentifierSemantics}, selected)
	if err != nil {
		return nil, err
	}
	result, err := runtime.NormalizeObjects(ctx, schemaext.NormalizationRequest{
		Target: info.Dialect, Identifiers: identity.semantics, Capabilities: info.Capabilities,
		Desired: schemaext.ObjectState{Objects: identity.desired.FeatureObjects, Coverage: identity.desired.FeatureCoverage},
		Current: schemaext.ObjectState{Objects: identity.current.FeatureObjects, Coverage: identity.current.FeatureCoverage},
		Session: conn,
	})
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if !result.Complete {
		return nil, fmt.Errorf("%w: feature normalization did not complete", schemaext.ErrInvalidValue)
	}
	objects, err := answersUnderDeclaredIdentities(desired.FeatureObjects, result.Desired.Objects, info.Dialect, identity.semantics)
	if err != nil {
		return nil, err
	}
	normalized := *desired
	normalized.FeatureObjects = objects
	return &normalized, nil
}

// answersUnderDeclaredIdentities returns each declared object under its own
// identity, with the value the owners answered for its bound identity.
// Binding refuses two objects that become one identity, so each answer has
// exactly one declaration.
func answersUnderDeclaredIdentities(declared, answered schemaext.Objects, target string, semantics identifier.Semantics) (schemaext.Objects, error) {
	objects, err := declared.All()
	if err != nil {
		return schemaext.Objects{}, err
	}
	for i, object := range objects {
		answer, found, err := answered.Get(tableidentity.BindRef(object.Ref, target, semantics))
		if err != nil {
			return schemaext.Objects{}, err
		}
		if !found {
			return schemaext.Objects{}, fmt.Errorf("%w: feature normalization lost %s", schemaext.ErrInvalidValue, object.Ref)
		}
		objects[i].Value = answer.Value
	}
	return schemaext.NewObjects(objects...)
}

func collectIdentifierNames(
	desired *schemamodel.Database,
	database *catalog.Database,
	defaultSchema, dialect string,
) []string {
	names := []string{defaultSchema}
	names = appendGeneratedIdentifierNames(names, desired)
	names = appendDatabaseIdentifierNames(names, database)
	names = appendDeclaredObjectNames(names, desired, dialect)
	names = appendHeldObjectNames(names, database)
	names = append(names, identifiervalidation.HeldDerivedConstraintNames(database, dialect)...)
	names = appendRoleNames(names, desired, database, dialect)
	return appendSequenceReferenceNames(names, desired, database, dialect)
}

// appendRoleNames adds every role name desired and database hold -- roles,
// the roles grants name and the roles of a membership -- on a dialect that
// compares a role name under its catalog's rules
// ([offlinenames.RolesByCatalog]).
//
// A SQL Server role name is compared under the database collation, as a
// table name is: measured on SQL Server 2025, CREATE ROLE [APP] beside the
// role App fails with error 15023 under a case-insensitive collation and
// succeeds under a case-sensitive one, and under Hungarian_CI_AS the roles
// ccs and cscs are one role. Without these names the comparison keys a
// declared APP and a held App apart, and the plan creates a role the
// database already holds, which fails with error 15023 (stokaro/ptah#6741).
// Another dialect states how it compares a role name, and asking its server
// about one would only spend the probe its index names need.
func appendRoleNames(names []string, desired *schemamodel.Database, database *catalog.Database, dialect string) []string {
	if !offlinenames.RolesByCatalog(dialect) {
		return names
	}
	named := func(values ...string) {
		for _, value := range values {
			if value != "" {
				names = append(names, value)
			}
		}
	}
	if desired != nil {
		for _, role := range desired.Roles {
			named(role.Name)
			named(role.MemberOf...)
		}
		for _, grant := range slices.Concat(desired.Grants, desired.RevokedGrants, desired.DeniedGrants) {
			named(grant.Role)
		}
	}
	if database != nil {
		for _, role := range slices.Concat(database.Roles, database.RolesOutOfScope) {
			named(role.Name)
		}
		for _, grant := range database.Grants {
			named(grant.Role)
		}
		for _, membership := range database.RoleMemberships {
			named(membership.Role, membership.Member)
		}
	}
	return names
}

// appendDeclaredObjectNames adds the names of the views, materialized views,
// routines, sequences and triggers desired declares, as
// [identifiervalidation.ObjectNames] lists them.
//
// Each of them is keyed by the table rule, as a table is (see
// [ptah.run/core/objectidentity.Builder.SchemaScopedParts]), so on a server
// that compares names under its collation each one needs the server's answer
// as a table name does. A name nobody asked about keeps its spelling. Then a
// declared procedure MIXEDCASE and a held procedure MixedCase are two objects:
// the plan creates the one and drops the other, and on a case-insensitive SQL
// Server database the drop removes the procedure the create has just changed
// (stokaro/ptah#6722).
func appendDeclaredObjectNames(names []string, desired *schemamodel.Database, dialect string) []string {
	return append(names, identifiervalidation.ObjectNames(desired, dialect)...)
}

// appendHeldObjectNames is [appendDeclaredObjectNames] for the objects the
// database holds. A reader blanks the schema of an object in the
// connection's own schema, and a blank is no name to ask about.
func appendHeldObjectNames(names []string, database *catalog.Database) []string {
	if database == nil {
		return names
	}
	named := func(values ...string) {
		for _, value := range values {
			if value != "" {
				names = append(names, value)
			}
		}
	}
	for _, view := range database.Views {
		named(view.Schema, view.Name)
	}
	for _, view := range database.MatViews {
		named(view.Schema, view.Name)
	}
	for _, function := range database.Functions {
		named(function.Schema, function.Name)
	}
	for _, sequence := range database.Sequences {
		named(sequence.Schema, sequence.Name)
	}
	for _, trigger := range database.Triggers {
		named(trigger.Schema, trigger.Table, trigger.Name)
	}
	return names
}

// appendSequenceReferenceNames adds the schema and sequence names the column
// defaults on both sides read a sequence through. A default compares a
// reference by the class the server puts each name in, and a name nobody
// asked about keeps its spelling, so a declared NEXT VALUE FOR S and a stored
// NEXT VALUE FOR [s] differ on a server that reads them as one sequence.
func appendSequenceReferenceNames(
	names []string,
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
) []string {
	if desired != nil {
		for _, field := range desired.Fields {
			names = append(names, compare.SequenceReferenceNames(field.DefaultExpr, dialect)...)
		}
	}
	if database != nil {
		for _, table := range database.Tables {
			for _, column := range table.Columns {
				if column.ColumnDefault != nil {
					names = append(names, compare.SequenceReferenceNames(*column.ColumnDefault, dialect)...)
				}
			}
		}
	}
	return names
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
	for _, schema := range desired.Schemas {
		names = append(names, schema.Name)
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
	for _, constraint := range desired.Constraints {
		names = append(names, constraint.Name)
	}
	// A grant names its table and columns as the grant declaration spells
	// them, which need not be how the table and its fields are declared. Left
	// out, such a name has no answer from the server and keeps its spelling, so
	// a column grant on NAME never matches the name column the catalog reports
	// and every plan grants and revokes it again.
	for _, grant := range desired.Grants {
		if grant.OnTable != "" {
			names = appendQualifiedIdentifier(names, grant.OnTable)
		}
		names = append(names, grant.Columns...)
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
	for _, schema := range database.Schemas {
		names = append(names, schema.Name)
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
	// A constraint's name compares under the index rule. Left out, it has no answer from
	// the server, and an unanswered name may collide with any other: adding a constraint
	// to a table that holds one read as a conflict.
	for _, constraint := range database.Constraints {
		names = append(names, constraint.Name)
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

// appendFacetIdentifierNames includes owner-declared names in the same catalog
// resolution as common identifiers. Target exclusions cannot contribute names.
func appendFacetIdentifierNames(names []string, slots []*schemaext.Facets, target schemaext.TargetSelection) ([]string, error) {
	for _, slot := range slots {
		facets, err := slot.ForTarget(target)
		if err != nil {
			return nil, err
		}
		values, err := facets.Values()
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			if source, ok := value.(schemaext.IdentifierValue); ok {
				names = append(names, source.IdentifierNames()...)
			}
		}
	}
	return names, nil
}
