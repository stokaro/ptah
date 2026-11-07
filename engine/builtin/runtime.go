package builtin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/routineext"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mssql/mssqllint"
	"ptah.run/dialect/mssql/mssqlroutine"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqlroutine"
	"ptah.run/dialect/postgres/pglint"
	"ptah.run/dialect/postgres/pgproject"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/clickhousecomparison"
	"ptah.run/internal/lintscan"
	"ptah.run/internal/mssqlroutineprep"
	"ptah.run/internal/mysqlcomparison"
	mysqlrules "ptah.run/internal/mysqlroutine"
	"ptah.run/internal/oracledata"
	"ptah.run/internal/oracleroutine"
	"ptah.run/internal/pgdefaultprivilegeprovider"
	"ptah.run/internal/postgrescomparison"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/sqlservercomparison"
	"ptah.run/internal/yugabytecomparison"
)

// bundled is the runtime the package-level helpers (RenderSQL,
// ValidateSchemaWithCapabilities and the omission reports) render with. A
// runtime is frozen when New returns and renders concurrently, so the helpers
// share one rather than assembling every provider and codec on each call.
var bundled = sync.OnceValues(func() (*engine.Runtime, error) { return New() })

// New assembles the bundled providers and explicit additional providers into a
// fresh runtime. Additional targets retain their own services; missing services
// remain unavailable. Conflicting provider IDs, targets, and aliases are refused.
// The bundled services create a visitor for each batch and share no output state.
func New(additional ...engine.Provider) (*engine.Runtime, error) {
	// The built-in descriptors, not the process registry: a dialect another
	// program registered has no bundled provider.
	described := builtindialects.Descriptors()
	providers := make([]engine.Provider, 0, len(described))
	for _, dialect := range described {
		name := dialect.Name
		factory, found := plannerFactory(dialect)
		if !found {
			return nil, fmt.Errorf("no migration planner for the bundled dialect %q", name)
		}
		provider := engine.Provider{
			ID: "ptah.run/" + name,
			Targets: []engine.Target{{
				Name:        name,
				Planner:     factory,
				Aliases:     dialect.Aliases,
				Rendering:   renderingService{},
				Preparation: schemapreparation.Identity{},
				Creations:   schemaprojection.IdentityCreations{},
				// Every built-in dialect splits its migrations with the
				// lint scanner in the dialect's mode; the SQL Server owner
				// replaces it below.
				Splitter: lintscan.Splitter{Dialect: name},
				// A routine body is the engine's own language, so its owner
				// reads it, and a routine that declares no language is in it.
				RoutineBodies: routineBodyReader(name),
				Comparison:    comparisonRules(name),
			}},
		}
		if name == platform.SQLServer {
			// The SQL Server owner judges PolyBase's drops, which a safety
			// assessment reads in place of their words.
			provider.Targets[0].Splitter = mssqllint.Splitter{}
			provider.Targets[0].Lowering = mssqlroutineprep.Service{}
			registerSQLServerRoutineServices(&provider)
		}
		if name == platform.Postgres {
			provider.Targets[0].Constraints = pgproject.Constraints{}
			provider.Lint = []engine.Lint{{Rules: pglint.Rules(), Service: pglint.Service{}}}
		}
		if name == platform.CockroachDB {
			registerCockroachDBServices(&provider, name)
		}
		if name == platform.ClickHouse {
			registerClickHouseServices(&provider, name)
		}
		if name == platform.MariaDB {
			// MariaDB's tables are created, and its indexes read back, by the
			// MySQL owner's rules too; the rest of that owner registers its
			// MariaDB services on the mysql provider.
			provider.Targets[0].Creations = mysqlconvert.CreationService{}
			provider.Targets[0].ReadBack = mysqlconvert.ReadBack{}
		}
		if name == platform.MySQL {
			registerMySQLServices(&provider)
		}
		if name == platform.Oracle {
			// The Oracle type map decides which declared columns hold a
			// moment, and Oracle refuses text for those; it is also the type a
			// read reports for a declared column.
			provider.Targets[0].Data = oracledata.Services()
			provider.Targets[0].ReadBack = oracledata.ReadBack{}
		}
		if name == platform.SQLite {
			registerSQLiteServices(&provider)
		}
		providers = append(providers, provider)
	}
	providers = append(providers,
		pgdefaultprivilegeprovider.Provider(), timescaleProvider(), postgresRoutineProvider(), postgresTableProvider(),
		pgpolicyProvider(), pgextensionProvider(), pgtypeProvider(), mssqlProvider(), mssqlPropertyProvider(), synonymProvider(),
		oratypeProvider(),
	)
	return assembleSchemaServices(providers, additional)
}

// routineBodyReader is the owner that reads a target's routine bodies, or nil
// for a target whose owner reads none. A routine body is written in the
// engine's own language: PL/pgSQL or SQL on the PostgreSQL family, a compound
// statement on MySQL and MariaDB, T-SQL on SQL Server.
func routineBodyReader(name string) routineext.Reader {
	switch {
	case platform.IsPostgresFamily(name):
		return pgroutine.Reader{}
	case name == platform.MySQL, name == platform.MariaDB:
		return mysqlroutine.Reader{}
	case name == platform.SQLServer:
		return mssqlroutine.Reader{}
	default:
		return nil
	}
}

// routineDefaults are the attributes the target's engine applies to a routine
// whose declaration leaves them out, as its reader reports them, or nil for a
// target whose reader reports no routines.
func routineDefaults(name string) schemacomparison.Routines {
	switch {
	case platform.IsPostgresFamily(name):
		return schemacomparison.RoutineDefaults{Language: pgroutine.Language, Security: pgroutine.Security}
	case name == platform.MySQL, name == platform.MariaDB:
		return schemacomparison.RoutineDefaults{Language: mysqlrules.Language, Security: mysqlrules.Security}
	case name == platform.SQLServer:
		return schemacomparison.RoutineDefaults{Language: mssqlroutine.Language, Security: mssqlroutine.Security}
	case name == platform.Oracle:
		return schemacomparison.RoutineDefaults{Language: oracleroutine.Language, Security: oracleroutine.Security}
	default:
		return nil
	}
}

// Whole-schema services depend on a frozen declaration dispatcher. Derive it
// from the same descriptors before constructing the outer runtime, so neither
// runtime needs a mutable self-reference or a second list of feature owners.
func assembleSchemaServices(providers, additional []engine.Provider) (*engine.Runtime, error) {
	selected := slices.Concat(providers, additional)
	declarations := make([]engine.Provider, len(selected))
	for i, provider := range selected {
		declarations[i] = engine.Provider{ID: provider.ID, Codecs: provider.Codecs, ModelDecoder: provider.ModelDecoder, Declarations: provider.Declarations}
		for _, target := range provider.Targets {
			declarations[i].Targets = append(declarations[i].Targets, engine.Target{Name: target.Name, Aliases: target.Aliases})
		}
	}
	runtime, err := engine.New(declarations...)
	if err != nil {
		return nil, err
	}
	for i := range providers {
		for j := range providers[i].Targets {
			providers[i].Targets[j].SchemaRendering = schemaRenderingService{declarations: runtime}
			providers[i].Targets[j].Validation = validationService{declarations: runtime}
		}
	}
	return engine.New(selected...)
}

type renderingService struct{}

func (renderingService) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	if err := ctx.Err(); err != nil {
		return renderer.Result{}, err
	}
	visitor, err := NewRendererWithCapabilities(request.Target, request.Capabilities)
	if err != nil {
		return renderer.Result{}, err
	}
	sink := &renderdiag.Sink{}
	if reporter, ok := visitor.(omissionReporter); ok {
		reporter.ReportOmissionsTo(sink)
	}
	result, err := renderNodes(ctx, visitor, request.Nodes...)
	if err != nil {
		if ctx.Err() != nil {
			return renderer.Result{}, ctx.Err()
		}
		if refused, ok := errors.AsType[*renderFailure](err); ok &&
			(errors.Is(err, ptaherr.ErrInvalidSchemaDiff) || errors.Is(err, ptaherr.ErrUnsupportedFeature)) {
			return renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{
				Problem: schemaDiagnostic(refused.cause), Input: new(refused.input),
			}}}, nil
		}
		return renderer.Result{}, err
	}
	result.Omissions = publicOmissions(request.Target, sink)
	return result, nil
}

// renderFailure identifies a declaration the local node pipeline refused. The
// service converts it to diagnostic data; setup and receipt errors stay errors.
type renderFailure struct {
	input int
	cause error
}

func (e *renderFailure) Error() string { return e.cause.Error() }
func (e *renderFailure) Unwrap() error { return e.cause }

// nodeRefusal gives unclassified local declaration errors the same schema
// sentinel on visitor and batch entry points. Messages and typed causes remain.
func nodeRefusal(target string, node ast.Node, err error) error {
	if err == nil || errors.Is(err, ptaherr.ErrUnsupportedFeature) || errors.Is(err, ptaherr.ErrInvalidSchemaDiff) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, renderer.ErrInvalidResult) {
		return err
	}
	return &ptaherr.RenderError{Dialect: target, Node: node, Err: errors.Join(ptaherr.ErrInvalidSchemaDiff, err), Message: err.Error()}
}

// renderTarget preserves an unknown spelling in the diagnostic while resolving
// all built-in transport aliases at the composition boundary.
func renderTarget(dialect string) string {
	if normalized := platform.NormalizeDialect(dialect); normalized != "" {
		return normalized
	}
	return dialect
}

// comparisonRules returns the rules a target gives the shared comparison
// before its owner's services run, which may register more: the comparison
// rules of each engine whose owner registers none with its services, and the
// routine defaults the engine applies. The shared comparison reads an index
// definition, a partial index's predicate, a generated column's expression
// and a foreign key's backing index through these rules alone.
func comparisonRules(name string) schemacomparison.Rules {
	var rules schemacomparison.Rules
	switch name {
	case platform.Postgres:
		rules = postgrescomparison.Rules()
	case platform.MySQL, platform.MariaDB:
		rules = mysqlcomparison.Rules(name)
	case platform.SQLServer:
		rules = sqlservercomparison.Rules()
	case platform.ClickHouse:
		rules = clickhousecomparison.Rules()
	case platform.YugabyteDB:
		rules = yugabytecomparison.Rules()
	}
	rules.Routines = routineDefaults(name)
	return rules
}
