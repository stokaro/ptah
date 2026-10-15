package builtin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/routineext"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mssql/mssqllint"
	"ptah.run/dialect/mssql/mssqlroutine"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqlroutine"
	"ptah.run/dialect/postgres/pglint"
	"ptah.run/dialect/postgres/pgproject"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydblint"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/lintscan"
	"ptah.run/internal/mssqlroutineprep"
	mysqlrules "ptah.run/internal/mysqlroutine"
	"ptah.run/internal/oracledata"
	"ptah.run/internal/oracleroutine"
	"ptah.run/internal/pgdefaultprivilegeprovider"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/ydbcomparison"
	"ptah.run/internal/ydbdata"
	"ptah.run/internal/ydbextensions"
	"ptah.run/internal/ydblowering"
	"ptah.run/internal/ydbsource"
	"ptah.run/internal/ydbsql"
	"ptah.run/internal/yugabytecomparison"
)

// bundled is the runtime the package-level helpers (RenderSQL,
// ValidateSchemaWithCapabilities and the omission reports) render with. A
// runtime is frozen when New returns and renders concurrently, so the helpers
// share one rather than assembling every provider and codec on each call.
var bundled = sync.OnceValues(New)

// New assembles a runtime with the bundled providers and their codecs. It returns a
// fresh registry on every call. The provider services create their own visitor
// for each batch, so the runtime can render concurrently without sharing output
// buffers.
//
// It installs process-global handlers, once per process: the migration
// planner of every bundled dialect, in migration/planner's registry, which
// links no dialect. Until New runs, planner.GetPlanner refuses a bundled
// dialect as unregistered.
func New() (*engine.Runtime, error) {
	if err := registerPlanners(); err != nil {
		return nil, fmt.Errorf("register the bundled migration planners: %w", err)
	}
	// The built-in descriptors, not the process registry: a dialect another
	// program registered has no bundled provider.
	described := builtindialects.Descriptors()
	providers := make([]engine.Provider, 0, len(described))
	for _, dialect := range described {
		name := dialect.Name
		provider := engine.Provider{
			ID: "ptah.run/" + name,
			Targets: []engine.Target{{
				Name:        name,
				Aliases:     dialect.Aliases,
				Rendering:   renderingService{},
				Preparation: schemapreparation.Identity{},
				Creations:   schemaprojection.IdentityCreations{},
				// Every built-in dialect splits its migrations with the
				// lint scanner in the dialect's mode; the SQL Server and YDB
				// owners replace it below.
				Splitter: lintscan.Splitter{Dialect: name},
				// A routine body is the engine's own language, so its owner
				// reads it, and a routine that declares no language is in it.
				RoutineBodies: routineBodyReader(name),
				Comparison:    schemacomparison.Rules{Routines: routineDefaults(name), Indexes: indexRules(name)},
			}},
		}
		if name == platform.SQLServer {
			// The SQL Server owner judges PolyBase's drops, which a safety
			// assessment reads in place of their words.
			provider.Targets[0].Splitter = mssqllint.Splitter{}
			provider.Targets[0].Lowering = mssqlroutineprep.Service{}
			registerSQLServerRoutineServices(&provider)
		}
		if name == platform.YDB {
			// YDB answers nothing it cannot hold with a skip comment, since
			// a comment lets an apply exit 0 without the object; see the
			// YDB renderer's dispatch table. An object an owner would skip
			// is refused here as well.
			provider.Targets[0].RefusesSkips = true
		}
		if name == platform.Postgres {
			provider.Targets[0].Constraints = pgproject.Constraints{}
			provider.Lint = []engine.Lint{{Rules: pglint.Rules(), Service: pglint.Service{}}}
		}
		if name == platform.CockroachDB {
			registerCockroachDBServices(&provider, name)
		}
		if name == platform.Spanner {
			registerSpannerServices(&provider, name)
		}
		if name == platform.ClickHouse {
			registerClickHouseServices(&provider, name)
		}
		if name == platform.MariaDB {
			// MariaDB's tables are created by the MySQL owner's rules too; the
			// rest of that owner registers its MariaDB services on the mysql
			// provider.
			provider.Targets[0].Creations = mysqlconvert.CreationService{}
		}
		if name == platform.MySQL {
			registerMySQLServices(&provider)
		}
		if name == platform.Oracle {
			// The Oracle type map decides which declared columns hold a
			// moment, and Oracle refuses text for those.
			provider.Targets[0].Data = oracledata.Services()
		}
		if name == platform.SQLite {
			registerSQLiteServices(&provider)
		}
		if name == platform.YDB {
			// The YDB owner splits its migrations as YQL and reports the
			// facts the shared lint rules read, and judges one of them over
			// the split of a run that names no dialect.
			provider.Targets[0].Splitter = ydblint.Splitter{}
			// YDB writes every literal in its column's type, compares rows
			// typed the same way, and runs a seed file as YQL queries in one
			// transaction with its record.
			provider.Targets[0].Data = ydbdata.Services()
			provider.Targets[0].Annotator = ydblint.Annotator{}
			provider.Lint = []engine.Lint{{Rules: ydblint.Rules(), Service: ydblint.Service{}}}
			// A YDB read reports a UNIQUE constraint as an index and a
			// privilege as a permission name; the comparison lowers the
			// desired schema to match.
			provider.Targets[0].Lowering = ydblowering.Service{}
			// The shared comparison reads a YDB index's kind and settings,
			// and a view's query, through YDB's own rules.
			provider.Targets[0].Comparison = ydbcomparison.Rules()
			provider.Codecs = ydbextensions.Codecs()
			provider.Conversions = []engine.Conversion{{Target: name, Kinds: []schemaext.Kind{ydbschema.ChangefeedKind}, Service: ydbconvert.Service{}}}
			provider.Comparisons = []engine.ObjectComparison{{Target: name, Kinds: []schemaext.Kind{ydbschema.ChangefeedKind}, ChangeKinds: []schemaext.Kind{ydbdiff.ChangefeedKind}, Service: ydbcompare.Service{}}}
			provider.Reversals = []engine.Reversal{{Target: name, Kinds: []schemaext.Kind{ydbdiff.ChangefeedKind}, Service: ydbreverse.Service{}}}
			provider.Conversions = append(provider.Conversions, engine.Conversion{Target: name, Kinds: []schemaext.Kind{ydbcoordination.Kind}, Service: ydbconvert.CoordinationService{}})
			provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: name, Kinds: []schemaext.Kind{ydbcoordination.Kind},
				ChangeKinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind}, Service: ydbcompare.CoordinationService{}})
			provider.Reversals = append(provider.Reversals, engine.Reversal{Target: name, Kinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind}, Service: ydbreverse.CoordinationService{}})
			provider.Planning = []engine.Planning{{
				Target: name, Kinds: []schemaext.Kind{ydbdiff.ChangefeedKind},
				ParentKinds:    []schemaext.Kind{ydbschema.ChangefeedKind},
				OperationKinds: []schemaext.Kind{(&ydbast.AddChangefeed{}).Kind(), (&ydbast.DropChangefeed{}).Kind(), (&ydbast.AlterChangefeedTopic{}).Kind()},
				Service:        ydbplan.Service{},
			}}
			provider.Planning = append(provider.Planning, engine.Planning{Target: name, Kinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind},
				OperationKinds: []schemaext.Kind{ydbast.CoordinationNodeKind}, Service: ydbplan.CoordinationService{}})
			provider.Declarations = []engine.DeclarationPlanning{{Target: name, Kinds: []schemaext.Kind{ydbcoordination.Kind},
				OperationKinds: []schemaext.Kind{ydbast.CoordinationNodeKind}, Service: ydbplan.CoordinationService{}}}
			provider.Conversions = append(provider.Conversions, engine.Conversion{Target: name, Kinds: []schemaext.Kind{ydbstreaming.Kind}, Service: ydbconvert.StreamingService{}})
			provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: name, Kinds: []schemaext.Kind{ydbstreaming.Kind}, ChangeKinds: []schemaext.Kind{ydbdiff.StreamingQueryKind}, Service: ydbcompare.StreamingService{}})
			provider.Reversals = append(provider.Reversals, engine.Reversal{Target: name, Kinds: []schemaext.Kind{ydbdiff.StreamingQueryKind}, Service: ydbreverse.StreamingService{}})
			registerWorkloadServices(&provider, name)
			registerSecretServices(&provider, name)
			registerTopicServices(&provider, name)
			registerExternalServices(&provider, name)
			registerReplicationServices(&provider, name)
			registerYDBTTLServices(&provider, name)
			registerYDBColumnFamilyServices(&provider, name)
			registerYDBTablePartitioningServices(&provider, name)
			registerYDBColumnStoreServices(&provider, name)
			registerYDBVectorIndexServices(&provider, name)
			registerYDBIndexPartitioningServices(&provider, name)
			provider.Annotations = append(provider.Annotations, ydbsource.Annotations())
			provider.YAML = append(provider.YAML, ydbsource.YAML())
			provider.HCL = append(provider.HCL, ydbsource.HCL())
			provider.Selections = append(provider.Selections, ydbsource.Selections())
			provider.SQL = append(provider.SQL, ydbsql.SQL())
			// The YDB state a read records and does not describe: what a
			// document's header may name, and what the owner consults in
			// process and no document names.
			provider.CoverageKinds = append(provider.CoverageKinds, ydbschema.CoverageKinds()...)
			provider.ProcessCoverageKinds = append(provider.ProcessCoverageKinds, ydbschema.ProcessCoverageKinds()...)
			for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: ydbreport.Definitions(), Service: ydbreport.Service{}})
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: ydbreport.CoordinationDefinitions(), Service: ydbreport.CoordinationService{}})
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: ydbreport.StreamingDefinitions(), Service: ydbreport.StreamingService{}})
			}
		}
		providers = append(providers, provider)
	}
	providers = append(providers,
		pgdefaultprivilegeprovider.Provider(), timescaleProvider(), postgresRoutineProvider(), postgresTableProvider(),
		pgpolicyProvider(), pgextensionProvider(), pgtypeProvider(), mssqlProvider(), mssqlPropertyProvider(), synonymProvider(),
		oratypeProvider(),
	)
	return assembleSchemaServices(providers)
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
func assembleSchemaServices(providers []engine.Provider) (*engine.Runtime, error) {
	declarations := make([]engine.Provider, len(providers))
	for i, provider := range providers {
		declarations[i] = engine.Provider{ID: provider.ID, Codecs: provider.Codecs, Declarations: provider.Declarations}
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
	return engine.New(providers...)
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

// indexRules returns the index rules a target registers before its owner's
// services run, or nil. The shared comparison reads a YugabyteDB index's whole
// definition through YugabyteDB's own rules; the owners of other targets
// register theirs with their services.
func indexRules(name string) schemacomparison.Indexes {
	if name == platform.YugabyteDB {
		return yugabytecomparison.Rules().Indexes
	}
	return nil
}
