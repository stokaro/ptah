package builtin

import (
	"context"
	"errors"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/dialect/postgres/pgproject"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/ydbextensions"
)

// New assembles a runtime with the bundled providers and their codecs. It returns a
// fresh registry on every call and installs no process-global handlers. The
// provider services create their own visitor for each batch, so the runtime
// can render concurrently without sharing output buffers.
func New() (*engine.Runtime, error) {
	aliases := make(map[string][]string)
	var names []string
	for _, spelling := range platform.DialectSpellings() {
		name := platform.NormalizeDialect(spelling)
		if _, exists := aliases[name]; !exists {
			names = append(names, name)
			aliases[name] = nil
		}
		if spelling != name {
			aliases[name] = append(aliases[name], spelling)
		}
	}
	providers := make([]engine.Provider, 0, len(names))
	for _, name := range names {
		provider := engine.Provider{
			ID: "ptah.run/" + name,
			Targets: []engine.Target{{
				Name:        name,
				Aliases:     aliases[name],
				Rendering:   renderingService{},
				Preparation: schemapreparation.Identity{},
				Creations:   schemaprojection.IdentityCreations{},
			}},
		}
		if name == platform.Postgres {
			provider.Targets[0].Constraints = pgproject.Constraints{}
		}
		if name == platform.ClickHouse {
			provider.Targets[0].Preparation = chprepare.Service{}
			provider.Targets[0].Creations = chprepare.Service{}
			provider.Codecs = append(append(chschema.Codecs(), chdiff.Codecs()...), chast.Codecs()...)
			provider.Reversals = []engine.Reversal{{Target: name, Kinds: []schemaext.Kind{chdiff.TableKind}, Service: chreverse.Service{}}}
			provider.Planning = []engine.Planning{{Target: name, Kinds: []schemaext.Kind{chdiff.TableKind}, ParentKinds: []schemaext.Kind{chschema.TableKind}, OperationKinds: []schemaext.Kind{chast.AlterTTLKind}, Service: chplan.Service{}}}
			provider.Properties = []engine.PropertySource{{Target: name, Format: schemaext.TablePlatformProperties, Definitions: chsource.Definitions(), Service: chsource.Service{}}}
			provider.Conversions = []engine.Conversion{{Target: name, Kinds: []schemaext.Kind{chschema.TableKind}, Service: chconvert.Service{}}}
			provider.FacetComparisons = []engine.FacetComparison{{
				Target: name, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
				Kinds: []schemaext.Kind{chschema.TableKind}, ChangeKinds: []schemaext.Kind{chdiff.TableKind}, Service: chcompare.Service{},
			}}
			for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: chreport.Definitions(), Service: chreport.Service{}})
			}
		}
		if name == platform.YDB {
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
			provider.Planning = append(provider.Planning, engine.Planning{Target: name, Kinds: []schemaext.Kind{ydbdiff.StreamingQueryKind}, OperationKinds: []schemaext.Kind{ydbast.StreamingQueryKind}, Service: ydbplan.StreamingService{}})
			provider.Declarations = append(provider.Declarations, engine.DeclarationPlanning{Target: name, Kinds: []schemaext.Kind{ydbstreaming.Kind}, OperationKinds: []schemaext.Kind{ydbast.StreamingQueryKind}, Service: ydbplan.StreamingService{}})
			for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: ydbreport.Definitions(), Service: ydbreport.Service{}})
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: ydbreport.CoordinationDefinitions(), Service: ydbreport.CoordinationService{}})
				provider.Reporting = append(provider.Reporting, engine.Reporting{Representation: representation, Definitions: ydbreport.StreamingDefinitions(), Service: ydbreport.StreamingService{}})
			}
		}
		providers = append(providers, provider)
	}
	return assembleSchemaServices(providers)
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
