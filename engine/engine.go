// Package engine assembles explicit providers for the schema pipeline. It
// imports model and rendering contracts, but no built-in database implementations.
package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/dataext"
	"ptah.run/core/hclext"
	"ptah.run/core/lintext"
	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/routineext"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
)

// ErrInvalidRegistration identifies a malformed provider descriptor or
// conflicting ownership. A failed registration produces no runtime.
var ErrInvalidRegistration = errors.New("invalid provider registration")

// Provider describes services supplied by one explicitly selected provider.
// ID is its stable, namespaced owner identity, separate from target names and
// payload format versions. New copies the descriptor; services themselves
// must remain safe for concurrent calls throughout the runtime's lifetime.
type Provider struct {
	// ID identifies the provider independently of its implementation version.
	ID string
	// Targets lists the target names and services owned by this provider.
	Targets []Target
	// Codecs declares understood model representations. Registration alone does
	// not grant any target permission or capability to use those models.
	Codecs []schemaext.Codec
	// ModelDecoder resolves this owner's serialized models in complete batches.
	// When present, all its Codecs omit the per-value Decode function.
	ModelDecoder *schemaext.BatchDecoder
	// Conversions assigns each target/kind pair to one batched service. The
	// provider must own both schema codecs for each kind it converts.
	Conversions []Conversion
	// Properties assigns source property grammars to their feature owners.
	Properties []PropertySource
	// Comparisons assigns named object semantics and change representations to
	// one owner per target/kind. Registration does not enroll source coverage.
	Comparisons []ObjectComparison
	// FacetComparisons assigns attached settings to contextual comparison owners.
	FacetComparisons []FacetComparison
	// Reporting declares owner-supplied inventory labels and contextual value
	// reports. It grants no inspection, comparison, or target capabilities.
	Reporting []Reporting
	// Reversals reconstruct directional changes and report state that cannot be recovered.
	Reversals []Reversal
	// Planning lowers accepted feature changes into owner-contributed operations.
	Planning []Planning
	// Declarations lowers authored standalone objects into creation operations.
	Declarations []DeclarationPlanning
	// Relations describes dependencies of captured model values without I/O.
	Relations []RelationDiscovery
	// Normalizations attach a connected target's spelling of declared objects
	// before a comparison, through a probe session.
	Normalizations []Normalization
	// FacetNormalizations attach target facts to declared settings on common
	// objects. Their probes use the same isolated session as object probes.
	FacetNormalizations []FacetNormalization
	// Annotations contributes Go annotation directives, their decoders and the
	// coverage a Go annotation source holds. Each extension names this
	// provider as its owner, and the provider owns the desired codec of every
	// model it produces. A directive name belongs to one provider.
	Annotations []annotation.Extension
	// YAML contributes the knowledge a YAML schema document holds about the
	// provider's models. Each extension names this provider as its owner, and
	// the provider owns the desired codec of every model it claims.
	YAML []yamlext.Extension
	// SQL contributes the syntax a SQL schema document of the provider's
	// targets carries for its models, and the knowledge such a document
	// holds about them. Each extension names this provider as its owner and
	// reads documents of targets the provider registers.
	SQL []sqlext.Extension
	// HCL contributes the blocks an HCL schema document carries for the
	// provider's models, and the knowledge every such document holds about
	// them. Each extension names this provider as its owner, and the
	// provider owns the desired codec of every model it claims.
	HCL []hclext.Extension
	// Selections say how a command's scope names the provider's objects:
	// the selector type of each kind and what only the owner can compute of
	// a scope. Each extension names this provider as its owner, and the
	// provider owns the desired codec of every model it selects.
	Selections []selectext.Extension
	// CoverageKinds are the not-described coverage kinds the provider's reader
	// records for state Ptah does not model, such as a YDB changefeed. A
	// document's coverage header and a Go notdescribed declaration may name
	// them only in a runtime that selects the provider; see
	// [Runtime.CoverageVocabulary]. A kind belongs to one provider and is
	// never a common kind.
	CoverageKinds []coverage.Kind
	// ProcessCoverageKinds are the not-described coverage kinds the
	// provider's reader records for an owner to consult in the same process,
	// which no document names, such as a YDB replica table. A writer leaves
	// a record of one out of a document; one of a kind the runtime knows
	// neither way is refused. A kind is registered once, and never as a
	// CoverageKinds kind or a common kind.
	ProcessCoverageKinds []coverage.Kind
	// CoverageFormatLimits state which of the provider's CoverageKinds a
	// document format has no spelling for, such as a YDB changefeed in HCL.
	// A document in that format records each one as not described, so its
	// silence about one is not read as a request to remove it; see
	// [coverage.Vocabulary.FormatLimits]. A kind is one the provider registers
	// in CoverageKinds, stated once per format.
	CoverageFormatLimits []coverage.FormatLimit
	// CoverageLimits map not-described kinds onto the provider's named-object
	// models, so a description that declines a kind, such as a document whose
	// header says it does not describe policies, does not describe the
	// owner's policies either. The kind is a common kind or one a selected
	// provider registers, and the provider owns both codecs of the model. One
	// kind may limit several models; a pair is registered once.
	CoverageLimits []schemaext.LimitMapping
	// Lint publishes the migration lint rules the provider checks and the
	// services that check them. A rule judges targets the runtime registers,
	// whichever provider owns them, and a code belongs to one rule.
	Lint []Lint
	// OffTarget states, per named-object model the provider owns, what a
	// target that serves the model nowhere does with one of its objects:
	// skip it with a note or refuse it with the scope that would keep it;
	// see [schemaext.OffTarget]. A model without a statement reaches the
	// target's services as before, which refuse it there. A model is stated
	// once.
	OffTarget []OffTargetPolicy
}

// OffTargetPolicy is a provider's statement about its objects on a target
// that does not serve their model: one where no provider compares or plans
// declarations of it.
type OffTargetPolicy struct {
	// Kinds are the provider's named-object models the statement covers.
	Kinds []schemaext.Kind
	// Action is the answer: [schemaext.OffTargetSkip] or
	// [schemaext.OffTargetRefuse].
	Action schemaext.OffTarget
	// Label names the model for a reader, such as "PostgreSQL extension".
	Label string
}

// Target declares a canonical target name, accepted aliases, and its optional
// services. Names use lowercase ASCII letters, digits, '-', '_' and '+',
// beginning with a letter. Registering a target does not assert that its server
// supports any particular capability. A nil Rendering service is unavailable.
type Target struct {
	// Name is the canonical target identifier passed to the service.
	Name string
	// Aliases contains additional accepted spellings, excluding Name.
	Aliases []string
	// Rendering is optional. A typed-nil service is an invalid registration.
	Rendering renderer.Service
	// Planner constructs the common migration planner for this target. Nil leaves
	// planning unavailable. Factories must be safe for concurrent calls.
	Planner planext.Factory
	// SchemaRendering lowers and renders whole declarations. Nil means unavailable.
	SchemaRendering renderer.SchemaService
	// Validation checks complete declarations offline. A nil service is unavailable.
	Validation schemavalidation.Service
	// Preparation normalizes captured tables before comparison. Nil is unavailable.
	Preparation schemapreparation.Service
	// Lowering rewrites a whole desired schema into the shape the target's
	// reader reports it in, before comparison. Nil leaves the desired schema
	// as declared.
	Lowering schemapreparation.Lowering
	// Spelling rewrites the values of a desired schema's views, materialized
	// views, sequences, roles and grants into the spelling the target's reader
	// reports them in, such as a view query as the server stores it, for the
	// comparison alone: a plan keeps each object as declared. Nil compares the
	// declaration as written.
	Spelling schemapreparation.Lowering
	// Creations predicts table defaults and key membership for source documents.
	// Nil leaves this offline operation unavailable.
	Creations schemaprojection.TableCreationService
	// Constraints predicts constraint-owned index and column effects. Nil
	// explicitly leaves that prediction unavailable for this target.
	Constraints schemaprojection.ConstraintService
	// Comparison holds the rules the shared comparison takes from this
	// target. A nil member keeps the shared rule for that area.
	Comparison schemacomparison.Rules
	// Splitter splits the target's migration SQL into the statements
	// migration lint reads. Nil leaves lint unable to run for the target.
	Splitter lintext.Splitter
	// Annotator judges the facts of statements a lint run with no target
	// split with the host's dialect-blind scanner: the facts the target's
	// splitter reports for its own split. Nil leaves those facts unjudged on
	// such a run.
	Annotator lintext.Annotator
	// RoutineBodies reads the target's routine bodies for the hosts that
	// analyze them: what a body writes and reads, and where it builds SQL at
	// run time. Nil leaves the target's bodies unanalyzed.
	RoutineBodies routineext.Reader
	// RefusesSkips makes the target refuse an object an owner would skip on
	// it ([schemaext.OffTargetSkip]), as the owner's refusal does. It is for a
	// target that answers nothing it cannot hold with a comment, because a
	// comment lets an apply succeed without the object.
	RefusesSkips bool
	// ReadBack answers what a read of the target reports for a declared
	// column or index where the server keeps something else. Nil means reads
	// report what the declaration wrote.
	ReadBack schemaprojection.ReadBack
	// Data holds the target's managed-data and seed services, which
	// [Runtime.DataServices] hands to the packages that write rows and run
	// seeds. A nil member is a service the target does not have; a typed-nil
	// member is an invalid registration.
	Data dataext.Services
}

// Runtime is a frozen selection of providers. It is safe for concurrent calls
// when its services satisfy their contracts. The zero value has no providers;
// it refuses every target and never falls back to a built-in implementation.
type Runtime struct {
	targets             map[string]target
	codecs              schemaext.Registry
	conversions         map[conversionKey]int
	conversionServices  []schemaext.ConversionService
	properties          map[propertyKey]int
	propertyServices    []PropertySource
	comparisons         map[conversionKey]int
	comparisonServices  []ObjectComparison
	facetComparisons    map[conversionKey]int
	facetServices       []FacetComparison
	reports             map[reportingKey]int
	reportingServices   []Reporting
	reversals           map[conversionKey]int
	reversalServices    []schemaext.ReversalService
	planning            map[conversionKey]int
	parentPlanning      map[conversionKey]int
	planningServices    []ownedPlanning
	declarations        map[conversionKey]int
	declarationServices []ownedDeclarationPlanning
	relations           map[relationKey]int
	relationServices    []RelationDiscovery
	probes              map[conversionKey]int
	probeServices       []Normalization
	facetProbes         []FacetNormalization
	annotations         annotation.Set
	yaml                yamlext.Set
	sql                 sqlext.Set
	hcl                 hclext.Set
	selections          selectext.Set
	vocabulary          coverage.Vocabulary
	limits              []schemaext.LimitMapping
	lintRules           map[string]ownedLintRule
	lintServices        []lintext.Service
	offTarget           map[schemaext.Kind]OffTargetPolicy
}

type target struct {
	planner         planext.Factory
	owner           string
	name            string
	selection       schemaext.TargetSelection
	rendering       renderer.Service
	schemaRendering renderer.SchemaService
	validation      schemavalidation.Service
	preparation     schemapreparation.Service
	lowering        schemapreparation.Lowering
	spelling        schemapreparation.Lowering
	comparison      schemacomparison.Rules
	creations       schemaprojection.TableCreationService
	constraints     schemaprojection.ConstraintService
	splitter        lintext.Splitter
	annotator       lintext.Annotator
	routineBodies   routineext.Reader
	refusesSkips    bool
	data            dataext.Services
	readBack        schemaprojection.ReadBack
}

// New validates and freezes explicit provider ownership. Duplicate provider
// IDs, target names, and aliases are errors, including duplicates within one
// provider. Registration calls no services and performs no database I/O.
func New(providers ...Provider) (*Runtime, error) {
	runtime := &Runtime{
		targets: make(map[string]target), conversions: make(map[conversionKey]int), comparisons: make(map[conversionKey]int),
		facetComparisons: make(map[conversionKey]int),
		reports:          make(map[reportingKey]int), reversals: make(map[conversionKey]int), planning: make(map[conversionKey]int),
		parentPlanning: make(map[conversionKey]int),
		declarations:   make(map[conversionKey]int),
		relations:      make(map[relationKey]int),
		properties:     make(map[propertyKey]int),
		probes:         make(map[conversionKey]int),
		lintRules:      make(map[string]ownedLintRule),
		offTarget:      make(map[schemaext.Kind]OffTargetPolicy),
	}
	owners := make(map[string]struct{}, len(providers))
	var codecs []schemaext.OwnedCodec
	for _, provider := range providers {
		if !schemaext.Kind(provider.ID).Valid() {
			return nil, fmt.Errorf("%w: invalid provider identity %q", ErrInvalidRegistration, provider.ID)
		}
		if _, found := owners[provider.ID]; found {
			return nil, fmt.Errorf("%w: duplicate provider %q", ErrInvalidRegistration, provider.ID)
		}
		owners[provider.ID] = struct{}{}
		for _, codec := range provider.Codecs {
			codecs = append(codecs, schemaext.OwnedCodec{Owner: provider.ID, Codec: codec, BatchDecoder: provider.ModelDecoder})
		}
		for _, declared := range provider.Targets {
			if err := runtime.register(provider.ID, declared); err != nil {
				return nil, err
			}
		}
	}
	registry, err := schemaext.NewRegistry(codecs...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	runtime.codecs = registry
	for _, provider := range providers {
		if err := runtime.registerServices(provider); err != nil {
			return nil, err
		}
		if err := runtime.registerStatements(provider); err != nil {
			return nil, err
		}
	}
	if err := runtime.registerSources(providers); err != nil {
		return nil, err
	}
	return runtime, nil
}

// registerStatements registers a provider's probes, lint rules and
// off-target answers, once every provider's codecs and services are known.
func (r *Runtime) registerStatements(provider Provider) error {
	for _, normalization := range provider.FacetNormalizations {
		if err := r.registerFacetNormalization(provider.ID, normalization); err != nil {
			return err
		}
	}

	for _, normalization := range provider.Normalizations {
		if err := r.registerNormalization(provider.ID, normalization); err != nil {
			return err
		}
	}
	for _, lint := range provider.Lint {
		if err := r.registerLint(provider.ID, lint); err != nil {
			return err
		}
	}
	for _, policy := range provider.OffTarget {
		if err := r.registerOffTarget(provider.ID, policy); err != nil {
			return err
		}
	}
	return nil
}

// registerSources freezes what the providers contribute to the desired-schema
// sources: the coverage kinds and limits a source may record, and the Go
// annotation, YAML and SQL extensions.
func (r *Runtime) registerSources(providers []Provider) error {
	for _, register := range []func([]Provider) error{
		r.registerCoverageKinds, r.registerCoverageLimits, r.registerAnnotations, r.registerYAML, r.registerSQL, r.registerHCL, r.registerSelections,
	} {
		if err := register(providers); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) registerServices(provider Provider) error {
	for _, declaration := range provider.Declarations {
		if err := r.registerDeclarationPlanning(provider.ID, declaration); err != nil {
			return err
		}
	}
	for _, discovery := range provider.Relations {
		if err := r.registerRelations(provider.ID, discovery); err != nil {
			return err
		}
	}
	for _, source := range provider.Properties {
		if err := r.registerPropertySource(provider.ID, source); err != nil {
			return err
		}
	}
	for _, planning := range provider.Planning {
		if err := r.registerPlanning(provider.ID, planning); err != nil {
			return err
		}
	}
	for _, conversion := range provider.Conversions {
		if err := r.registerConversion(provider.ID, conversion); err != nil {
			return err
		}
	}
	for _, comparison := range provider.Comparisons {
		if err := r.registerComparison(provider.ID, comparison); err != nil {
			return err
		}
	}
	for _, comparison := range provider.FacetComparisons {
		if err := r.registerFacetComparison(provider.ID, comparison); err != nil {
			return err
		}
	}
	for _, reversal := range provider.Reversals {
		if err := r.registerReversal(provider.ID, reversal); err != nil {
			return err
		}
	}
	for _, reporting := range provider.Reporting {
		if err := r.registerReporting(provider.ID, reporting); err != nil {
			return err
		}
	}
	return nil
}

// Codecs returns the runtime's immutable model registry. A nil runtime knows
// no codecs. Callers use its context-aware batch operations at artifact boundaries.
func (r *Runtime) Codecs() schemaext.Registry {
	if r == nil {
		return schemaext.Registry{}
	}
	return r.codecs
}

func (r *Runtime) register(owner string, declared Target) error {
	selection, err := schemaext.NewTargetSelection(declared.Name, declared.Aliases...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRegistration, err)
	}
	for _, service := range []struct {
		name  string
		value any
	}{
		{"rendering", declared.Rendering},
		{"schema rendering", declared.SchemaRendering},
		{"validation", declared.Validation},
		{"preparation", declared.Preparation},
		{"lowering", declared.Lowering},
		{"spelling", declared.Spelling},
		{"creation projection", declared.Creations},
		{"constraint projection", declared.Constraints},
		{"column comparison", declared.Comparison.Columns},
		{"index comparison", declared.Comparison.Indexes},
		{"index definition comparison", declared.Comparison.IndexDefinitions},
		{"index predicate comparison", declared.Comparison.IndexPredicates},
		{"foreign key index comparison", declared.Comparison.ForeignKeyIndexes},
		{"generated expression comparison", declared.Comparison.GeneratedExpressions},
		{"view comparison", declared.Comparison.Views},
		{"routine comparison", declared.Comparison.Routines},
		{"statement splitter", declared.Splitter},
		{"statement annotator", declared.Annotator},
		{"routine body reader", declared.RoutineBodies},
		{"read-back", declared.ReadBack},
		{"data literal", declared.Data.Literals},
		{"data column", declared.Data.Columns},
		{"data row", declared.Data.Rows},
		{"data write", declared.Data.Writes},
	} {
		if service.value != nil && nilService(service.value) {
			return fmt.Errorf("%w: target %q has a typed-nil %s service", ErrInvalidRegistration, declared.Name, service.name)
		}
	}
	for _, name := range selection.Names() {
		if existing, found := r.targets[name]; found {
			return fmt.Errorf("%w: target name %q is claimed by %q and %q",
				ErrInvalidRegistration, name, existing.owner, owner)
		}
		r.targets[name] = target{
			owner: owner, name: declared.Name, selection: selection, planner: declared.Planner,
			rendering: declared.Rendering, schemaRendering: declared.SchemaRendering,
			validation: declared.Validation, preparation: declared.Preparation, lowering: declared.Lowering, spelling: declared.Spelling,
			constraints: declared.Constraints, creations: declared.Creations, comparison: declared.Comparison,
			splitter: declared.Splitter, annotator: declared.Annotator, routineBodies: declared.RoutineBodies,
			refusesSkips: declared.RefusesSkips, data: declared.Data, readBack: declared.ReadBack,
		}
	}
	return nil
}

// Targets returns canonical target names, sorted and without aliases. The
// result is a fresh slice. A nil or zero runtime returns an empty slice.
func (r *Runtime) Targets() []string {
	result := make([]string, 0)
	if r == nil {
		return result
	}
	for name, target := range r.targets {
		if name == target.name {
			result = append(result, name)
		}
	}
	slices.Sort(result)
	return result
}

// Render dispatches a complete batch to its single selected owner. Target
// names are matched without surrounding whitespace and ASCII case. Unknown
// targets satisfy errors.Is(err, ptaherr.ErrUnsupportedDialect); unavailable
// rendering satisfies errors.Is(err, ptaherr.ErrUnsupportedFeature).
//
// Cancellation and service errors propagate without partial SQL. A nil context
// is rejected. The service receives a cloned node slice and the caller's
// capability set, which is read-only; its contract also prohibits mutation of
// the nodes themselves. Completed
// refusals return renderer.BatchRefusalError with diagnostic data and local
// input-node provenance. Every reply must account for batch completion.
func (r *Runtime) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	if ctx == nil {
		return renderer.Result{}, errors.New("rendering requires a context")
	}
	if err := ctx.Err(); err != nil {
		return renderer.Result{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return renderer.Result{}, &ptaherr.RenderError{
			Dialect: request.Target,
			Err:     ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("unsupported database dialect: %s", request.Target),
		}
	}
	if selected.rendering == nil {
		return renderer.Result{}, &ptaherr.CapabilityError{
			Dialect: selected.name,
			Feature: "rendering",
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("provider %q does not offer rendering for %q", selected.owner, selected.name),
		}
	}
	request.Target = selected.name
	return renderer.Render(ctx, selected.rendering, request)
}

func (r *Runtime) lookup(name string) (target, bool) {
	if r == nil {
		return target{}, false
	}
	spelling, err := schemaext.NormalizeTargetSpelling(name)
	if err != nil {
		return target{}, false
	}
	selected, found := r.targets[spelling]
	return selected, found
}

func nilService(service any) bool {
	value := reflect.ValueOf(service)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
