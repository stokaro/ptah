// Package engine assembles explicit providers for the schema pipeline. It
// imports model and rendering contracts, but no built-in database implementations.
package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
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
	// SchemaRendering lowers and renders whole declarations. Nil means unavailable.
	SchemaRendering renderer.SchemaService
	// Validation checks complete declarations offline. A nil service is unavailable.
	Validation schemavalidation.Service
	// Preparation normalizes captured tables before comparison. Nil is unavailable.
	Preparation schemapreparation.Service
	// Creations predicts table defaults and key membership for source documents.
	// Nil leaves this offline operation unavailable.
	Creations schemaprojection.TableCreationService
	// Constraints predicts constraint-owned index and column effects. Nil
	// explicitly leaves that prediction unavailable for this target.
	Constraints schemaprojection.ConstraintService
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
}

type target struct {
	owner           string
	name            string
	selection       schemaext.TargetSelection
	rendering       renderer.Service
	schemaRendering renderer.SchemaService
	validation      schemavalidation.Service
	preparation     schemapreparation.Service
	creations       schemaprojection.TableCreationService
	constraints     schemaprojection.ConstraintService
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
		properties:     make(map[propertyKey]int),
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
			codecs = append(codecs, schemaext.OwnedCodec{Owner: provider.ID, Codec: codec})
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
	}
	return runtime, nil
}

func (r *Runtime) registerServices(provider Provider) error {
	for _, declaration := range provider.Declarations {
		if err := r.registerDeclarationPlanning(provider.ID, declaration); err != nil {
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
	if declared.Rendering != nil && nilService(declared.Rendering) {
		return fmt.Errorf("%w: target %q has a typed-nil rendering service", ErrInvalidRegistration, declared.Name)
	}
	if declared.SchemaRendering != nil && nilService(declared.SchemaRendering) {
		return fmt.Errorf("%w: target %q has a typed-nil schema rendering service", ErrInvalidRegistration, declared.Name)
	}
	if declared.Validation != nil && nilService(declared.Validation) {
		return fmt.Errorf("%w: target %q has a typed-nil validation service", ErrInvalidRegistration, declared.Name)
	}
	if declared.Preparation != nil && nilService(declared.Preparation) {
		return fmt.Errorf("%w: target %q has a typed-nil preparation service", ErrInvalidRegistration, declared.Name)
	}
	if declared.Creations != nil && nilService(declared.Creations) {
		return fmt.Errorf("%w: target %q has a typed-nil creation projection service", ErrInvalidRegistration, declared.Name)
	}
	if declared.Constraints != nil && nilService(declared.Constraints) {
		return fmt.Errorf("%w: target %q has a typed-nil constraint projection service", ErrInvalidRegistration, declared.Name)
	}
	for _, name := range selection.Names() {
		if existing, found := r.targets[name]; found {
			return fmt.Errorf("%w: target name %q is claimed by %q and %q",
				ErrInvalidRegistration, name, existing.owner, owner)
		}
		r.targets[name] = target{
			owner: owner, name: declared.Name, selection: selection,
			rendering: declared.Rendering, schemaRendering: declared.SchemaRendering,
			validation: declared.Validation, preparation: declared.Preparation,
			constraints: declared.Constraints, creations: declared.Creations,
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
// is rejected. The service receives a cloned capability set and node slice;
// its contract also prohibits mutation of the nodes themselves. Completed
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
