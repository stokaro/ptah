package providerhost

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/internal/providersource"
)

// goSource is the provider's claim on Go annotation syntax: the directives it
// owns, indexed by name, and the coverage a Go source states for them. The
// annotation frontend lexes each comment and checks the attributes against the
// claim before the host sends anything to the provider.
type goSource struct {
	claims map[string]providersource.GoClaim
	// base is the coverage of a Go source that declares nothing, which the
	// provider states once for the process. A source with declarations adds its
	// subject records to it.
	mu   sync.Mutex
	base schemaext.Coverage
	read bool
}

// goDirectiveClaims validates the Go directives of every engine, so that a
// provider whose syntax the host cannot read fails to start.
func goDirectiveClaims(description *protocol.Description) (*goSource, error) {
	claims, err := providersource.GoClaims(description.GetEngines())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", provider.ErrProtocol, err)
	}
	return &goSource{claims: claims}, nil
}

// annotationExtensions returns the provider's Go annotation extension, or none
// when no engine claims a directive. A provider's models reach the annotation
// frontend only through the directives it claims.
func (h *Host) annotationExtensions() []annotation.Extension {
	if h.goSource == nil || len(h.goSource.claims) == 0 {
		return nil
	}
	extension := annotation.Extension{Owner: h.description.GetProviderId(),
		File:     func() annotation.FileDecoder { return &goFileDecoder{host: h} },
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) { return h.goBaseCoverage() }}
	for _, name := range slices.Sorted(maps.Keys(h.goSource.claims)) {
		claim := h.goSource.claims[name].Directive
		directive := annotation.Directive{Name: name,
			Description: "Directive owned by provider " + h.description.GetProviderId() + ".",
			Scopes:      []annotation.Scope{annotation.ScopeStruct}}
		for _, attribute := range claim.GetAttributes() {
			directive.Attributes = append(directive.Attributes, annotation.Attribute{Name: attribute, Value: "string",
				Description: "Attribute of " + name + ", read by provider " + h.description.GetProviderId() + "."})
		}
		extension.Directives = append(extension.Directives, directive)
		for _, kind := range claim.GetModels() {
			if !slices.Contains(extension.Kinds, schemaext.Kind(kind)) {
				extension.Kinds = append(extension.Kinds, schemaext.Kind(kind))
			}
		}
	}
	return []annotation.Extension{extension}
}

// goBaseCoverage returns what a Go source with no declaration of the provider's
// says about its models. The annotation frontend asks for it for every file,
// and the answer does not depend on the file, so the provider is asked once.
func (h *Host) goBaseCoverage() (schemaext.Coverage, error) {
	h.goSource.mu.Lock()
	defer h.goSource.mu.Unlock()
	if h.goSource.read {
		return h.goSource.base, nil
	}
	var combined schemaext.Coverage
	for _, engine := range h.description.GetEngines() {
		if len(engine.GetGoSourceDirectives()) == 0 {
			continue
		}
		decoded, err := h.decodeGoSource(engine, nil, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		claim, err := h.sourceCoverage(engine.GetId(), protocol.SourceDocument_GO_ANNOTATIONS, decoded.GetCoverage())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if combined, err = combined.Combine(claim); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	h.goSource.base, h.goSource.read = combined, true
	return combined, nil
}

// decodeGoSource sends one engine's Go declarations to the provider as one
// document. The annotation frontend passes no context to an owner, so the
// request is not tied to the command's cancellation; it ends when the provider
// process does.
func (h *Host) decodeGoSource(engine *protocol.Engine, tables annotation.Tables, declarations []annotation.Declaration) (*protocol.DecodedSourceDocument, error) {
	ctx := context.Background()
	request, err := providersource.GoAnnotations(ctx, engine, tables, declarations)
	if err != nil {
		return nil, err
	}
	decoded, err := h.client.DecodeSource(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("decode Go directives of engine %q: %w", engine.GetId(), err)
	}
	if len(decoded.GetDocuments()) != 1 {
		return nil, fmt.Errorf("provider returned %d Go documents for one", len(decoded.GetDocuments()))
	}
	return decoded.GetDocuments()[0], nil
}

// goFileDecoder collects the provider's declarations of one Go file and decodes
// them together once the file's tables are known, so a table-scope directive
// can be written before the table directive of its struct.
type goFileDecoder struct {
	host         *Host
	declarations []annotation.Declaration
	subjects     []schemaext.SubjectCoverage
}

func (d *goFileDecoder) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	d.declarations = append(d.declarations, declaration)
	return nil, nil
}

func (d *goFileDecoder) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	var engines []*protocol.Engine
	byEngine := make(map[string][]annotation.Declaration)
	for _, declaration := range d.declarations {
		engine := d.host.goSource.claims[declaration.Directive].Engine
		if _, seen := byEngine[engine.GetId()]; !seen {
			engines = append(engines, engine)
		}
		byEngine[engine.GetId()] = append(byEngine[engine.GetId()], declaration)
	}
	var contributions []annotation.Contribution
	for _, engine := range engines {
		declarations := byEngine[engine.GetId()]
		document, err := d.host.decodeGoSource(engine, tables, declarations)
		if err != nil {
			return nil, err
		}
		fileCoverage, err := d.host.sourceCoverage(engine.GetId(), protocol.SourceDocument_GO_ANNOTATIONS, document.GetCoverage())
		if err != nil {
			return nil, err
		}
		d.subjects = append(d.subjects, fileCoverage.SubjectRecords()...)
		for i, fragment := range document.GetFragments() {
			for _, value := range fragment.GetValues() {
				contribution, err := d.host.goContribution(engine.GetId(), tables, declarations[i], value)
				if err != nil {
					return nil, &annotation.DeclarationError{Declaration: declarations[i], Err: err}
				}
				contributions = append(contributions, contribution)
			}
		}
	}
	return contributions, nil
}

// Cover adds the subjects the provider stated for this file's declarations to
// the coverage a Go source holds, so a table that wrote a directive and a table
// that did not are told apart.
func (d *goFileDecoder) Cover(claim schemaext.Coverage) (schemaext.Coverage, error) {
	if len(d.subjects) == 0 {
		return claim, nil
	}
	return schemaext.NewCoverage(claim.Representation(), claim.KindRecords(), slices.Concat(claim.SubjectRecords(), d.subjects))
}

func (h *Host) goContribution(engine string, tables annotation.Tables, declaration annotation.Declaration, source *protocol.SourceModelValue) (annotation.Contribution, error) {
	decoded, err := h.decodedValue(engine, source)
	if err != nil {
		return annotation.Contribution{}, err
	}
	result := annotation.Contribution{Label: string(decoded.kind), Source: declaration, Targets: []string{engine}}
	switch source.GetPlacement() {
	case protocol.SourceModelValue_NATIVE_OBJECT:
		result.Object = &schemaext.Object{Ref: decoded.subject, Value: decoded.value, Targets: []string{engine}}
	case protocol.SourceModelValue_FACET:
		if err := decoded.requireTable("Go"); err != nil {
			return annotation.Contribution{}, err
		}
		index, err := goTable(tables, source.GetValue().GetValue().GetOwner())
		if err != nil {
			return annotation.Contribution{}, err
		}
		result.Table, result.Facet = tables[index].QualifiedName(), decoded.value
	default:
		return annotation.Contribution{}, fmt.Errorf("unknown decoded Go placement")
	}
	return result, nil
}

func goTable(tables annotation.Tables, subject *protocol.Reference) (int, error) {
	if subject.GetKind() != "table" {
		return 0, unsupported("Go facet attachment to non-table objects")
	}
	found := -1
	for i, table := range tables {
		if table.Name == subject.GetName() && table.Schema == subject.GetSchema() {
			if found >= 0 {
				return 0, fmt.Errorf("decoded Go facet has ambiguous table attachment")
			}
			found = i
		}
	}
	if found < 0 {
		return 0, fmt.Errorf("decoded Go facet names no table of the file")
	}
	return found, nil
}
