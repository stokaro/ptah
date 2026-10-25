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
	"ptah.run/core/ptaherr"
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
	extension := claimedExtension(h.description.GetProviderId(), h.goSource.claims)
	extension.File = func() annotation.FileDecoder { return &goFileDecoder{host: h} }
	extension.Coverage = func([]coverage.Object) (schemaext.Coverage, error) { return h.goBaseCoverage() }
	return []annotation.Extension{extension}
}

// PublishedAnnotations returns the Go annotation extension a provider's
// description claims, with the directives and attributes a started provider's
// extension declares, for a caller that reads them without starting the
// provider: an editor's completion and the annotation schema. It decodes no
// declaration, and states no coverage of a Go source: both refuse, naming the
// provider to select, because only the provider reads its claims. It returns
// false when no engine of the description claims a directive.
func PublishedAnnotations(description *protocol.Description) (annotation.Extension, bool, error) {
	claims, err := providersource.GoClaims(description.GetEngines())
	if err != nil {
		return annotation.Extension{}, false, err
	}
	if len(claims) == 0 {
		return annotation.Extension{}, false, nil
	}
	providerID := description.GetProviderId()
	extension := claimedExtension(providerID, claims)
	unread := func() error {
		return fmt.Errorf("%w: provider %s reads its Go annotations; select it to read this file", ptaherr.ErrUnsupportedFeature, providerID)
	}
	extension.File = func() annotation.FileDecoder { return unreadFile{err: unread()} }
	extension.Coverage = func([]coverage.Object) (schemaext.Coverage, error) { return schemaext.Coverage{}, unread() }
	return extension, true, nil
}

// unreadFile refuses every declaration of a provider that was not started.
type unreadFile struct{ err error }

func (u unreadFile) Decode(annotation.Declaration) ([]annotation.Contribution, error) {
	return nil, u.err
}
func (u unreadFile) Finish(annotation.Tables) ([]annotation.Contribution, error) { return nil, u.err }

// claimedExtension describes the directives and attributes a provider claims,
// and the models they declare, without a decoder or a coverage claim.
func claimedExtension(providerID string, claims map[string]providersource.GoClaim) annotation.Extension {
	extension := annotation.Extension{Owner: providerID}
	for _, name := range slices.Sorted(maps.Keys(claims)) {
		owned := claims[name]
		claim := owned.Directive
		var attributes []annotation.Attribute
		for _, attribute := range claim.GetAttributes() {
			attributes = append(attributes, annotation.Attribute{Name: attribute, Value: "string",
				Description: "Attribute of " + name + ", read by provider " + providerID + "."})
		}
		if owned.Attributes {
			// The attributes of the directive the claim inherits reach the
			// provider with its own: an index's type, say. The SDK keeps an
			// inherited list only where both sides listed
			// provider.EntryKeysFeature.
			extension.Attributes = append(extension.Attributes, annotation.DirectiveAttributes{Directive: name, Attributes: attributes, PerFile: true,
				Reads: slices.Clone(claim.GetInherited())})
		} else {
			extension.Directives = append(extension.Directives, goDirective(providerID, name, claim, attributes))
		}
		for _, kind := range claim.GetModels() {
			if !slices.Contains(extension.Kinds, schemaext.Kind(kind)) {
				extension.Kinds = append(extension.Kinds, schemaext.Kind(kind))
			}
		}
	}
	return extension
}

// goDirective describes a directive of the provider's own. Its scope says where
// the comment is written and which part of a table the declaration describes:
// a column directive sits on a field beside the field's directive, and an index
// directive beside the index directive it describes.
func goDirective(providerID, name string, claim *protocol.GoSourceDirective, attributes []annotation.Attribute) annotation.Directive {
	directive := annotation.Directive{Name: name, Attributes: attributes,
		Description: "Directive owned by provider " + providerID + ".",
		Scopes:      []annotation.Scope{annotation.ScopeStruct}}
	switch claim.GetScope() {
	case protocol.GoSourceDirective_COLUMN:
		directive.Scopes, directive.Describes = []annotation.Scope{annotation.ScopeField}, annotation.SubjectColumn
	case protocol.GoSourceDirective_INDEX:
		directive.Scopes, directive.Describes = []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField}, annotation.SubjectIndex
	}
	return directive
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
			return nil, refuseDocument(declarations, err)
		}
		fileCoverage, err := d.host.sourceCoverage(engine.GetId(), protocol.SourceDocument_GO_ANNOTATIONS, document.GetCoverage())
		if err != nil {
			return nil, refuseDocument(declarations, err)
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

// refuseDocument places a refusal of a whole document at the first declaration
// the document carries, which is the one a provider that names none is read as
// refusing. It says so where the document carried several.
func refuseDocument(declarations []annotation.Declaration, err error) error {
	if len(declarations) > 1 {
		err = fmt.Errorf("%w (reported at the first of %d Go directives sent together)", err, len(declarations))
	}
	return &annotation.DeclarationError{Declaration: declarations[0], Err: err}
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
		if result.Table, result.Subject, err = h.goFacetSubject(engine, tables, declaration, decoded, source.GetValue().GetValue().GetOwner()); err != nil {
			return annotation.Contribution{}, err
		}
		result.Facet = decoded.value
	default:
		return annotation.Contribution{}, fmt.Errorf("unknown decoded Go placement")
	}
	return result, nil
}

// goFacetSubject says where a decoded facet attaches. A facet of a column or
// an index directive must belong to the column or index the directive
// describes: the host sent that subject, and a provider that answers for
// another part would set it on an object the author never named. A facet of a
// table or document directive attaches to the table its owner names, except
// that a table directive may also answer for a column of its own table: see
// [goTableColumn].
func (h *Host) goFacetSubject(engine string, tables annotation.Tables, declaration annotation.Declaration,
	decoded decodedSourceValue, owner *protocol.Reference) (table string, subject annotation.Subject, err error) {
	claim := h.goSource.claims[declaration.Directive].Directive
	scope := claim.GetScope()
	if scope == protocol.GoSourceDirective_COLUMN || scope == protocol.GoSourceDirective_INDEX {
		index, expected, err := providersource.GoSubject(engine, tables, claim, declaration)
		if err != nil {
			return "", annotation.Subject{}, err
		}
		if !providersource.SameSubject(owner, expected) {
			return "", annotation.Subject{}, fmt.Errorf("decoded Go facet is owned by %s %q, not the %s %q the directive describes",
				owner.GetKind(), owner.GetName(), expected.GetKind(), expected.GetName())
		}
		return tables[index].QualifiedName(), declaration.Subject, nil
	}
	if scope == protocol.GoSourceDirective_TABLE && owner.GetKind() == "column" {
		return goTableColumn(engine, tables, claim, declaration, owner)
	}
	if err := decoded.requireTable("Go"); err != nil {
		return "", annotation.Subject{}, err
	}
	index, err := goTable(tables, owner)
	if err != nil {
		return "", annotation.Subject{}, err
	}
	return tables[index].QualifiedName(), annotation.Subject{}, nil
}

// goTableColumn places a value that a table-scope directive declares for a
// column, such as the column families a table directive lists the columns of.
// The column must belong to the table of the directive's struct: the owner
// names it by the table's schema, the table as the only path component and the
// column's name, and the field the name denotes is a field of that struct. The
// frontend attaches the value to that field once the file is read and refuses
// a name the struct declares no field for. A column of another table, and a
// reference with a database, catalog or overload, are refused rather than
// placed by guess.
func goTableColumn(engine string, tables annotation.Tables, claim *protocol.GoSourceDirective, declaration annotation.Declaration,
	owner *protocol.Reference) (table string, subject annotation.Subject, err error) {
	index, _, err := providersource.GoSubject(engine, tables, claim, declaration)
	if err != nil {
		return "", annotation.Subject{}, err
	}
	home := tables[index]
	if owner.GetName() == "" || owner.GetDatabase() != "" || owner.GetCatalog() != "" || len(owner.GetOverload()) != 0 ||
		owner.GetSchema() != home.Schema || !slices.Equal(owner.GetPath(), []string{home.Name}) {
		return "", annotation.Subject{}, fmt.Errorf("decoded Go facet is owned by column %q of %q, which is not a column of the table %q the directive declares",
			owner.GetName(), owner.GetPath(), home.QualifiedName())
	}
	return home.QualifiedName(), annotation.Subject{Kind: annotation.SubjectColumn, Struct: declaration.Struct, Name: owner.GetName()}, nil
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
