package providerhost

import (
	"context"
	"fmt"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/providersource"
)

// hclEngines are the engines whose provider negotiated provider.HCLSourceFeature
// and declares HCL claims. The SDK drops the claims a host did not ask for.
func (h *Host) hclEngines() []*protocol.Engine {
	var engines []*protocol.Engine
	for _, engine := range h.description.GetEngines() {
		if engine.GetHclSource() != nil && h.targets[engine.GetId()].negotiated(provider.HCLSourceFeature) {
			engines = append(engines, engine)
		}
	}
	return engines
}

// checkHCLSource refuses, when the provider starts, an HCL claim the host
// cannot read, so a provider whose syntax would be dropped does not start, and
// one block that two engines claim: the host picks the engine by the block a
// document writes, as it does by the name of a Go directive.
func checkHCLSource(description *protocol.Description) error {
	claimed := make(map[[2]string]string)
	for _, engine := range description.GetEngines() {
		if err := providersource.CheckHCLSource(engine); err != nil {
			return fmt.Errorf("%w: %w", provider.ErrProtocol, err)
		}
		for _, block := range engine.GetHclSource().GetBlocks() {
			key := [2]string{block.GetSite().String(), block.GetType()}
			if previous, taken := claimed[key]; taken {
				return fmt.Errorf("%w: HCL block %q at site %s is claimed by engines %q and %q; a block has one owner", provider.ErrProtocol,
					block.GetType(), block.GetSite(), previous, engine.GetId())
			}
			claimed[key] = engine.GetId()
		}
	}
	return nil
}

// hclExtensions returns the provider's HCL extension, or none when no engine
// declares HCL claims. Its blocks are the claims of every such engine, each
// read for every dialect, since a block has one engine. The provider reads the
// blocks. It states the document's knowledge of its models where the document's
// header names one of its header kinds; a captured feature coverage header is
// the exact claim an export wrote, which the host reads and writes with the
// provider's codecs; and a document with neither claims nothing of them (see
// coverage).
func (h *Host) hclExtensions() []hclext.Extension {
	engines := h.hclEngines()
	if len(engines) == 0 {
		return nil
	}
	owner := &hclOwner{host: h, engines: engines}
	extension := hclext.Extension{Owner: h.description.GetProviderId(), Decoder: owner, Coverage: owner.coverage, CapturedCoverage: true, Split: owner}
	if slices.ContainsFunc(engines, func(engine *protocol.Engine) bool { return h.targets[engine.GetId()].encodes() }) {
		extension.Encoder = owner
	}
	for _, engine := range engines {
		for _, claim := range engine.GetHclSource().GetBlocks() {
			block := hclBlock(claim)
			if claim.GetSite() == protocol.HCLSourceBlock_TABLE_PLATFORM {
				// The frontend reads the engine's group under its aliases
				// too, as a built-in owner's platform group is read.
				block.Aliases = slices.Clone(engine.GetAliases())
			}
			extension.Blocks = append(extension.Blocks, block)
			for _, kind := range claim.GetModels() {
				if !slices.Contains(extension.Kinds, schemaext.Kind(kind)) {
					extension.Kinds = append(extension.Kinds, schemaext.Kind(kind))
				}
			}
		}
		for _, kind := range engine.GetHclSource().GetHeaderKinds() {
			if !slices.Contains(extension.Header, coverage.Kind(kind)) {
				extension.Header = append(extension.Header, coverage.Kind(kind))
			}
		}
	}
	// An index block of a type an engine claims is read by the provider whole
	// (provider.IndexKindsFeature); the SDK keeps the claims only where both
	// sides listed the feature.
	extension.ReadsIndex, extension.Kinds = readsIndex(engines), withIndexKindModels(extension.Kinds, engines)
	return []hclext.Extension{extension}
}

// hclBlock is the frontend's shape of a provider's block claim.
func hclBlock(claim *protocol.HCLSourceBlock) hclext.Block {
	attributes := func(shapes []*protocol.HCLSourceAttribute) []hclext.Attribute {
		converted := make([]hclext.Attribute, 0, len(shapes))
		for _, shape := range shapes {
			converted = append(converted, hclext.Attribute{Name: shape.GetName(), Kind: providersource.HCLKinds[shape.GetKind()], Refers: shape.GetRefers(),
				Optional: shape.GetOptional()})
		}
		return converted
	}
	block := hclext.Block{Site: providersource.HCLSites[claim.GetSite()], Type: claim.GetType(), Naming: providersource.HCLNamings[claim.GetNaming()],
		Attributes: attributes(claim.GetAttributes()), DeclaresType: claim.GetDeclaresType(), GrantKind: claim.GetGrantKind()}
	for _, child := range claim.GetChildren() {
		block.Children = append(block.Children, hclext.Child{Type: child.GetType(), Unlabeled: child.GetUnlabeled(), Attributes: attributes(child.GetAttributes())})
	}
	return block
}

// hclOwner reads a provider's HCL blocks through DecodeSource.
type hclOwner struct {
	host    *Host
	engines []*protocol.Engine
	// headers reads and writes a captured feature coverage header of the
	// provider's desired models; it is built on first use.
	headers func() (headerCodecs, error)
	once    sync.Once
}

// engineOf is the engine that claims the block a claim matched: for an index
// block the frontend claimed whole, the engine that claims its type.
func (o *hclOwner) engineOf(claim hclext.Claim) (*protocol.Engine, error) {
	if claim.Index != nil && claim.Index.Owned != nil {
		if engine := indexKindEngine(o.engines, claim.Index.Type); engine != nil {
			return engine, nil
		}
		return nil, fmt.Errorf("%w: no engine of provider %s reads an index of type %q", ptaherr.ErrUnsupportedFeature, o.host.description.GetProviderId(), claim.Index.Type)
	}
	for _, engine := range o.engines {
		if providersource.HCLBlockClaim(engine, claim) != nil {
			return engine, nil
		}
	}
	return nil, fmt.Errorf("%w: no engine of provider %s claims the HCL block %q", ptaherr.ErrUnsupportedFeature, o.host.description.GetProviderId(), claim.Block)
}

// DecodeHCL sends the claims of each engine to the provider in one document
// and answers each claim with what the provider decoded from its fragment. A
// facet belongs to what the claim's site attaches to: the index of index
// attributes, the table of table attributes and of a block in a table, the
// routine or the view of their attributes, and, for a top-level block, the
// table its value names, as a table setting.
func (o *hclOwner) DecodeHCL(ctx context.Context, request hclext.Request) (hclext.Result, error) {
	result := hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}
	groups := make(map[*protocol.Engine][]int)
	var order []*protocol.Engine
	for i, claim := range request.Claims {
		engine, err := o.engineOf(claim)
		if err != nil {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: err}
		}
		if _, seen := groups[engine]; !seen {
			order = append(order, engine)
		}
		groups[engine] = append(groups[engine], i)
	}
	for _, engine := range order {
		claims := make([]hclext.Claim, 0, len(groups[engine]))
		for _, i := range groups[engine] {
			claims = append(claims, request.Claims[i])
		}
		wire, err := providersource.HCL(ctx, engine, claims, nil)
		if err != nil {
			return hclext.Result{}, err
		}
		decoded, err := o.host.client.DecodeSource(ctx, wire)
		if err != nil {
			return hclext.Result{}, err
		}
		sent := wire.GetDocuments()[0]
		for position, fragment := range decoded.GetDocuments()[0].GetFragments() {
			claim := groups[engine][position]
			declaration, err := o.declaration(engine.GetId(), sent, sent.GetFragments()[position], fragment.GetValues())
			if err != nil {
				return hclext.Result{}, &hclext.ClaimError{Claim: claim, Err: err}
			}
			result.Declarations[claim] = declaration
		}
	}
	return result, ctx.Err()
}

// declaration is what the values the provider decoded from one fragment
// declare.
func (o *hclOwner) declaration(engine string, document *protocol.SourceDocument, fragment *protocol.SourceFragment, values []*protocol.SourceModelValue) (hclext.Declaration, error) {
	var declaration hclext.Declaration
	for _, source := range values {
		decoded, err := o.host.decodedValue(engine, source)
		if err != nil {
			return hclext.Declaration{}, err
		}
		if source.GetPlacement() == protocol.SourceModelValue_NATIVE_OBJECT {
			declaration.Objects = append(declaration.Objects, schemaext.Object{Ref: decoded.subject, Value: decoded.value, Targets: []string{engine}})
			continue
		}
		owner := source.GetValue().GetValue().GetOwner()
		if fragment.GetHclBlock().GetSite() == protocol.HCLSourceBlock_TOP_LEVEL {
			if owner.GetKind() != "table" || declaration.TableSetting != nil {
				return hclext.Declaration{}, fmt.Errorf("%w: a top-level HCL block declares one setting of a table, and the provider decoded a facet of %s %q",
					ptaherr.ErrInvalidAttributeValue, owner.GetKind(), owner.GetName())
			}
			if err := decoded.requireTable("HCL"); err != nil {
				return hclext.Declaration{}, err
			}
			declaration.TableSetting = decoded.value
			continue
		}
		var bound *protocol.Reference
		for _, binding := range document.GetBindings() {
			if binding.GetId() == fragment.GetBindingId() {
				bound = binding.GetObject()
			}
		}
		if err := hclFacetOwner(fragment, bound, owner); err != nil {
			return hclext.Declaration{}, err
		}
		facets, err := declaration.Facets.With(decoded.value)
		if err != nil {
			return hclext.Declaration{}, err
		}
		if declaration.Facets, err = facets.WithTargetScope(decoded.kind, engine); err != nil {
			return hclext.Declaration{}, err
		}
	}
	return declaration, nil
}

// hclFacetOwner refuses a facet whose owner is not what the fragment's site
// attaches to: the bound table of a block in a table, of table attributes and
// of an index block the provider reads whole, and the subject of the other
// sites.
func hclFacetOwner(fragment *protocol.SourceFragment, bound, owner *protocol.Reference) error {
	want := fragment.GetSubject()
	if (want == nil || fragment.GetIndexKind() != nil) && bound != nil {
		want = &protocol.Reference{Kind: "table", Schema: bound.GetSchema(), Name: bound.GetName()}
	}
	if want != nil && providersource.SameSubject(owner, want) {
		return nil
	}
	return fmt.Errorf("%w: decoded HCL facet is owned by %s %q, which is not what its block declares a setting of", ptaherr.ErrInvalidAttributeValue,
		owner.GetKind(), owner.GetName())
}

// coverage is what one document holds of the provider's models: the captured
// feature coverage header's exact claim where the document carries one, the
// provider's account of the document for each engine whose HCL header kinds
// the document's ptah:not-described header names, and nothing of any other
// engine's models. A document that carries both a captured header and limits
// of the provider's kinds is refused: two accounts of one document cannot
// both govern it.
//
// A document without such a header entry is an ordinary Atlas file, which
// names none of the provider's objects it leaves out, so it makes no claim of
// the engine's models and the provider is not asked: a decoding may claim
// less than complete only through the document's header limits, so the
// provider's account of a document without them would be complete for every
// model, and a plan would drop each object of those models the document does
// not declare. This is the built-in YDB engine's rule for its coordination
// nodes. An export keeps its claim through the captured header the encoder
// writes.
func (o *hclOwner) coverage(request hclext.CoverageRequest) (schemaext.Coverage, error) {
	if len(request.Captured) > 0 {
		return o.capturedCoverage(request)
	}
	ctx := context.Background()
	var combined schemaext.Coverage
	for _, engine := range o.engines {
		var claims []hclext.Claim
		for _, claim := range request.Claims {
			if owner, err := o.engineOf(claim); err == nil && owner == engine {
				claims = append(claims, claim)
			}
		}
		wire, err := providersource.HCL(ctx, engine, claims, request.Limits)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if len(wire.GetDocuments()[0].GetLimits()) == 0 {
			continue
		}
		decoded, err := o.host.client.DecodeSource(ctx, wire)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		known, err := o.host.sourceCoverage(engine.GetId(), protocol.SourceDocument_HCL, decoded.GetDocuments()[0].GetCoverage())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if combined, err = combined.Combine(known); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return combined, nil
}

// capturedCoverage reads a captured feature coverage header with the
// provider's desired codecs (see headerCodecs). The provider takes no part in
// it.
func (o *hclOwner) capturedCoverage(request hclext.CoverageRequest) (schemaext.Coverage, error) {
	if len(request.Limits) > 0 {
		return schemaext.Coverage{}, fmt.Errorf("%w: a %s header and %s limits of provider %s cannot both govern one HCL document", schemaext.ErrDuplicate,
			schemaext.CoverageHeaderMarker, coverage.DirectiveMarker, o.host.description.GetProviderId())
	}
	headers, err := o.codecs()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return headers.read(context.Background(), request.Captured)
}

// Recognizes reports whether an engine of the provider reads a
// ptah:not-described entry of kind as a limit on its HCL claim, so that every
// file a writer cuts a document into carries the entry.
func (o *hclOwner) Recognizes(kind coverage.Kind) bool {
	return slices.ContainsFunc(o.engines, func(engine *protocol.Engine) bool {
		return slices.Contains(engine.GetHclSource().GetHeaderKinds(), string(kind))
	})
}

// Restate is the captured feature coverage header line that carries the
// document's claim of the provider's models into every file a writer cuts the
// document into, written again with the provider's codecs, and none for a
// document that captured no claim: a file read on its own keeps the claim the
// document made, as the built-in YDB engine's splitter keeps its own. A
// document whose captured header comes with entries the provider reads is
// refused, as reading it is.
func (o *hclOwner) Restate(captured []string, limits []coverage.Object) ([]string, error) {
	if len(captured) == 0 {
		return nil, nil
	}
	known, err := o.capturedCoverage(hclext.CoverageRequest{Captured: captured, Limits: limits})
	if err != nil {
		return nil, err
	}
	headers, err := o.codecs()
	if err != nil {
		return nil, err
	}
	line, err := headers.line(context.Background(), known)
	if err != nil || line == "" {
		return nil, err
	}
	return []string{line}, nil
}

// codecs are the codecs of the provider's desired models, which read and
// write a captured feature coverage header. They are built on first use.
func (o *hclOwner) codecs() (headerCodecs, error) {
	o.once.Do(func() {
		headers, err := o.host.headerCodecs()
		o.headers = func() (headerCodecs, error) { return headers, err }
	})
	return o.headers()
}
