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
// blocks and states the document's knowledge of its models, except where a
// document carries a captured feature coverage header: that is the exact claim
// an export wrote, which the host reads with the provider's codecs.
func (h *Host) hclExtensions() []hclext.Extension {
	engines := h.hclEngines()
	if len(engines) == 0 {
		return nil
	}
	owner := &hclOwner{host: h, engines: engines}
	extension := hclext.Extension{Owner: h.description.GetProviderId(), Decoder: owner, Coverage: owner.coverage, CapturedCoverage: true}
	for _, engine := range engines {
		for _, claim := range engine.GetHclSource().GetBlocks() {
			extension.Blocks = append(extension.Blocks, hclBlock(claim))
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
	// registry reads a captured feature coverage header with the provider's
	// desired codecs; it is built on first use.
	registry func() (schemaext.Registry, error)
	once     sync.Once
}

// engineOf is the engine that claims the block a claim matched.
func (o *hclOwner) engineOf(claim hclext.Claim) (*protocol.Engine, error) {
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
// attaches to: the bound table of a block in a table and of table attributes,
// and the subject of the other sites.
func hclFacetOwner(fragment *protocol.SourceFragment, bound, owner *protocol.Reference) error {
	want := fragment.GetSubject()
	if want == nil && bound != nil {
		want = &protocol.Reference{Kind: "table", Schema: bound.GetSchema(), Name: bound.GetName()}
	}
	if want != nil && providersource.SameSubject(owner, want) {
		return nil
	}
	return fmt.Errorf("%w: decoded HCL facet is owned by %s %q, which is not what its block declares a setting of", ptaherr.ErrInvalidAttributeValue,
		owner.GetKind(), owner.GetName())
}

// coverage is what one document holds of the provider's models: the captured
// feature coverage header's exact claim where the document carries one, and
// otherwise the provider's account of the document, its claims and its header
// limits sent as DecodeSource receives them. A document that carries both a
// captured header and limits of the provider's kinds is refused: two accounts
// of one document cannot both govern it.
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
// provider's desired codecs. The provider takes no part in it.
func (o *hclOwner) capturedCoverage(request hclext.CoverageRequest) (schemaext.Coverage, error) {
	if len(request.Limits) > 0 {
		return schemaext.Coverage{}, fmt.Errorf("%w: a %s header and %s limits of provider %s cannot both govern one HCL document", schemaext.ErrDuplicate,
			schemaext.CoverageHeaderMarker, coverage.DirectiveMarker, o.host.description.GetProviderId())
	}
	o.once.Do(func() {
		registry, err := o.host.desiredRegistry()
		o.registry = func() (schemaext.Registry, error) { return registry, err }
	})
	registry, err := o.registry()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	known, _, err := registry.DecodeCoverageHeaderBodies(context.Background(), schemaext.Desired, request.Captured)
	return known, err
}

// desiredRegistry is a registry of the provider's desired codecs, which reads
// what a document captured of the provider's models.
func (h *Host) desiredRegistry() (schemaext.Registry, error) {
	var owned []schemaext.OwnedCodec
	for _, codec := range h.modelCodecs {
		if codec.Representation == schemaext.Desired {
			owned = append(owned, schemaext.OwnedCodec{Owner: h.description.GetProviderId(), Codec: codec})
		}
	}
	return schemaext.NewRegistry(owned...)
}
