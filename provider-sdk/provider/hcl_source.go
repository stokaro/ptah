package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// HCLSourceFeature enables Engine.hcl_source, the HCL blocks and attributes an
// engine claims, SourceFragment.hcl_block and SourceDocument.limits. It is a
// two-way feature. A provider lists it, and so does each engine that declares
// HCL claims, beside DecodeSourceFeature. An engine without claims keeps
// accepting HCL fragments without one. Contract 25.3.
const HCLSourceFeature = "source.hcl"

// The type of the frontend's own block each attribute site extends. An
// IN_TABLE and a TOP_LEVEL claim name a block of the owner's.
var hclSiteTypes = map[protocol.HCLSourceBlock_Site][]string{
	protocol.HCLSourceBlock_TABLE_ATTRIBUTES:             {"table"},
	protocol.HCLSourceBlock_INDEX_ATTRIBUTES:             {"index"},
	protocol.HCLSourceBlock_ROUTINE_ATTRIBUTES:           {"function", "procedure"},
	protocol.HCLSourceBlock_MATERIALIZED_VIEW_ATTRIBUTES: {"materialized"},
}

// hclOwnBlock reports whether a site reads a block of the owner's own rather
// than attributes of the frontend's block.
func hclOwnBlock(site protocol.HCLSourceBlock_Site) bool {
	return site == protocol.HCLSourceBlock_TOP_LEVEL || site == protocol.HCLSourceBlock_IN_TABLE
}

// validHCLName accepts an HCL identifier: a letter or an underscore, then
// letters, digits, underscores and hyphens.
func validHCLName(name string) bool {
	letter := func(r rune) bool { return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }
	return name != "" && letter(rune(name[0])) && !strings.ContainsFunc(name, func(r rune) bool {
		return !letter(r) && r != '-' && (r < '0' || r > '9')
	})
}

// validateHCLSource checks an engine's HCL claim: present exactly when the
// engine lists the feature, which needs the provider's and source decoding,
// with well-formed blocks, one claim per site and type, and every HCL model of
// the engine named by a claim.
func validateHCLSource(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), HCLSourceFeature)
	if listed && (!slices.Contains(description.GetFeatures(), HCLSourceFeature) || !slices.Contains(description.GetFeatures(), DecodeSourceFeature)) {
		return sourceError("HCL claims need the provider's " + HCLSourceFeature + " and source decoding")
	}
	declaration := engine.GetHclSource()
	if listed != (declaration != nil) {
		return sourceError("an engine declares HCL claims exactly when it lists " + HCLSourceFeature)
	}
	if declaration == nil {
		return nil
	}
	type claimKey struct {
		site  protocol.HCLSourceBlock_Site
		block string
	}
	claimed := make(map[claimKey]bool)
	covered := make(map[string]bool)
	for _, block := range declaration.GetBlocks() {
		if err := validateHCLBlock(description, engine.GetId(), block); err != nil {
			return err
		}
		key := claimKey{block.GetSite(), block.GetType()}
		if claimed[key] {
			return sourceError("duplicate HCL block claim")
		}
		claimed[key] = true
		for _, kind := range block.GetModels() {
			covered[kind] = true
		}
	}
	for _, model := range engine.GetValueModels() {
		if slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_HCL) && !covered[model.GetKind()] {
			return sourceError("HCL model has no block claim")
		}
	}
	for i, kind := range declaration.GetHeaderKinds() {
		if !comparisonReason(kind) || slices.Contains(declaration.GetHeaderKinds()[:i], kind) {
			return sourceError("invalid or duplicate HCL header kind")
		}
	}
	return nil
}

func validateHCLBlock(description *protocol.Description, engine string, block *protocol.HCLSourceBlock) error {
	site := block.GetSite()
	if _, known := protocol.HCLSourceBlock_Site_name[int32(site)]; !known || site == protocol.HCLSourceBlock_SITE_UNSPECIFIED {
		return sourceError("unknown HCL block site")
	}
	if !validHCLName(block.GetType()) || strings.TrimSpace(block.GetSelector()) == "" || len(block.GetModels()) == 0 {
		return sourceError("incomplete HCL block claim")
	}
	if types, extends := hclSiteTypes[site]; extends && !slices.Contains(types, block.GetType()) {
		return sourceError("an HCL attribute claim names another block than its site's")
	}
	if err := validateHCLBlockShape(block); err != nil {
		return err
	}
	if err := validateHCLAttributes(block.GetAttributes()); err != nil {
		return err
	}
	if err := validateInheritedNames(block.GetInherited(), hclAttributeNames(block.GetAttributes())); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, kind := range block.GetModels() {
		model := findValueModel(description, engine, kind, protocol.ValueInput_DESIRED)
		if !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_HCL) || seen[kind] {
			return sourceError("HCL block names an unknown, duplicate, or unsupported desired model")
		}
		seen[kind] = true
	}
	return nil
}

// validateHCLBlockShape checks what only some sites take: a naming at the top
// level, children in an owner's own block, a declared type and a grant kind
// at the top level, and an inherited list at an attribute site.
func validateHCLBlockShape(block *protocol.HCLSourceBlock) error {
	site := block.GetSite()
	topLevel := site == protocol.HCLSourceBlock_TOP_LEVEL
	switch block.GetNaming() {
	case protocol.HCLSourceBlock_NAMING_UNSPECIFIED:
		if topLevel {
			return sourceError("a top-level HCL block needs a naming")
		}
	case protocol.HCLSourceBlock_UNLABELED, protocol.HCLSourceBlock_SCHEMA_AND_NAME, protocol.HCLSourceBlock_SCHEMA_AND_QUALIFIED_NAME, protocol.HCLSourceBlock_JOINED_NAME:
		if !topLevel {
			return sourceError("only a top-level HCL block takes a naming")
		}
	default:
		return sourceError("unknown HCL block naming")
	}
	if (len(block.GetChildren()) != 0 && !hclOwnBlock(site)) || ((block.GetDeclaresType() || block.GetGrantKind() != "") && !topLevel) {
		return sourceError("children, a declared type or a grant kind at a site that takes none")
	}
	if len(block.GetInherited()) != 0 && hclOwnBlock(site) {
		return sourceError("only an HCL attribute claim inherits")
	}
	if block.GetGrantKind() != "" && !comparisonReason(block.GetGrantKind()) {
		return sourceError("invalid HCL grant kind")
	}
	for i, child := range block.GetChildren() {
		if !validHCLName(child.GetType()) || slices.ContainsFunc(block.GetChildren()[:i], func(other *protocol.HCLSourceChild) bool { return other.GetType() == child.GetType() }) {
			return sourceError("invalid or repeated HCL child block")
		}
		if err := validateHCLAttributes(child.GetAttributes()); err != nil {
			return err
		}
	}
	return nil
}

func validateHCLAttributes(attributes []*protocol.HCLSourceAttribute) error {
	for i, attribute := range attributes {
		if !validHCLName(attribute.GetName()) || slices.ContainsFunc(attributes[:i], func(other *protocol.HCLSourceAttribute) bool { return other.GetName() == attribute.GetName() }) {
			return sourceError("invalid or repeated HCL attribute")
		}
		kind := attribute.GetKind()
		if _, known := protocol.HCLSourceAttribute_Kind_name[int32(kind)]; !known || kind == protocol.HCLSourceAttribute_KIND_UNSPECIFIED {
			return sourceError("unknown HCL attribute kind")
		}
		reference := kind == protocol.HCLSourceAttribute_REFERENCE || kind == protocol.HCLSourceAttribute_REFERENCES
		if reference != validHCLName(attribute.GetRefers()) || (attribute.GetRefers() != "" && !reference) {
			return sourceError("an HCL reference attribute names the block it refers to, and no other attribute does")
		}
		if attribute.GetOptional() && kind != protocol.HCLSourceAttribute_REFERENCE {
			return sourceError("only an HCL reference attribute is optional")
		}
	}
	return nil
}

// hclAttributeNames returns the names a list of attribute shapes takes.
func hclAttributeNames(attributes []*protocol.HCLSourceAttribute) []string {
	names := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		names = append(names, attribute.GetName())
	}
	return names
}

// validateHCLSourceFragments binds the fragments of an HCL document to the
// engine's negotiated claims. An engine without claims takes fragments
// without one, and no header limit.
func validateHCLSourceFragments(description *protocol.Description, engine string, document *protocol.SourceDocument, models map[string]bool) error {
	if document.GetFormat() != protocol.SourceDocument_HCL {
		return nil
	}
	var declaration *protocol.HCLSourceDeclaration
	for _, declared := range description.GetEngines() {
		if declared.GetId() == engine {
			declaration = declared.GetHclSource()
		}
	}
	for _, limit := range document.GetLimits() {
		if !slices.Contains(declaration.GetHeaderKinds(), limit.GetKind()) {
			return sourceError("HCL limit kind is not a declared header kind")
		}
	}
	for _, fragment := range document.GetFragments() {
		claim := fragment.GetHclBlock()
		if declaration == nil {
			if claim != nil {
				return sourceError("HCL fragment claims a block on an engine that declares none")
			}
			continue
		}
		if claim == nil || !slices.ContainsFunc(declaration.GetBlocks(), func(declared *protocol.HCLSourceBlock) bool { return proto.Equal(claim, declared) }) ||
			fragment.GetSelector() != claim.GetSelector() {
			return sourceError("HCL fragment changed its negotiated block claim")
		}
		for _, kind := range claim.GetModels() {
			if !models[kind] {
				return sourceError("HCL fragment model is outside document scope")
			}
		}
		if err := validateHCLContent(claim, fragment); err != nil {
			return err
		}
		if err := validateHCLBinding(document, claim, fragment); err != nil {
			return err
		}
		if err := validateInheritedAttributes(claim.GetInherited(), fragment); err != nil {
			return err
		}
	}
	return nil
}

// validateHCLContent requires a block of the claim's type at the sites that
// read the owner's own block, and an attribute mapping at the others, holding
// only what the claim names.
func validateHCLContent(claim *protocol.HCLSourceBlock, fragment *protocol.SourceFragment) error {
	if !hclOwnBlock(claim.GetSite()) {
		mapping := fragment.GetValue().GetMapping()
		if mapping == nil {
			return sourceError("HCL attribute claim content must be a mapping")
		}
		return hclMappingWithin(mapping, hclAttributeNames(claim.GetAttributes()))
	}
	// A claim's type is an identifier, so content that is no block differs.
	block := fragment.GetBlock()
	if block.GetType() != claim.GetType() {
		return sourceError("HCL block claim content must be a block of its type")
	}
	if err := hclMappingWithin(block.GetAttributes(), hclAttributeNames(claim.GetAttributes())); err != nil {
		return err
	}
	for _, child := range block.GetChildren() {
		index := slices.IndexFunc(claim.GetChildren(), func(shape *protocol.HCLSourceChild) bool { return shape.GetType() == child.GetType() })
		if index < 0 || len(child.GetChildren()) != 0 {
			return sourceError("HCL child block is outside its claim")
		}
		if err := hclMappingWithin(child.GetAttributes(), hclAttributeNames(claim.GetChildren()[index].GetAttributes())); err != nil {
			return err
		}
	}
	return nil
}

func hclMappingWithin(mapping *protocol.SourceMapping, names []string) error {
	for _, entry := range mapping.GetEntries() {
		if !slices.Contains(names, entry.GetKey()) {
			return sourceError("HCL attribute is outside its claim")
		}
	}
	return nil
}

// validateHCLBinding requires what each site attaches to: nothing at the top
// level, the table for a block in a table and for table attributes, the table
// and the index for index attributes, and the routine or the view for their
// attributes.
func validateHCLBinding(document *protocol.SourceDocument, claim *protocol.HCLSourceBlock, fragment *protocol.SourceFragment) error {
	tableBound := slices.ContainsFunc(document.GetBindings(), func(binding *protocol.SourceBinding) bool {
		return binding.GetId() == fragment.GetBindingId() && binding.GetObject().GetKind() == "table"
	})
	subject := fragment.GetSubject()
	var valid bool
	switch claim.GetSite() {
	case protocol.HCLSourceBlock_TOP_LEVEL:
		valid = fragment.GetBindingId() == "" && subject == nil
	case protocol.HCLSourceBlock_IN_TABLE, protocol.HCLSourceBlock_TABLE_ATTRIBUTES:
		valid = tableBound && subject == nil
	case protocol.HCLSourceBlock_INDEX_ATTRIBUTES:
		valid = tableBound && subject.GetKind() == "index" && len(subject.GetPath()) == 1
	case protocol.HCLSourceBlock_ROUTINE_ATTRIBUTES:
		valid = subject.GetKind() == claim.GetType()
	case protocol.HCLSourceBlock_MATERIALIZED_VIEW_ATTRIBUTES:
		valid = subject.GetKind() == "materialized_view"
	}
	if !valid {
		return sourceError("HCL fragment binding or subject does not match its site")
	}
	return nil
}
