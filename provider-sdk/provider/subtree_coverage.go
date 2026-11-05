package provider

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// SchemaSubtreeCoverageFeature enables coverage regions that cover a schema
// and every schema nested below it (ADR 0064): ModelNamespace.schema_subtree
// in a read's coverage and ComparisonNamespace.schema_subtree in a
// comparison's, under the IdentifierRules.schema_separator an engine declares.
// The provider and each engine that sends such regions list it, and the host
// lists it in its host features. Contract 25.22.
//
// A subtree region covers a subject whose normalized schema SchemaWithin
// places below the region's. Regions combine as before: unknown knowledge in a
// matching region overrides complete knowledge, so a read that claims a
// subtree complete and leaves a schema below it out sends an unknown subtree
// region for that schema. A filtered read sends no subtree region, since the
// namespaces its selectors bind each name one exact schema.
const SchemaSubtreeCoverageFeature = "schema.subtree-coverage"

// SchemaWithin reports whether the normalized schema subject lies in the
// subtree of the normalized schema region, under the engine's schema
// separator: subject is region, region is the empty root schema, or subject
// begins with region followed by separator. An empty separator nests nothing,
// so only region itself lies within it. The comparison is on normalized
// spellings and never splits a component, which no separator occurs in.
//
// The host's coverage lookup and a provider's comparison both decide a
// subtree region with this rule.
func SchemaWithin(region, subject, separator string) bool {
	if subject == region {
		return true
	}
	if separator == "" {
		return false
	}
	return region == "" || strings.HasPrefix(subject, region+separator)
}

// validSchemaSeparator accepts the empty separator of an engine whose schemas
// do not nest, or one character that is neither a letter, a digit nor a
// space, so it cannot be part of a schema name an author writes plainly.
func validSchemaSeparator(separator string) bool {
	if separator == "" {
		return true
	}
	char, size := utf8.DecodeRuneInString(separator)
	return size == len(separator) && char != utf8.RuneError && unicode.IsGraphic(char) &&
		!unicode.IsLetter(char) && !unicode.IsDigit(char) && !unicode.IsSpace(char)
}

// validateSchemaSubtreeFeature ties an engine's schema separator to the
// feature: an engine that lists it declares a separator, and one that
// declares a separator lists it, since without the feature no peer reads the
// separator.
func validateSchemaSubtreeFeature(_ *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), SchemaSubtreeCoverageFeature)
	separator := engine.GetIdentifiers().GetSchemaSeparator()
	switch {
	case listed && separator == "":
		return fmt.Errorf("%w: %s requires a schema separator in the engine's identifier rules", ErrProtocol, SchemaSubtreeCoverageFeature)
	case !listed && separator != "":
		return fmt.Errorf("%w: a schema separator requires %s", ErrProtocol, SchemaSubtreeCoverageFeature)
	}
	return nil
}

var (
	errSubtreeNotNegotiated = fmt.Errorf("%w: a subtree region needs %s", ErrProtocol, SchemaSubtreeCoverageFeature)
	errSubtreeWithoutSchema = fmt.Errorf("%w: a subtree region names no schema", ErrProtocol)
)

// subtreeRefusal is the error a subtree region in a read of engine meets: nil
// where the negotiated description lists the feature for the engine.
func subtreeRefusal(description *protocol.Description, engine string) error {
	if EngineOffers(description, engine, SchemaSubtreeCoverageFeature) {
		return nil
	}
	return errSubtreeNotNegotiated
}

// modelSubtreeError is the error a read's region meets for its subtree flag:
// refusal where subtrees were not negotiated, or a subtree that names no
// schema.
func modelSubtreeError(scope *protocol.ModelNamespace, refusal error) error {
	switch {
	case !scope.GetSchemaSubtree():
		return nil
	case refusal != nil:
		return refusal
	case scope.GetSchema() == nil:
		return errSubtreeWithoutSchema
	}
	return nil
}

// comparisonSubtreeError is modelSubtreeError for a comparison's region.
func comparisonSubtreeError(scope *protocol.ComparisonNamespace, refusal error) error {
	switch {
	case !scope.GetSchemaSubtree():
		return nil
	case refusal != nil:
		return refusal
	case scope.GetSchema() == nil:
		return errSubtreeWithoutSchema
	}
	return nil
}

// comparisonSubtreeRefusal is the error a subtree region of a comparison
// meets: nil where the engine negotiated the feature and the comparison's
// identifier rules carry the separator a subtree is decided by.
func comparisonSubtreeRefusal(description *protocol.Description, request *protocol.CompareModelsRequest) error {
	if request.GetIdentifiers().GetRules().GetSchemaSeparator() == "" {
		return errSubtreeNotNegotiated
	}
	return subtreeRefusal(description, request.GetEngine())
}
