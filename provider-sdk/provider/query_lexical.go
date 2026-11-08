package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// QueryLexicalFeature has an engine state, in QueryDialect.lexical, the rules
// the host's shared SQL lexer reads its text with where they differ from the
// SQL standard: escape strings, continued and Unicode strings, nested block
// comments and the others of a dialect descriptor's lexical section. A host
// that does not list it reads a provider engine's text by the standard's
// rules alone, as a host of contract 25.33 does. It is a two-way feature: the
// provider lists it, and so does each engine that states the rules. An engine
// that lists ScanSQLFeature states none, since its scanner reads the text in
// place of the shared lexer. Contract 25.34.
const QueryLexicalFeature = "query.lexical"

// validateQueryLexical checks an engine's lexical rules: the engine states
// them exactly when it lists QueryLexicalFeature, the provider lists the
// feature too, an engine that scans its own SQL states none, and the
// executable comment style is a word. The SDK does not own that style's
// vocabulary: the host refuses one it does not know.
func validateQueryLexical(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), QueryLexicalFeature)
	lexical := engine.GetQueryDialect().GetLexical()
	switch {
	case listed && !slices.Contains(description.GetFeatures(), QueryLexicalFeature):
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, QueryLexicalFeature)
	case listed != (lexical != nil):
		return fmt.Errorf("%w: engine %s states lexical rules exactly when it lists %s", ErrProtocol, engine.GetId(), QueryLexicalFeature)
	case listed && slices.Contains(engine.GetFeatures(), ScanSQLFeature):
		return fmt.Errorf("%w: engine %s scans its own SQL and states no lexical rules", ErrProtocol, engine.GetId())
	case lexical.GetExecutableComments() != "" && !queryWord(lexical.GetExecutableComments()):
		return fmt.Errorf("%w: the executable comment style of engine %s is not a word", ErrProtocol, engine.GetId())
	}
	return nil
}

// negotiateQueryLexical removes an engine's lexical rules unless the host
// lists QueryLexicalFeature, and the feature from an engine left without
// them, as one whose query dialect the host did not ask for is.
func negotiateQueryLexical(engine *protocol.Engine, hostFeatures []string) {
	if engine.GetQueryDialect() != nil && !slices.Contains(hostFeatures, QueryLexicalFeature) {
		engine.QueryDialect.Lexical = nil
	}
	if engine.GetQueryDialect().GetLexical() == nil {
		engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == QueryLexicalFeature })
	}
}
