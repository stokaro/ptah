package providerhost

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// Published returns what a recorded copy of the provider keeps of the
// description it negotiated: its identity and, for each engine, the lint rules
// and the Go annotation claims it publishes. A tool that runs without the
// provider, such as the documentation build, reads such a record through
// [PublishedLintRules] and [PublishedAnnotations], the conversions a started
// provider's declarations go through. The result is a copy.
func (h *Host) Published() *protocol.Description {
	published := &protocol.Description{Revision: h.description.GetRevision(), ProviderId: h.description.GetProviderId(),
		Version: h.description.GetVersion()}
	for _, engine := range h.description.GetEngines() {
		recorded := &protocol.Engine{Id: engine.GetId(), Aliases: slices.Clone(engine.GetAliases())}
		for _, rule := range engine.GetLintRules() {
			recorded.LintRules = append(recorded.LintRules, proto.CloneOf(rule))
		}
		for _, directive := range engine.GetGoSourceDirectives() {
			recorded.GoSourceDirectives = append(recorded.GoSourceDirectives, proto.CloneOf(directive))
		}
		published.Engines = append(published.Engines, recorded)
	}
	return published
}
