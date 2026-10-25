// Package providerpublished holds what official executable providers publish
// about their engines, recorded from a provider build, for the tools that run
// without a provider: the lint rule catalog the documentation renders, the Go
// annotation schema `ptah schema annotations` writes, and the language
// server's completion.
//
// A record keeps the provider's identity and, for each engine, the lint rules
// and the Go annotation claims its description publishes, in the provider
// contract's JSON. It is read through the conversions a started provider's
// declarations go through, so a tool sees an engine's rules and directives as
// a command that starts the provider sees them. A record speaks for an engine
// only where the runtime a tool reads does not serve it: while an
// implementation in the runtime serves the engine, that implementation is the
// source.
//
// `go run ./internal/cmd/providerpublished <executable> <provider ID>` writes
// a record, named for the last element of the provider ID, from a provider
// build. A record changes when the provider's published catalog does, and is
// refreshed then.
package providerpublished

import (
	"embed"
	"fmt"
	"io/fs"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/encoding/protojson"

	"ptah.run/core/annotation"
	"ptah.run/core/lintext"
	"ptah.run/core/schemaext"
	"ptah.run/internal/providerhost"
)

// records holds one recorded description per file.
//
//go:embed *.json
var records embed.FS

// Descriptions returns every recorded description, ordered by provider ID.
func Descriptions() ([]*protocol.Description, error) {
	names, err := fs.Glob(records, "*.json")
	if err != nil {
		return nil, err
	}
	descriptions := make([]*protocol.Description, 0, len(names))
	for _, name := range names {
		data, err := records.ReadFile(name)
		if err != nil {
			return nil, err
		}
		description := &protocol.Description{}
		if err := protojson.Unmarshal(data, description); err != nil {
			return nil, fmt.Errorf("read the published declarations %s: %w", name, err)
		}
		if strings.TrimSpace(description.GetProviderId()) == "" || len(description.GetEngines()) == 0 {
			return nil, fmt.Errorf("read the published declarations %s: no provider or no engine", name)
		}
		descriptions = append(descriptions, description)
	}
	slices.SortFunc(descriptions, func(a, b *protocol.Description) int { return strings.Compare(a.GetProviderId(), b.GetProviderId()) })
	return descriptions, nil
}

// LintRules returns the lint rules every recorded engine publishes that served
// does not resolve, each targeting its engine, sorted by code.
func LintRules(served schemaext.TargetResolver) ([]lintext.Rule, error) {
	engines, err := unservedEngines(served)
	if err != nil {
		return nil, err
	}
	var rules []lintext.Rule
	for _, engine := range engines {
		rules = append(rules, providerhost.PublishedLintRules(engine)...)
	}
	slices.SortFunc(rules, func(a, b lintext.Rule) int { return strings.Compare(a.Code, b.Code) })
	return rules, nil
}

// LintRuntime returns runtime with the rules [LintRules] returns for served
// beside its own: listing every rule names both, and a recorded engine served
// does not resolve, named by its ID or an alias, lists that engine's rules.
// Every other method is runtime's, so the result enumerates rules and runs no
// recorded engine's analysis. A code both publish is refused.
func LintRuntime(runtime lintext.Runtime, served schemaext.TargetResolver) (lintext.Runtime, error) {
	engines, err := unservedEngines(served)
	if err != nil {
		return nil, err
	}
	published := publishedLint{Runtime: runtime, engines: make(map[string][]lintext.Rule)}
	for _, engine := range engines {
		rules := providerhost.PublishedLintRules(engine)
		for _, name := range append([]string{engine.GetId()}, engine.GetAliases()...) {
			published.engines[name] = rules
		}
		published.rules = append(published.rules, rules...)
	}
	return published, nil
}

// publishedLint lists the recorded rules beside a runtime's own.
type publishedLint struct {
	lintext.Runtime
	rules   []lintext.Rule
	engines map[string][]lintext.Rule
}

func (p publishedLint) LintRules(target string) ([]lintext.Rule, error) {
	if rules, recorded := p.engines[target]; recorded {
		return slices.Clone(rules), nil
	}
	rules, err := p.Runtime.LintRules(target)
	if err != nil || target != "" {
		return rules, err
	}
	for _, rule := range p.rules {
		if slices.ContainsFunc(rules, func(own lintext.Rule) bool { return own.Code == rule.Code }) {
			return nil, fmt.Errorf("%w: lint rule %s is published by the runtime and by a recorded provider", schemaext.ErrDuplicate, rule.Code)
		}
	}
	rules = append(rules, p.rules...)
	slices.SortFunc(rules, func(a, b lintext.Rule) int { return strings.Compare(a.Code, b.Code) })
	return rules, nil
}

// AnnotationSet returns the Go annotation extensions of every recorded
// provider, each claiming the directives of its engines that served does not
// resolve, as one set. The extensions describe directives and attributes for
// a catalog; reading a declaration through them is refused, naming the
// provider to select (see [providerhost.PublishedAnnotations]).
func AnnotationSet(served schemaext.TargetResolver) (annotation.Set, error) {
	descriptions, err := Descriptions()
	if err != nil {
		return annotation.Set{}, err
	}
	var extensions []annotation.Extension
	for _, description := range descriptions {
		engines := unserved(description, served)
		if len(engines) == 0 {
			continue
		}
		extension, claims, err := providerhost.PublishedAnnotations(&protocol.Description{ProviderId: description.GetProviderId(), Engines: engines})
		if err != nil {
			return annotation.Set{}, fmt.Errorf("read the Go annotation claims of %s: %w", description.GetProviderId(), err)
		}
		if claims {
			extensions = append(extensions, extension)
		}
	}
	return annotation.NewSet(extensions...)
}

// unservedEngines returns the recorded engines served does not resolve.
func unservedEngines(served schemaext.TargetResolver) ([]*protocol.Engine, error) {
	descriptions, err := Descriptions()
	if err != nil {
		return nil, err
	}
	var engines []*protocol.Engine
	for _, description := range descriptions {
		engines = append(engines, unserved(description, served)...)
	}
	return engines, nil
}

// unserved returns the engines of description that served does not resolve.
func unserved(description *protocol.Description, served schemaext.TargetResolver) []*protocol.Engine {
	return slices.DeleteFunc(slices.Clone(description.GetEngines()), func(engine *protocol.Engine) bool {
		_, err := served.ResolveTarget(engine.GetId())
		return err == nil
	})
}
