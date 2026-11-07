package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/query"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/sqlident"
)

// registerQueryDialects registers the query-builder spelling each engine
// declares, so core/query renders statements for it while the provider is
// selected, and the quoting of that spelling with internal/sqlident, so the
// host writes and reads the engine's names as the engine does. An engine the
// process registry already describes keeps the descriptor Ptah ships, which
// is the one a built-in engine and its provider share. A spelling the
// builder does not know refuses the provider.
func registerQueryDialects(engines []*protocol.Engine) error {
	for _, engine := range engines {
		declared := engine.GetQueryDialect()
		if declared == nil {
			continue
		}
		if _, described := platform.Lookup(engine.GetId()); described {
			continue
		}
		spelling := querySpelling(declared)
		if err := query.RegisterDialect(engine.GetId(), spelling); err != nil {
			return fmt.Errorf("%w: engine %s declares a query dialect the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
		if err := sqlident.RegisterQuoting(spelling.Quoting, append([]string{engine.GetId()}, engine.GetAliases()...)...); err != nil {
			return fmt.Errorf("%w: engine %s declares quoting the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
	}
	return nil
}

// declareLexicalRules registers the lexical rules each engine's query
// dialect states (provider.QueryLexicalFeature) with internal/dialectlexer,
// under the engine's ID and aliases, and returns what releases them. Every
// reader of the engine's text through the shared lexer then reads it as the
// engine does: a SQL schema document in the FRAGMENTS mode, a migration
// body's directives, lint and lineage. An engine the process registry
// describes keeps its descriptor's rules. Rules the lexer does not know
// refuse the provider, and release what was registered before them.
func declareLexicalRules(engines []*protocol.Engine) ([]func(), error) {
	var releases []func()
	for _, engine := range engines {
		lexical := engine.GetQueryDialect().GetLexical()
		if lexical == nil {
			continue
		}
		if _, described := platform.Lookup(engine.GetId()); described {
			continue
		}
		release, err := dialectlexer.DeclareLexical(lexicalRules(lexical), append([]string{engine.GetId()}, engine.GetAliases()...)...)
		if err != nil {
			for _, registered := range releases {
				registered()
			}
			return nil, fmt.Errorf("%w: engine %s states lexical rules the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
		releases = append(releases, release)
	}
	return releases, nil
}

// lexicalRules reads declared lexical rules into the descriptor's lexical
// section. The field names are the descriptor's, so each value moves across
// as it is; the lexer refuses an executable comment style it does not know.
func lexicalRules(declared *protocol.QueryLexical) descriptor.Lexical {
	return descriptor.Lexical{
		BackslashEscapes:      declared.GetBackslashEscapes(),
		EscapeStrings:         declared.GetEscapeStrings(),
		ContinuedStrings:      declared.GetContinuedStrings(),
		UnicodeStrings:        declared.GetUnicodeStrings(),
		NestedComments:        declared.GetNestedComments(),
		DashCommentNeedsSpace: declared.GetDashCommentNeedsSpace(),
		ExecutableComments:    descriptor.ExecutableComments(declared.GetExecutableComments()),
		BracketIdentifiers:    declared.GetBracketIdentifiers(),
		HashNames:             declared.GetHashNames(),
	}
}

// querySpelling reads a declared query dialect into the builder's spelling.
// The field names are the descriptor's, so each value moves across as it is;
// the builder refuses a value it does not know.
func querySpelling(declared *protocol.QueryDialect) query.Spelling {
	quoting, spelling := declared.GetQuoting(), declared.GetQuery()
	joins := make([]descriptor.Join, 0, len(spelling.GetUnsupportedJoins()))
	for _, join := range spelling.GetUnsupportedJoins() {
		joins = append(joins, descriptor.Join(join))
	}
	capabilities := make(map[capability.Capability]bool, len(declared.GetDefaultCapabilities()))
	for name, value := range declared.GetDefaultCapabilities() {
		capabilities[capability.Capability(name)] = value
	}
	result := query.Spelling{
		Placeholder: descriptor.Placeholder(declared.GetPlaceholder()),
		Quoting: descriptor.Quoting{Style: descriptor.QuoteStyle(quoting.GetStyle()), Bare: descriptor.BareNames(quoting.GetBare()),
			Keywords: slices.Clone(quoting.GetKeywords()), Paths: quoting.GetPaths()},
		Query: descriptor.Query{ChecksCapabilities: spelling.GetChecksCapabilities(), OffsetFetch: spelling.GetOffsetFetch(),
			FetchOrderBy: spelling.GetFetchOrderBy(), OffsetOnlyLimit: spelling.GetOffsetOnlyLimit(), UnsignedBounds: spelling.GetUnsignedBounds(),
			Upsert: descriptor.Upsert(spelling.GetUpsert()), UpsertRefusal: spelling.GetUpsertRefusal(), UpdateRefusal: spelling.GetUpdateRefusal()},
		DefaultCapabilities: capabilities,
	}
	if len(joins) > 0 {
		result.Query.UnsupportedJoins = joins
	}
	return result
}
