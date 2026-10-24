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
)

// registerQueryDialects registers the query-builder spelling each engine
// declares, so core/query renders statements for it while the provider is
// selected. An engine the process registry already describes keeps the
// descriptor Ptah ships, which is the one a built-in engine and its provider
// share. A spelling the builder does not know refuses the provider.
func registerQueryDialects(engines []*protocol.Engine) error {
	for _, engine := range engines {
		declared := engine.GetQueryDialect()
		if declared == nil {
			continue
		}
		if _, described := platform.Lookup(engine.GetId()); described {
			continue
		}
		if err := query.RegisterDialect(engine.GetId(), querySpelling(declared)); err != nil {
			return fmt.Errorf("%w: engine %s declares a query dialect the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
	}
	return nil
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
