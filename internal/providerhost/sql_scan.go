package providerhost

import (
	"context"
	"slices"

	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// provideScanners registers the provider's scanner as the lexer of each
// engine that scans its SQL (provider.ScanSQLFeature), under the engine's ID
// and aliases, and returns what releases them. Every reader of the engine's
// text through internal/dialectlexer then reads the engine's own tokens: view
// lineage, the indentation of the sql template function, statement splitting
// and the other scans of SQL text.
//
// An engine another host of this process already registered keeps that
// registration: both hosts run the provider the command selected for the
// engine, so either answers the same tokens.
func (h *Host) provideScanners() []func() {
	if !slices.Contains(h.description.GetFeatures(), provider.ScanSQLFeature) {
		return nil
	}
	var releases []func()
	for _, engine := range h.description.GetEngines() {
		if !slices.Contains(engine.GetFeatures(), provider.ScanSQLFeature) {
			continue
		}
		id := engine.GetId()
		scanner := providerscan.New(func(ctx context.Context, text string, head bool) (*protocol.SQLScan, error) {
			return h.client.ScanSQL(ctx, &protocol.ScanSQLRequest{Engine: id, Text: text, Head: head})
		})
		if release, err := lexerext.Provide(scanner, append([]string{id}, engine.GetAliases()...)...); err == nil {
			releases = append(releases, release)
		}
	}
	return releases
}
