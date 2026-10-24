// Package data serves the fixture's rows, row writes, seeds and query-builder
// spelling on SQLite: the services contract 25.1 adds. It lives apart from
// package backend, which keeps to contract 25.0, and reaches a session only
// through backend.Provider.WithSession.
package data

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// LedgerTable is the table the fixture keeps its seed ledger in. Its layout is
// the fixture's own, and inspection leaves it out like other storage a
// provider keeps for itself.
const LedgerTable = "fixture_seed_ledger"

// Provider serves the base fixture with transactions, session locks and the
// data services.
type Provider struct {
	backend.TransactionalProvider
	// NativeWrites renders every row write as a native operation rather than
	// SQL text, for a host test of the refusal that answers it.
	NativeWrites bool
}

// Features are the services this package adds, which the provider and its
// engine both declare.
func Features() []string {
	return []string{provider.TypeRowsFeature, provider.ReadRowsFeature, provider.RenderRowWritesFeature, provider.SeedsFeature}
}

// Description is the fixture's description with transactions, session locks,
// the data services and the query-builder spelling.
func Description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity = "fixture-data-1"
	added := append([]string{provider.TransactionsFeature, provider.LocksFeature}, Features()...)
	description.Features = append(description.Features, added...)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, added...)
	engine.QueryDialect = QueryDialect()
	return description
}

// QueryDialect is how the query builder spells statements for the fixture:
// question-mark placeholders, double-quoted names and SQLite's ON CONFLICT.
func QueryDialect() *protocol.QueryDialect {
	return &protocol.QueryDialect{
		Placeholder:         "question",
		Quoting:             &protocol.QueryQuoting{Style: "double"},
		Query:               &protocol.QuerySpelling{Upsert: "on-conflict", UnsupportedJoins: []string{"full"}},
		DefaultCapabilities: map[string]bool{"returning_clause": true},
	}
}

// Inspect leaves the seed ledger out of the snapshot.
func (p *Provider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.TransactionalProvider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.Tables = slices.DeleteFunc(snapshot.Tables, func(table *protocol.Table) bool {
		return strings.EqualFold(table.GetIdentity().GetName(), LedgerTable)
	})
	return snapshot, nil
}

// tableName returns the unqualified name of a row operation's table. The
// fixture has one schema, main, which a reference may name or leave out.
func tableName(table *protocol.Reference) (string, error) {
	if schema := table.GetSchema(); schema != "" && !strings.EqualFold(schema, "main") {
		return "", fmt.Errorf("the fixture has no schema other than main")
	}
	if strings.EqualFold(table.GetName(), LedgerTable) {
		return "", fmt.Errorf("the seed ledger is the fixture's own storage")
	}
	return table.GetName(), nil
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
