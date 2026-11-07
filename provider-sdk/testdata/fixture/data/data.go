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
	"google.golang.org/protobuf/proto"
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
	// SQL text, for a host test of the refusal that answers it. A host that
	// asks for the text form receives SQL text.
	NativeWrites bool
	// ReservedSeeds reports the seed ledger as reserved storage of purpose
	// SEEDS to a host that negotiated it, rather than leaving it out of the
	// read unsaid.
	ReservedSeeds bool
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

// Open opens a session that, with ReservedSeeds, states the reserved seed
// ledger among its read features where the host negotiated it, as a read of
// it then does.
func (p *Provider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.TransactionalProvider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if p.reservesSeeds(ctx) {
		session.Features = append(session.Features, provider.ReservedSeedsFeature)
	}
	return session, nil
}

// reservesSeeds reports ReservedSeeds under a host that lists the feature.
func (p *Provider) reservesSeeds(ctx context.Context) bool {
	return p.ReservedSeeds && slices.Contains(provider.HostFeatures(ctx), provider.ReservedSeedsFeature)
}

// Inspect leaves the seed ledger out of the snapshot's tables. With
// ReservedSeeds, and a host that lists schema.reserved-seeds, it reports the
// ledger as reserved storage of purpose SEEDS instead.
func (p *Provider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.TransactionalProvider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	reserve := p.reservesSeeds(ctx)
	snapshot.Tables = slices.DeleteFunc(snapshot.Tables, func(table *protocol.Table) bool {
		if !strings.EqualFold(table.GetIdentity().GetName(), LedgerTable) {
			return false
		}
		if reserve {
			snapshot.ReservedObjects = append(snapshot.ReservedObjects, &protocol.ReservedObject{Object: proto.CloneOf(table.GetIdentity()),
				NormalizedObject: proto.CloneOf(table.GetNormalizedIdentity()), Purpose: protocol.ReservedObject_SEEDS, ConflictKinds: []string{"table"}})
		}
		return true
	})
	if reserve {
		snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ReservedSeedsFeature)
	}
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
