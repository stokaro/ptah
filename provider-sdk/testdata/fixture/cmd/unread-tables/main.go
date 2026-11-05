// Package main exercises a read that leaves a table out across the
// subprocess boundary. The provider leaves the table orphan_replica out of
// its tables, as the YDB provider leaves a replica table no replication
// writes any more, and names it with a reason under schema.unread-tables
// when the host lists that feature. With FIXTURE_UNREAD_TABLES=silent it does
// not list the feature and leaves the table out silently, as a provider of an
// earlier contract does.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

// unreadName is the table the read leaves out, and unreadReason the reason it
// gives.
const (
	unreadName   = "orphan_replica"
	unreadReason = "orphan_replica is a replica table no replication of this database writes any more, which the fixture keeps read-only"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := backend.Description()
	description.SemanticIdentity += "-unread-tables"
	listed := os.Getenv("FIXTURE_UNREAD_TABLES") != "silent"
	if listed {
		description.Features = append(description.Features, provider.UnreadTablesFeature)
		description.Engines[0].Features = append(description.Engines[0].Features, provider.UnreadTablesFeature)
	}
	owner := &unreadProvider{Provider: &backend.Provider{}, listed: listed}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type unreadProvider struct {
	*backend.Provider
	listed bool
}

// negotiated reports whether this call may use the feature: the provider
// lists it and the host listed it.
func (p *unreadProvider) negotiated(ctx context.Context) bool {
	return p.listed && slices.Contains(provider.HostFeatures(ctx), provider.UnreadTablesFeature)
}

func (p *unreadProvider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if p.negotiated(ctx) {
		session.Features = append(session.Features, provider.UnreadTablesFeature)
	}
	return session, nil
}

func (p *unreadProvider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	negotiated := p.negotiated(ctx)
	if negotiated {
		snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.UnreadTablesFeature)
	}
	snapshot.Tables = slices.DeleteFunc(snapshot.Tables, func(table *protocol.Table) bool {
		if table.GetNormalizedIdentity().GetName() != unreadName {
			return false
		}
		if negotiated {
			ref := proto.CloneOf(table.GetIdentity())
			ref.DisplayName = ""
			snapshot.UnreadTables = append(snapshot.UnreadTables, &protocol.UnreadTable{Table: ref, Diagnostic: &protocol.ProviderDiagnostic{Reason: unreadReason}})
		}
		return true
	})
	return snapshot, nil
}
