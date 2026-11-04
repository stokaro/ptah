package main

import (
	"context"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// readRetention reports whether a read gives every table a retention, which
// FIXTURE_READ_RETENTION=1 selects. Without it the program reads no value of
// its own, as the base fixture does.
func readRetention() bool { return os.Getenv("FIXTURE_READ_RETENTION") == "1" }

// readSeconds is the retention a read gives every table.
const readSeconds = 30

// narrowedReason is the knowledge a read of selected schemas states outside
// them: it lists the tables of those schemas only, so it knows their
// retention and nothing of another schema's.
const narrowedReason = "retention is read for the selected schemas only"

// Inspect reads the base fixture's tables and, when the program reads
// retention, gives each table a retention of readSeconds, with the table's
// own identity as the owner, so a value names the database it was read in.
// An unfiltered read knows retention completely. A read of selected schemas
// knows it completely only in the namespaces of their scope bindings, the
// shape a provider reports when it lists a namespace at a time.
func (p *hclClaims) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil || !readRetention() || snapshot.GetCoverage() == nil {
		return snapshot, err
	}
	model := backend.Description().GetEngines()[0].GetValueModels()[1]
	for _, table := range snapshot.GetTables() {
		table.Facets = append(table.Facets, &protocol.Envelope{ProviderId: "example.org/fixture", Kind: retentionKind, SchemaVersion: model.GetSchemaVersion(),
			Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: proto.CloneOf(table.GetIdentity()),
			Payload: fmt.Appendf(nil, `{"seconds":%d}`, readSeconds)})
		// The base fixture's normalization keeps an owner as it is.
		snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: retentionKind, Subject: proto.CloneOf(table.GetIdentity()),
			NormalizedSubject: proto.CloneOf(table.GetIdentity()), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	}
	kind := &protocol.ModelKindCoverage{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
	if len(snapshot.GetScopeBindings()) != 0 {
		kind.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: narrowedReason}
		for _, binding := range snapshot.GetScopeBindings() {
			for _, namespace := range binding.GetNamespaces() {
				kind.Regions = append(kind.Regions, &protocol.ModelNamespaceKnowledge{Scope: proto.CloneOf(namespace),
					Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
			}
		}
	}
	snapshot.Coverage.Kinds = append(snapshot.Coverage.Kinds, kind)
	return snapshot, nil
}

// requireReadRegions refuses an encoding request that does not give back the
// regions a read of selected schemas reported: wherever the request's
// knowledge of retention says the read was narrowed, it has to carry the one
// region of the main schema in the database the request's tables are bound
// in. A host that left the region out, or bound the tables without their
// database, would have the program write a source whose knowledge is not the
// read's.
func requireReadRegions(request *protocol.EncodeSourceRequest) error {
	if !readRetention() {
		return nil
	}
	databases := make(map[string]bool)
	for _, binding := range request.GetBindings() {
		databases[binding.GetObject().GetDatabase()] = true
	}
	for _, kind := range request.GetCoverage().GetKinds() {
		if kind.GetModel().GetKind() != retentionKind || kind.GetKnowledge().GetReason() != narrowedReason {
			continue
		}
		if len(databases) != 1 || len(kind.GetRegions()) != 1 {
			return fmt.Errorf("a narrowed read of retention has one region and binds its tables in one database")
		}
		var database string
		for name := range databases {
			database = name
		}
		want := &protocol.ModelNamespaceKnowledge{Scope: &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Source: database, Normalized: database},
			Catalog: &protocol.NamespaceComponent{}, Schema: &protocol.NamespaceComponent{}}, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
		if database == "" || !proto.Equal(kind.GetRegions()[0], want) {
			return fmt.Errorf("the region of a narrowed read of retention is not the one the read reported")
		}
	}
	return nil
}
