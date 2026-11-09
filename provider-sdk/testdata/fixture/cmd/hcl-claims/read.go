package main

import (
	"context"
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// readRetention reports whether a read gives every table a retention, which
// FIXTURE_READ_RETENTION=1 selects. Without it the program reads no value of
// its own, as the base fixture does.
func readRetention() bool { return os.Getenv("FIXTURE_READ_RETENTION") == "1" }

// readHidden reports whether a read of selected schemas also reports a
// nested namespace of the main schema it did not read, which
// FIXTURE_READ_HIDDEN=1 selects.
func readHidden() bool { return os.Getenv("FIXTURE_READ_HIDDEN") == "1" }

// hiddenReason is the knowledge of the nested namespace a read does not read.
const hiddenReason = "the nested namespace was not read"

// readRegions are the regions a read of the main schema in database reports:
// the schema it listed, complete, and the nested namespace it did not read.
func readRegions(database string) (listed, hidden *protocol.ModelNamespaceKnowledge) {
	scope := &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Source: database, Normalized: database}, Catalog: &protocol.NamespaceComponent{},
		Schema: &protocol.NamespaceComponent{}}
	listed = &protocol.ModelNamespaceKnowledge{Scope: scope, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
	nested := proto.CloneOf(scope)
	nested.PathPrefix = []*protocol.NamespaceComponent{{Source: "hidden", Normalized: "hidden"}}
	hidden = &protocol.ModelNamespaceKnowledge{Scope: nested, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: hiddenReason}}
	return listed, hidden
}

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
// shape a provider reports when it lists a namespace at a time, and with
// readHidden not in a nested namespace of them. With readUnread it also
// records the legacy object it does not describe (see recordUnread).
func (p *hclClaims) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil || snapshot.GetCoverage() == nil {
		return snapshot, err
	}
	if readUnread() {
		recordUnread(snapshot)
	}
	if !readRetention() {
		return snapshot, nil
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
		if readHidden() {
			_, hidden := readRegions(snapshot.GetTables()[0].GetIdentity().GetDatabase())
			kind.Regions = append(kind.Regions, hidden)
		}
	}
	snapshot.Coverage.Kinds = append(snapshot.Coverage.Kinds, kind)
	return snapshot, nil
}

// requireReadRegions refuses an encoding request whose knowledge of retention
// is not the one a host owes a source written from the program's read. A read
// of selected schemas knows retention completely in the namespaces it listed,
// so the source is complete for the selection it is applied to: the request
// carries retention as complete, without the read's regions. A read that also
// reports a nested namespace it did not read (readHidden) knows less there, and
// the request carries both regions as the read reported them, in the database
// the request's tables are bound in.
func requireReadRegions(request *protocol.EncodeSourceRequest) error {
	if !readRetention() {
		return nil
	}
	databases := make(map[string]bool)
	for _, binding := range request.GetBindings() {
		databases[binding.GetObject().GetDatabase()] = true
	}
	for _, kind := range request.GetCoverage().GetKinds() {
		if kind.GetModel().GetKind() != retentionKind {
			continue
		}
		if !readHidden() {
			if len(kind.GetRegions()) != 0 || kind.GetKnowledge().GetReason() == narrowedReason {
				return fmt.Errorf("a read that knows retention in every namespace it listed writes a source complete for its selection")
			}
			continue
		}
		if len(databases) != 1 || kind.GetKnowledge().GetReason() != narrowedReason {
			return fmt.Errorf("a narrowed read of retention binds its tables in one database and knows nothing outside its regions")
		}
		var database string
		for name := range databases {
			database = name
		}
		listed, hidden := readRegions(database)
		if database == "" || len(kind.GetRegions()) != 2 || !slices.ContainsFunc(kind.GetRegions(), func(region *protocol.ModelNamespaceKnowledge) bool { return proto.Equal(region, listed) }) ||
			!slices.ContainsFunc(kind.GetRegions(), func(region *protocol.ModelNamespaceKnowledge) bool { return proto.Equal(region, hidden) }) {
			return fmt.Errorf("the regions of a narrowed read of retention are not the ones the read reported")
		}
	}
	return nil
}
