package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"

	"google.golang.org/protobuf/proto"

	"ptah.run/provider-sdk/modeljson"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// legacyKind is a model of objects no HCL block of the program writes, which
// a read lists and may not describe.
const legacyKind = "example.org/fixture/legacy"

// legacyReason is the knowledge of the legacy object a read lists and does
// not describe.
const legacyReason = "a legacy object, which the fixture does not read"

// readUnread reports whether the engine describes the legacy model and a read
// records the legacy object audit as one it lists and does not describe,
// which FIXTURE_READ_UNREAD=1 selects.
func readUnread() bool { return os.Getenv("FIXTURE_READ_UNREAD") == "1" }

// legacyModels are the desired and observed legacy models. No source format
// declares one: a read is the only place a legacy object comes from.
func legacyModels() []*protocol.ValueModel {
	definition, err := modeljson.Canonical([]byte(`{"encoding":"json","kind":"` + legacyKind + `","value":"an object the fixture lists and never describes","schema_version":1}`))
	if err != nil {
		panic(err)
	} // Static owner definition; invalid JSON is a fixture programming error.
	digest := sha256.Sum256(definition)
	var models []*protocol.ValueModel
	for _, representation := range []protocol.ValueInput_Representation{protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED} {
		models = append(models, &protocol.ValueModel{DisplayName: "legacy objects", Kind: legacyKind, Representation: representation,
			SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]),
			CanonicalFormat: legacyKind + "/json-v1"})
	}
	return models
}

// legacyNarrowedReason is the knowledge of the legacy model outside the
// schemas a read of selected schemas lists.
const legacyNarrowedReason = "legacy objects are listed for the selected schemas only"

// recordUnread states the read's knowledge of the legacy model in snapshot,
// complete where the read listed, less the object audit beside the read's
// first table, which the read lists and does not describe. A read of no
// table states nothing.
func recordUnread(snapshot *protocol.Schema) {
	if len(snapshot.GetTables()) == 0 {
		return
	}
	var observed *protocol.ValueModel
	for _, model := range legacyModels() {
		if model.GetRepresentation() == protocol.ValueInput_OBSERVED {
			observed = model
		}
	}
	kind := &protocol.ModelKindCoverage{Model: observed, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
	if len(snapshot.GetScopeBindings()) != 0 {
		kind.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: legacyNarrowedReason}
		for _, binding := range snapshot.GetScopeBindings() {
			for _, namespace := range binding.GetNamespaces() {
				kind.Regions = append(kind.Regions, &protocol.ModelNamespaceKnowledge{Scope: proto.CloneOf(namespace),
					Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
			}
		}
	}
	table := snapshot.GetTables()[0].GetIdentity()
	subject := &protocol.Reference{Engine: "fixture", Database: table.GetDatabase(), Schema: table.GetSchema(), Kind: legacyKind, Name: "audit"}
	snapshot.Coverage.Kinds = append(snapshot.Coverage.Kinds, kind)
	snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: legacyKind, Subject: subject, NormalizedSubject: proto.CloneOf(subject),
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: legacyReason}})
}
