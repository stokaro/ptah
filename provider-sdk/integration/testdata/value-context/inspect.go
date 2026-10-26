package main

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func (p *contextOwner) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, found := p.contexts.Load(request.SessionId)
	if !found {
		return nil, fmt.Errorf("unknown fixture session")
	}
	bound := raw.(*protocol.DatabaseContext)
	engine := p.description.Engines[0]
	model := engine.ValueModels[1]
	identity := &protocol.Reference{Engine: engine.Id, Database: bound.Database, Schema: "main", Kind: "view", Name: "active"}
	facet := &protocol.Envelope{ProviderId: p.description.ProviderId, Kind: model.Kind, SchemaVersion: model.SchemaVersion,
		Encoding: model.Encoding, DefinitionSha256: model.DefinitionSha256, Owner: proto.CloneOf(identity), Context: proto.CloneOf(bound), Payload: []byte(`{"seconds":60}`)}
	complete := &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
	table := proto.CloneOf(identity)
	table.Kind, table.Name = "table", "items"
	return &protocol.Schema{Complete: true, Engine: engine.Id, TargetFeatures: slices.Clone(engine.Features),
		Tables: []*protocol.Table{{Identity: table, NormalizedIdentity: proto.CloneOf(table), Columns: []*protocol.Column{
			{Name: "id", Observed: &protocol.ObservedColumn{Name: "id", OrdinalPosition: 1, DataType: "INTEGER", IsNullable: "NO", PrimaryKey: true,
				AutoIncrement: true, IdentityStart: "7", IdentityIncrement: "3", SequenceRestart: "42", OwnedSequence: "items_id_seq"}},
			{Name: "label", Observed: &protocol.ObservedColumn{Name: "label", OrdinalPosition: 2, DataType: "TEXT", IsNullable: "YES", DefaultExpression: new("")}},
		}}},
		CommonObjects: &protocol.ObservedCommonObjects{Views: []*protocol.ObservedView{{Identity: identity, Body: "SELECT 1", Facets: []*protocol.Envelope{facet}}},
			Coverage: []*protocol.CommonObjectCoverage{{Kind: protocol.CommonObjectSupport_VIEW, Knowledge: proto.CloneOf(complete)}}},
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED,
			Kinds:    []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: proto.CloneOf(complete)}},
			Subjects: []*protocol.ModelSubjectCoverage{{Subject: proto.CloneOf(identity), NormalizedSubject: proto.CloneOf(identity), Kind: model.Kind, Knowledge: proto.CloneOf(complete)}},
		},
	}, nil
}
