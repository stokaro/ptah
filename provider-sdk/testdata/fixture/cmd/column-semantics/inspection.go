package main

import (
	"context"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func (c *columns) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := c.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Features = []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature}
	if provider.EngineOffers(provider.NegotiatedDescription(c.description, provider.HostFeatures(ctx)), "fixture", provider.ObservedIndexFacetsFeature) {
		session.Features = append(session.Features, provider.ObservedIndexFacetsFeature)
	}
	return session, nil
}

func (c *columns) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := c.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.TargetFeatures = []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature}
	if provider.EngineOffers(provider.NegotiatedDescription(c.description, provider.HostFeatures(ctx)), "fixture", provider.ObservedIndexFacetsFeature) {
		snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedIndexFacetsFeature)
	}
	for _, table := range snapshot.Tables {
		switch table.Identity.Name {
		case "index_facet":
			// The index carries a retention of 30 seconds as its own facet,
			// owned by the index, as a provider reports an index's settings.
			index := &protocol.ObservedIndex{Name: "index_facet_id", TableName: table.Identity.Name, Schema: table.Identity.Schema, Columns: []string{"Id"}}
			facet := &protocol.Envelope{ProviderId: "example.org/fixture", Kind: "example.org/fixture/retention", SchemaVersion: 1, Encoding: "json",
				DefinitionSha256: c.retention.GetDefinitionSha256(), Owner: provider.IndexReference(table.Identity, index.Name), Payload: []byte(`{"seconds":30}`)}
			index.Facets = []*protocol.Envelope{facet}
			table.Indexes = []*protocol.ObservedIndex{index}
			snapshot.Coverage.Kinds = append(snapshot.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(c.retention),
				Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the read describes the index it reports"}})
			snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: facet.Kind, Subject: proto.CloneOf(facet.Owner),
				NormalizedSubject: proto.CloneOf(facet.Owner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		case "column_indexed":
			table.Indexes = []*protocol.ObservedIndex{{Name: "column_indexed_id", TableName: table.Identity.Name, Schema: table.Identity.Schema,
				Columns: []string{"Id"}, Unique: true, Method: "global", Comment: "captured index"}}
		case "index_column":
			table.Indexes = []*protocol.ObservedIndex{{Name: "index_column_missing", TableName: table.Identity.Name, Schema: table.Identity.Schema, Columns: []string{"missing"}}}
		}
		for i, column := range table.Columns {
			nullable := "NO"
			if column.Nullable {
				nullable = "YES"
			}
			observed := &protocol.ObservedColumn{Name: column.Name, DataType: column.Type, FormattedType: column.Type, IsNullable: nullable, PrimaryKey: column.PrimaryKey, DefaultExpression: column.DefaultExpression, OrdinalPosition: int64(i + 1)}
			column.Observed = observed
			column.Type = ""
			column.Nullable = false
			column.PrimaryKey = false
			column.DefaultExpression = nil
			switch table.Identity.Name {
			case "column_metadata":
				observed.DataType = "Int64"
				observed.FormattedType = "INT64"
				observed.ColumnType = "signed integer"
				observed.AutoIncrement = true
				observed.IdentityGeneration = "BY DEFAULT"
				observed.IdentityStart = "5"
				observed.IdentityIncrement = "2"
				observed.SequenceRestart = "11"
				observed.OwnedSequence = "private_sequence"
				observed.Comment = "captured column"
				observed.NotNullConstraintName = "id_present"
				observed.Unique = true
				observed.NumericPrecision = new(int64(19))
				observed.NumericScale = new(int64(0))
				observed.DatetimePrecision = new(int64(6))
				observed.CharacterMaxLength = new(int64(42))
				observed.UdtName = "int8"
				observed.UdtSchema = "types"
				observed.DomainName = "identifier"
				observed.DomainSchema = "domain_schema"
				observed.Collation = "case_sensitive"
				observed.TypeIsDeclaredText = true
			case "column_generated":
				observed.GeneratedExpression = new("lower(source)")
				observed.GeneratedKind = "STORED"
				observed.DefaultExpression = new("typed()")
			case "column_missing":
				column.Observed = nil
			case "column_legacy":
				column.Type = "INTEGER"
			case "column_name":
				observed.Name = "foreign"
			case "column_ordinal":
				observed.OrdinalPosition = 2
			case "column_nullable":
				observed.IsNullable = ""
			}
		}
	}
	return snapshot, nil
}
