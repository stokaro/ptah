package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func columnExchange() (*protocol.Description, *protocol.CompareColumnsRequest, *protocol.ComparedColumns) {
	d, preparation, _ := preparationExchange()
	d.Features = append(d.Features, provider.ColumnsFeature, provider.ProjectTableCreationsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ColumnsFeature, provider.ProjectTableCreationsFeature)
	table := preparation.Tables[0]
	table.Current.Columns = []*protocol.ObservedColumn{{Name: "old_id", DataType: "INTEGER", IsNullable: "NO"}}
	column := proto.CloneOf(table.Subject)
	column.Kind, column.Parent = "column", proto.CloneOf(table.Subject.Name)
	column.Name = &protocol.ComparisonPart{Source: "id", Normalized: "id"}
	current := proto.CloneOf(column)
	current.Name = &protocol.ComparisonPart{Source: "old_id", Normalized: "old_id"}
	pair := &protocol.ColumnComparisonPair{Subject: column, CurrentSubject: current, DesiredColumn: "id", CurrentColumn: "old_id",
		Aspects: []protocol.ColumnAspect{protocol.ColumnAspect_COLUMN_TYPE, protocol.ColumnAspect_COLUMN_DEFAULT, protocol.ColumnAspect_COLUMN_NULLABILITY, protocol.ColumnAspect_COLUMN_SEQUENCE}}
	request := &protocol.CompareColumnsRequest{Engine: preparation.Engine, TargetFeatures: preparation.TargetFeatures, Identifiers: preparation.Identifiers,
		Capabilities: preparation.Capabilities, Tables: []*protocol.ColumnComparisonTable{{Subject: table.Subject, CurrentSubject: proto.CloneOf(table.Subject),
			Desired: table.Desired, Current: table.Current, Pairs: []*protocol.ColumnComparisonPair{pair}}}}
	result := &protocol.ComparedColumns{Complete: true, Tables: []*protocol.ComparedColumnTable{{Subject: proto.CloneOf(table.Subject), CurrentSubject: proto.CloneOf(table.Subject),
		Columns: []*protocol.ComparedColumn{{Subject: proto.CloneOf(column), CurrentSubject: proto.CloneOf(current), Aspects: []*protocol.ColumnAspectReceipt{
			{Aspect: protocol.ColumnAspect_COLUMN_TYPE, Receipt: &protocol.ColumnAspectReceipt_Difference{Difference: &protocol.ColumnDifference{Relation: protocol.ColumnDifference_SAME}}},
			{Aspect: protocol.ColumnAspect_COLUMN_DEFAULT, Receipt: &protocol.ColumnAspectReceipt_Difference{Difference: &protocol.ColumnDifference{Relation: protocol.ColumnDifference_CHANGED, Desired: "0"}}},
			{Aspect: protocol.ColumnAspect_COLUMN_NULLABILITY, Receipt: &protocol.ColumnAspectReceipt_Nullability{Nullability: &protocol.ColumnNullability{Nullable: new(false)}}},
			{Aspect: protocol.ColumnAspect_COLUMN_SEQUENCE, Receipt: &protocol.ColumnAspectReceipt_Sequence{Sequence: &protocol.ColumnSequenceDifferences{}}},
		}}}}}}
	return d, request, result
}

func TestColumnComparisonPreservesCapturesAndExplicitFacts(t *testing.T) {
	c := qt.New(t)
	d, request, result := columnExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateComparedColumns(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(result.Tables[0].Columns[0].Aspects[2].GetNullability().Nullable, qt.DeepEquals, new(false))
	request.Tables[0].Current.Name = "renamed_from"
	request.Tables[0].CurrentSubject.Name = &protocol.ComparisonPart{Source: "renamed_from", Normalized: "renamed_from"}
	request.Tables[0].Pairs[0].CurrentSubject.Parent = proto.CloneOf(request.Tables[0].CurrentSubject.Name)
	result.Tables[0].CurrentSubject = proto.CloneOf(request.Tables[0].CurrentSubject)
	result.Tables[0].Columns[0].CurrentSubject = proto.CloneOf(request.Tables[0].Pairs[0].CurrentSubject)
	c.Assert(provider.ValidateComparedColumns(d, request, result), qt.IsNil)
	request.Tables, result.Tables = nil, nil
	c.Assert(provider.ValidateComparedColumns(d, request, result), qt.IsNil)
}

func TestColumnComparisonRejectsUnboundCaptures(t *testing.T) {
	for _, row := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.CompareColumnsRequest)
	}{
		{"provider feature", func(d *protocol.Description, _ *protocol.CompareColumnsRequest) {
			d.Features = slices.DeleteFunc(d.Features, func(s string) bool { return s == provider.ColumnsFeature })
		}},
		{"engine feature", func(d *protocol.Description, _ *protocol.CompareColumnsRequest) { d.Engines[0].Features = nil }},
		{"creation service", func(d *protocol.Description, _ *protocol.CompareColumnsRequest) {
			d.Features = slices.DeleteFunc(d.Features, func(s string) bool { return s == provider.ProjectTableCreationsFeature })
		}},
		{"empty engine", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Engine = "" }},
		{"missing identifier rules", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Identifiers = nil }},
		{"missing table", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0] = nil }},
		{"unknown current type", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Current.Columns[0].DataType = ""
		}},
		{"unknown current nullable", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Current.Columns[0].IsNullable = ""
		}},
		{"missing current", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Current = nil }},
		{"missing declaration", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Desired = nil }},
		{"missing current features", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Current.Features = nil }},
		{"missing declared features", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Desired.Features = nil }},
		{"invalid declared type", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Desired.Columns[0].Type = ""
		}},
		{"duplicate current column", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Current.Columns = append(r.Tables[0].Current.Columns, proto.CloneOf(r.Tables[0].Current.Columns[0]))
		}},
		{"current table identity", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Current.Name = "other" }},
		{"normalized parent", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Subject.Name.Normalized = "other"
		}},
		{"normalized column", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Pairs[0].Subject.Name.Normalized = "other"
		}},
		{"current parent boundary", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Pairs[0].CurrentSubject.Parent.Normalized = "other"
		}},
		{"current database boundary", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Pairs[0].CurrentSubject.Database = &protocol.ComparisonPart{Source: "other", Normalized: "other"}
		}},
		{"unbound current name", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Pairs[0].CurrentColumn = "id"
		}},
		{"duplicate pair", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Pairs = append(r.Tables[0].Pairs, proto.CloneOf(r.Tables[0].Pairs[0]))
		}},
		{"duplicate table", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
		{"no aspects", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Pairs[0].Aspects = nil }},
		{"unknown aspect", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) { r.Tables[0].Pairs[0].Aspects[0] = 99 }},
		{"duplicate aspect", func(_ *protocol.Description, r *protocol.CompareColumnsRequest) {
			r.Tables[0].Pairs[0].Aspects[0] = protocol.ColumnAspect_COLUMN_DEFAULT
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := columnExchange()
			c.Assert(provider.ValidateColumnsRequest(d, request), qt.IsNil)
			row.mutate(d, request)
			c.Assert(provider.ValidateColumnsRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestColumnComparisonRejectsPartialOrForgedReceipts(t *testing.T) {
	for _, row := range []struct {
		name   string
		mutate func(*protocol.ComparedColumns)
	}{
		{"incomplete", func(r *protocol.ComparedColumns) { r.Complete = false }},
		{"missing table", func(r *protocol.ComparedColumns) { r.Tables = nil }},
		{"missing pair", func(r *protocol.ComparedColumns) { r.Tables[0].Columns = nil }},
		{"changed current identity", func(r *protocol.ComparedColumns) { r.Tables[0].Columns[0].CurrentSubject.Name.Source = "other" }},
		{"missing aspect", func(r *protocol.ComparedColumns) { r.Tables[0].Columns[0].Aspects = r.Tables[0].Columns[0].Aspects[:3] }},
		{"reordered aspect", func(r *protocol.ComparedColumns) { slices.Reverse(r.Tables[0].Columns[0].Aspects) }},
		{"missing relation", func(r *protocol.ComparedColumns) { r.Tables[0].Columns[0].Aspects[0].GetDifference().Relation = 0 }},
		{"missing type spelling", func(r *protocol.ComparedColumns) {
			r.Tables[0].Columns[0].Aspects[0].GetDifference().Relation = protocol.ColumnDifference_CHANGED
		}},
		{"missing nullable", func(r *protocol.ComparedColumns) { r.Tables[0].Columns[0].Aspects[2].GetNullability().Nullable = nil }},
		{"wrong receipt type", func(r *protocol.ComparedColumns) {
			r.Tables[0].Columns[0].Aspects[2].Receipt = r.Tables[0].Columns[0].Aspects[0].Receipt
		}},
		{"missing sequence", func(r *protocol.ComparedColumns) { r.Tables[0].Columns[0].Aspects[3].Receipt = nil }},
		{"arbitrary sequence attribute", func(r *protocol.ComparedColumns) {
			r.Tables[0].Columns[0].Aspects[3].GetSequence().Changes = []*protocol.ColumnSequenceDifference{{Attribute: 99, Desired: "5"}}
		}},
		{"duplicate sequence attribute", func(r *protocol.ComparedColumns) {
			r.Tables[0].Columns[0].Aspects[3].GetSequence().Changes = []*protocol.ColumnSequenceDifference{{Attribute: protocol.ColumnSequenceDifference_IDENTITY_START, Desired: "5"}, {Attribute: protocol.ColumnSequenceDifference_IDENTITY_START, Desired: "6"}}
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := columnExchange()
			c.Assert(provider.ValidateComparedColumns(d, request, result), qt.IsNil)
			row.mutate(result)
			c.Assert(provider.ValidateComparedColumns(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestColumnComparisonKeepsDefaultSchemaAndRequestedAspectOrder(t *testing.T) {
	for _, aspects := range [][]protocol.ColumnAspect{
		{protocol.ColumnAspect_COLUMN_TYPE},
		{protocol.ColumnAspect_COLUMN_DEFAULT},
		{protocol.ColumnAspect_COLUMN_NULLABILITY},
		{protocol.ColumnAspect_COLUMN_SEQUENCE},
		{protocol.ColumnAspect_COLUMN_DEFAULT, protocol.ColumnAspect_COLUMN_TYPE},
		{protocol.ColumnAspect_COLUMN_NULLABILITY, protocol.ColumnAspect_COLUMN_SEQUENCE},
	} {
		t.Run(aspects[0].String(), func(t *testing.T) {
			c := qt.New(t)
			d, request, result := columnExchange()
			request.Identifiers.DefaultSchema = "a.b"
			table := request.Tables[0]
			table.Desired.Schema, table.Current.Schema = "", ""
			for _, subject := range []*protocol.ComparisonIdentity{table.Subject, table.CurrentSubject, table.Pairs[0].Subject, table.Pairs[0].CurrentSubject,
				result.Tables[0].Subject, result.Tables[0].CurrentSubject, result.Tables[0].Columns[0].Subject, result.Tables[0].Columns[0].CurrentSubject} {
				subject.Schema.Defaulted = true
			}
			all := result.Tables[0].Columns[0].Aspects
			table.Pairs[0].Aspects, result.Tables[0].Columns[0].Aspects = aspects, nil
			for _, aspect := range aspects {
				result.Tables[0].Columns[0].Aspects = append(result.Tables[0].Columns[0].Aspects, all[int(aspect)-1])
			}
			c.Assert(provider.ValidateComparedColumns(d, request, result), qt.IsNil)
			table.CurrentSubject.Schema.Normalized = "foreign"
			c.Assert(provider.ValidateColumnsRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
