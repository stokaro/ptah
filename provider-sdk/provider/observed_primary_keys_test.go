package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func primaryKeyInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := columnInspection()
	d.Features = append(d.Features, provider.ObservedPrimaryKeysFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedPrimaryKeysFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedPrimaryKeysFeature)
	table := snapshot.Tables[0]
	table.Columns = append(table.Columns, &protocol.Column{Name: "tenant", Observed: &protocol.ObservedColumn{
		Name: "tenant", DataType: "bigint", IsNullable: "NO", OrdinalPosition: 3, PrimaryKey: true,
	}})
	table.PrimaryKey = &protocol.ObservedConstraint{
		Name: "items_pk", TableName: "items", Schema: "app", Type: "PRIMARY KEY", ColumnNames: []string{"tenant", "id"},
		IncludeColumns: []string{"label"}, UsingMethod: new("btree"), Comment: "Tenant order", Deferrable: true, Initially: "DEFERRED",
	}
	return d, request, snapshot
}

func TestObservedPrimaryKeyPreservesOrderAndAttributes(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := primaryKeyInspection()
	before := proto.CloneOf(snapshot)
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(snapshot, before), qt.IsTrue)
	data, err := proto.Marshal(snapshot)
	c.Assert(err, qt.IsNil)
	decoded := &protocol.Schema{}
	c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
	_, err = provider.InspectionValues(d, request, decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(decoded.Tables[0].PrimaryKey, before.Tables[0].PrimaryKey), qt.IsTrue)
	c.Assert(decoded.Tables[0].PrimaryKey.ColumnNames, qt.DeepEquals, []string{"tenant", "id"})
	decoded.Tables[0].PrimaryKey.ColumnNames[0] = "changed"
	*decoded.Tables[0].PrimaryKey.UsingMethod = "changed"
	c.Assert(proto.Equal(snapshot, before), qt.IsTrue)
}

func TestObservedPrimaryKeyRequiresCompleteNegotiatedRecord(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"provider opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedPrimaryKeysFeature })
		}},
		{"engine opt out", func(d *protocol.Description, _ *protocol.Schema) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ObservedPrimaryKeysFeature })
		}},
		{"host opt out", func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.ObservedPrimaryKeysFeature })
		}},
		{"missing record", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey = nil }},
		{"empty snapshot without column negotiation", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables = nil
			s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.ObservedColumnsFeature })
		}},
		{"another table", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.TableName = "other" }},
		{"another schema", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.Schema = "other" }},
		{"another type", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.Type = "UNIQUE" }},
		{"empty key", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.ColumnNames = nil }},
		{"singular alias", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.ColumnName = "tenant" }},
		{"unknown member", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].PrimaryKey.ColumnNames = append(s.Tables[0].PrimaryKey.ColumnNames, "missing")
		}},
		{"repeated member", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].PrimaryKey.ColumnNames = append(s.Tables[0].PrimaryKey.ColumnNames, "tenant")
		}},
		{"unreported member", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[0].Observed.PrimaryKey = false }},
		{"extra member flag", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].Columns[1].Observed.PrimaryKey = true }},
		{"unknown included column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].PrimaryKey.IncludeColumns = []string{"missing"}
		}},
		{"repeated included column", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].PrimaryKey.IncludeColumns = []string{"label", "label"}
		}},
		{"included key member", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].PrimaryKey.IncludeColumns = []string{"id"}
		}},
		{"foreign table", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.ForeignTable = new("other") }},
		{"empty check clause", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.CheckClause = new("") }},
		{"unique null semantics", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.NullsDistinct = new(false) }},
		{"dependency", func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].PrimaryKey.Requires = []*protocol.ComparisonIdentity{{Kind: "table"}}
		}},
		{"unknown initial mode", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.Initially = "UNKNOWN" }},
		{"mode without deferral", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.Deferrable = false }},
		{"empty method", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.UsingMethod = new("") }},
		{"negative shards", func(_ *protocol.Description, s *protocol.Schema) { s.Tables[0].PrimaryKey.HashShardBuckets = -1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := primaryKeyInspection()
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.IsNil)
			test.mutate(d, snapshot)
			_, err = provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestObservedPrimaryKeyAbsenceIsExplicit(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := primaryKeyInspection()
	snapshot.Tables[0].PrimaryKey = nil
	for _, column := range snapshot.Tables[0].Columns {
		column.Observed.PrimaryKey = false
	}
	_, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
}

func TestObservedPrimaryKeyIsNotACreationOperand(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := operationRender()
	table := &protocol.Table{Identity: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "items"}}
	request.Changes = []*protocol.Change{{Id: "create", Object: proto.CloneOf(table.Identity), Operation: &protocol.Change_CreateTable{CreateTable: table}}}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	table.PrimaryKey = &protocol.ObservedConstraint{Type: "PRIMARY KEY", TableName: "items", ColumnNames: []string{"id"}}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
}

func TestObservedPrimaryKeyDescriptionRequiresColumnSupport(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"provider primary-key feature", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedPrimaryKeysFeature })
		}},
		{"provider columns", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ObservedColumnsFeature })
		}},
		{"engine columns", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ObservedColumnsFeature })
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := primaryKeyInspection()
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
