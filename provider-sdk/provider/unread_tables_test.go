package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// unreadInspection is an inspection whose provider, engine and target list
// UnreadTablesFeature, holding one unread table app.rep.
func unreadInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	description, request, snapshot := inspection()
	description.Features = append(description.Features, provider.UnreadTablesFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.UnreadTablesFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.UnreadTablesFeature)
	snapshot.UnreadTables = []*protocol.UnreadTable{unreadTable("rep")}
	return description, request, snapshot
}

func unreadTable(name string) *protocol.UnreadTable {
	return &protocol.UnreadTable{
		Table:      &protocol.Reference{Engine: "fixture", Database: "db", Schema: "app", Kind: "table", Name: name},
		Diagnostic: &protocol.ProviderDiagnostic{Reason: name + " is a replica table no replication writes any more"},
	}
}

// appScope filters the request to the schema app, bound to db.app.
func appScope(request *protocol.InspectRequest, snapshot *protocol.Schema) {
	request.Schemas = []string{"app"}
	snapshot.ScopeBindings = []*protocol.InspectionScopeBinding{{Selector: "app", Namespaces: []*protocol.ModelNamespace{{
		Database: &protocol.NamespaceComponent{Source: "db", Normalized: "db"}, Schema: &protocol.NamespaceComponent{Source: "app", Normalized: "app"},
	}}}}
	snapshot.NativeObjects = nil
	snapshot.Coverage = &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}
}

func TestInspectionUnreadTables_HappyPath(t *testing.T) {
	t.Run("unfiltered", func(t *testing.T) {
		c := qt.New(t)
		description, request, snapshot := unreadInspection()
		before := proto.CloneOf(snapshot)
		values, err := provider.InspectionValues(description, request, snapshot)
		c.Assert(err, qt.IsNil)
		c.Assert(values.GetValues(), qt.HasLen, 1)
		c.Assert(proto.Equal(snapshot, before), qt.IsTrue)
	})
	t.Run("within the requested scope", func(t *testing.T) {
		c := qt.New(t)
		description, request, snapshot := unreadInspection()
		appScope(request, snapshot)
		values, err := provider.InspectionValues(description, request, snapshot)
		c.Assert(err, qt.IsNil)
		c.Assert(values.GetValues(), qt.HasLen, 0)
	})
	t.Run("beside a described table and a reservation", func(t *testing.T) {
		c := qt.New(t)
		description, request, snapshot := unreadInspection()
		described := &protocol.Reference{Engine: "fixture", Database: "db", Schema: "app", Kind: "table", Name: "src"}
		snapshot.Tables = []*protocol.Table{{Identity: described, NormalizedIdentity: proto.CloneOf(described)}}
		snapshot.ReservedObjects = []*protocol.ReservedObject{reservedTable()}
		snapshot.UnreadTables = append(snapshot.UnreadTables, unreadTable("other"))
		_, err := provider.InspectionValues(description, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a reason of the longest length", func(t *testing.T) {
		c := qt.New(t)
		description, request, snapshot := unreadInspection()
		snapshot.UnreadTables[0].Diagnostic.Reason = strings.Repeat("r", 512)
		_, err := provider.InspectionValues(description, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
}

func TestInspectionUnreadTables_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		change  func(*protocol.Description, *protocol.InspectRequest, *protocol.Schema)
		wantErr string
	}{
		{"an engine that does not list the feature", func(d *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			d.Engines[0].Features = []string{"retention"}
			s.TargetFeatures = []string{"retention"}
		}, `.*unread tables: the feature was not negotiated`},
		{"a provider that does not list the feature", func(d *protocol.Description, _ *protocol.InspectRequest, _ *protocol.Schema) {
			d.Features = d.Features[:len(d.Features)-1]
		}, `.*unread tables: the feature was not negotiated`},
		{"a target that does not list the feature", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.TargetFeatures = []string{"retention"}
		}, `.*unread tables: the feature was not negotiated`},
		{"no table", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table = nil
		}, `.*an entry must name a common table of the read's engine`},
		{"another engine", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table.Engine = "other"
		}, `.*an entry must name a common table of the read's engine`},
		{"another kind", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table.Kind = "view"
		}, `.*an entry must name a common table of the read's engine`},
		{"a catalog", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table.Catalog = "catalog"
		}, `.*an entry must name a common table of the read's engine`},
		{"a path", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table.Path = []string{"nested"}
		}, `.*an entry must name a common table of the read's engine`},
		{"an overload", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table.Overload = []string{"integer"}
		}, `.*an entry must name a common table of the read's engine`},
		{"a name with white space around it", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Table.Name = " rep"
		}, `.*an entry must name a common table of the read's engine`},
		{"no reason", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Diagnostic = nil
		}, `.*an entry's reason must be one line a host may show`},
		{"a reason of two lines", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Diagnostic.Reason = "first\nsecond"
		}, `.*an entry's reason must be one line a host may show`},
		{"a reason above 512 bytes", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables[0].Diagnostic.Reason = strings.Repeat("r", 513)
		}, `.*an entry's reason must be one line a host may show`},
		{"a table named twice", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.UnreadTables = append(s.UnreadTables, unreadTable("rep"))
		}, `.*an entry names a table twice`},
		{"a table the read describes", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			table := s.UnreadTables[0].Table
			s.Tables = []*protocol.Table{{Identity: proto.CloneOf(table), NormalizedIdentity: proto.CloneOf(table)}}
		}, `.*an entry names a table the read describes`},
		{"a table the read reserves", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.ReservedObjects = []*protocol.ReservedObject{reservedTable()}
			s.UnreadTables[0].Table.Name = "history"
		}, `.*an entry names a table the read reserves`},
		{"too many entries", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			for range 4096 {
				s.UnreadTables = append(s.UnreadTables, unreadTable("rep"))
			}
		}, `.*unread tables: too many entries`},
		{"outside the requested scope", func(_ *protocol.Description, r *protocol.InspectRequest, s *protocol.Schema) {
			appScope(r, s)
			s.UnreadTables[0].Table.Schema = "outside"
		}, `.*unread table is outside the requested scope`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, snapshot := unreadInspection()
			row.change(description, request, snapshot)
			values, err := provider.InspectionValues(description, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

// A host that does not list the feature removes it from the negotiated
// description, so a snapshot carrying an entry is refused at both
// boundaries, which validate with that copy.
func TestInspectionUnreadTables_UnlistedHostFeature(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := unreadInspection()
	negotiated := provider.NegotiatedDescription(description, nil)
	c.Assert(negotiated.GetFeatures(), qt.Not(qt.Contains), provider.UnreadTablesFeature)
	values, err := provider.InspectionValues(negotiated, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(values, qt.IsNil)
	listed := provider.NegotiatedDescription(description, []string{provider.UnreadTablesFeature})
	values, err = provider.InspectionValues(listed, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.IsNotNil)
}

func TestDescriptionUnreadTables_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, _, _ := unreadInspection()
	description.Features = description.Features[:len(description.Features)-1]
	err := provider.ValidateDescription(description, nil)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*engine schema\.unread-tables requires the provider's`)
}
