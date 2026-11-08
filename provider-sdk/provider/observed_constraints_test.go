package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// constraintInspection is a read negotiated with observed constraints of a
// table that holds a foreign key and a CHECK.
func constraintInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := columnInspection()
	d.Features = append(d.Features, provider.ObservedConstraintsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedConstraintsFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedConstraintsFeature)
	snapshot.Tables[0].Constraints = []*protocol.ObservedConstraint{
		{Name: "items_owner_fk", TableName: "items", Schema: "app", Type: "FOREIGN KEY", ColumnNames: []string{"id"},
			ForeignTable: new("owners"), ForeignSchema: "app", ForeignColumns: []string{"id"}, DeleteRule: new("CASCADE"), UpdateRule: new("NO ACTION")},
		{Name: "items_label_check", TableName: "items", Schema: "app", Type: "CHECK", ColumnNames: []string{"label"}, CheckClause: new("(label <> '')")},
	}
	return d, request, snapshot
}

func TestObservedConstraints_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a negotiated read reports its constraints and they survive the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := constraintInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
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
		c.Assert(proto.Equal(decoded.Tables[0].Constraints[0], before.Tables[0].Constraints[0]), qt.IsTrue)
		c.Assert(decoded.Tables[0].Constraints[1].GetCheckClause(), qt.Equals, "(label <> '')")
	})
	t.Run("a table that lists no constraint holds none", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := constraintInspection()
		snapshot.Tables[0].Constraints = nil
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a foreign key without rules, MATCH type or deferral is complete", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := constraintInspection()
		key := snapshot.Tables[0].Constraints[0]
		key.DeleteRule, key.UpdateRule, key.ForeignSchema = nil, nil, ""
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a deferrable foreign key with MATCH FULL is complete", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := constraintInspection()
		key := snapshot.Tables[0].Constraints[0]
		key.Deferrable, key.Initially, key.Match = true, "DEFERRED", "FULL"
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := constraintInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.ObservedConstraintsFeature))
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.ObservedConstraintsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ObservedConstraintsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ObservedColumnsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ObservedConstraintsFeature)
	})
}

// A read that did not negotiate constraints reports none, and a negotiated
// one reports each constraint whole: named once, bound to its table, of a
// type the contract carries, over the table's own columns, and with the
// attributes of its type only.
func TestObservedConstraints_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"the provider did not list the feature", `.*observed constraints were not negotiated`, func(d *protocol.Description, _ *protocol.Schema) {
			d.Features = withoutFeature(d.Features, provider.ObservedConstraintsFeature)
		}},
		{"the host did not list the feature", `.*observed constraints were not negotiated`, func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = withoutFeature(s.TargetFeatures, provider.ObservedConstraintsFeature)
		}},
		{"columns were not negotiated", `.*column observations.*`, func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = withoutFeature(s.TargetFeatures, provider.ObservedColumnsFeature)
		}},
		{"unnamed", `.*requires a name unique within its table`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].Name = ""
		}},
		{"named twice", `.*requires a name unique within its table`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].Name = "items_owner_fk"
		}},
		{"another table", `.*has an invalid table binding`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].TableName = "other"
		}},
		{"another schema", `.*has an invalid table binding`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].Schema = "other"
		}},
		{"a primary key", `.*is neither a FOREIGN KEY nor a CHECK`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].Type = "PRIMARY KEY"
		}},
		{"a unique constraint", `.*is neither a FOREIGN KEY nor a CHECK`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].Type = "UNIQUE"
		}},
		{"the singular column field", `.*lists them in column_names`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].ColumnName = "id"
		}},
		{"an unknown column", `.*names an unknown or repeated column`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].ColumnNames = []string{"missing"}
		}},
		{"a repeated column", `.*names an unknown or repeated column`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].ColumnNames = []string{"label", "label"}
		}},
		{"index attributes", `.*carries index attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].IncludeColumns = []string{"id"}
		}},
		{"a foreign key without columns", `.*requires its columns, the referenced table and as many referenced columns`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].ColumnNames = nil
		}},
		{"a foreign key without its table", `.*requires its columns, the referenced table and as many referenced columns`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].ForeignTable = nil
		}},
		{"a foreign key with fewer referenced columns", `.*requires its columns, the referenced table and as many referenced columns`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].ForeignColumns = nil
		}},
		{"a foreign key with the singular referenced column", `.*requires its columns, the referenced table and as many referenced columns`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].ForeignColumn = new("id")
		}},
		{"a foreign key with a CHECK clause", `.*is a foreign key with a CHECK clause`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].CheckClause = new("(id > 0)")
		}},
		{"a rule in another spelling", `.*has rule "cascade"; a rule is one of NO ACTION, RESTRICT, CASCADE, SET NULL, SET DEFAULT`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].DeleteRule = new("cascade")
		}},
		{"an unknown update rule", `.*has rule "NOACTION".*`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].UpdateRule = new("NOACTION")
		}},
		{"MATCH SIMPLE spelled out", `.*has MATCH type "SIMPLE"; it is FULL, PARTIAL or empty for SIMPLE`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].Match = "SIMPLE"
		}},
		{"a timing without deferral", `.*has invalid deferral`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[0].Initially = "DEFERRED"
		}},
		{"a CHECK without its expression", `.*is a CHECK without its expression`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].CheckClause = new(" ")
		}},
		{"a CHECK with a referenced table", `.*is a CHECK with foreign key attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].ForeignTable = new("owners")
		}},
		{"a deferrable CHECK", `.*is a CHECK with foreign key attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].Deferrable = true
		}},
		{"an invalid requirement", `.*comparison identity requires a kind and name`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[1].Requires = []*protocol.ComparisonIdentity{{Kind: "constraint"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := constraintInspection()
			test.mutate(d, snapshot)
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A provider and its engine list the feature together, and the engine reports
// its columns, against which the SDK checks each constraint.
func TestObservedConstraintsDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.ObservedConstraintsFeature)
		}},
		{"without column observations", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ObservedColumnsFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := constraintInspection()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*observed constraints require the provider's feature and complete column observations`)
		})
	}
}

// A creation operand is a declaration's table, never a read's: it carries no
// constraint the read reported.
func TestObservedConstraintIsNotACreationOperand(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := operationRender()
	table := &protocol.Table{Identity: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "items"}}
	request.Changes = []*protocol.Change{{Id: "create", Object: proto.CloneOf(table.Identity), Operation: &protocol.Change_CreateTable{CreateTable: table}}}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	table.Constraints = []*protocol.ObservedConstraint{{Name: "items_check", TableName: "items", Type: "CHECK", CheckClause: new("(id > 0)")}}
	err := provider.ValidateRenderRequest(d, request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*inspected constraints are not a table creation operand`)
}
