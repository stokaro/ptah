package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// uniqueInspection is constraintInspection with UNIQUE constraints
// negotiated, of a table that holds one over both its columns and one over
// its label alone, which the label states.
func uniqueInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, snapshot := constraintInspection()
	d.Features = append(d.Features, provider.ObservedUniqueConstraintsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ObservedUniqueConstraintsFeature)
	snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.ObservedUniqueConstraintsFeature)
	snapshot.Tables[0].Columns[1].Observed.Unique = true
	snapshot.Tables[0].Constraints = append(snapshot.Tables[0].Constraints,
		&protocol.ObservedConstraint{Name: "items_id_label_key", TableName: "items", Schema: "app", Type: "UNIQUE", ColumnNames: []string{"id", "label"}},
		&protocol.ObservedConstraint{Name: "items_label_key", TableName: "items", Schema: "app", Type: "UNIQUE", ColumnNames: []string{"label"}},
	)
	return d, request, snapshot
}

func TestObservedUniqueConstraints_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a negotiated read reports its UNIQUE constraints and they survive the wire", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := uniqueInspection()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
		data, err := proto.Marshal(snapshot)
		c.Assert(err, qt.IsNil)
		decoded := &protocol.Schema{}
		c.Assert(proto.Unmarshal(data, decoded), qt.IsNil)
		_, err = provider.InspectionValues(d, request, decoded)
		c.Assert(err, qt.IsNil)
		c.Assert(decoded.Tables[0].Constraints[2].GetColumnNames(), qt.DeepEquals, []string{"id", "label"})
	})
	t.Run("a UNIQUE constraint with NULLS NOT DISTINCT and deferral is complete", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := uniqueInspection()
		unique := snapshot.Tables[0].Constraints[2]
		unique.NullsDistinct, unique.Deferrable, unique.Initially = new(false), true, "IMMEDIATE"
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a column that states it is unique needs no UNIQUE constraint over it", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := uniqueInspection()
		snapshot.Tables[0].Constraints = snapshot.Tables[0].Constraints[:3]
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a host that does not list the feature negotiates it away", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := uniqueInspection()
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.ObservedUniqueConstraintsFeature))
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.ObservedUniqueConstraintsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ObservedUniqueConstraintsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.ObservedConstraintsFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ObservedUniqueConstraintsFeature)
	})
}

// A read that did not negotiate UNIQUE constraints reports none, and one
// that did reports each over the table's columns, with the attributes a
// UNIQUE constraint has only.
func TestObservedUniqueConstraints_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.Schema)
	}{
		{"the provider did not list the feature", `.*observed constraint "items_id_label_key" of table "items" is neither a FOREIGN KEY nor a CHECK`,
			func(d *protocol.Description, _ *protocol.Schema) {
				d.Features = withoutFeature(d.Features, provider.ObservedUniqueConstraintsFeature)
			}},
		{"the host did not list the feature", `.*observed constraint "items_id_label_key" of table "items" is neither a FOREIGN KEY nor a CHECK`,
			func(_ *protocol.Description, s *protocol.Schema) {
				s.TargetFeatures = withoutFeature(s.TargetFeatures, provider.ObservedUniqueConstraintsFeature)
			}},
		{"a primary key", `.*observed constraint "items_label_key" of table "items" is not a FOREIGN KEY, a CHECK or a UNIQUE constraint`,
			func(_ *protocol.Description, s *protocol.Schema) {
				s.Tables[0].Constraints[3].Type = "PRIMARY KEY"
			}},
		{"no columns", `.*is a UNIQUE constraint without its columns`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].ColumnNames = nil
		}},
		{"an unknown column", `.*names an unknown or repeated column`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].ColumnNames = []string{"id", "missing"}
		}},
		{"a repeated column", `.*names an unknown or repeated column`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].ColumnNames = []string{"id", "id"}
		}},
		{"the singular column field", `.*lists them in column_names`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].ColumnName = "id"
		}},
		{"index attributes", `.*carries index attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].IncludeColumns = []string{"label"}
		}},
		{"an index method", `.*carries index attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].UsingMethod = new("btree")
		}},
		{"NULLS NOT DISTINCT on a CHECK", `.*observed constraint "items_label_check" of table "items" carries index attributes`,
			func(_ *protocol.Description, s *protocol.Schema) {
				s.Tables[0].Constraints[1].NullsDistinct = new(false)
			}},
		{"a referenced table", `.*is a UNIQUE constraint with foreign key or CHECK attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].ForeignTable = new("owners")
		}},
		{"a MATCH type", `.*is a UNIQUE constraint with foreign key or CHECK attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].Match = "FULL"
		}},
		{"a CHECK clause", `.*is a UNIQUE constraint with foreign key or CHECK attributes`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].CheckClause = new("(id > 0)")
		}},
		{"a timing without deferral", `.*has invalid deferral`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].Initially = "DEFERRED"
		}},
		{"a timing of another spelling", `.*has invalid deferral`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].Deferrable, s.Tables[0].Constraints[2].Initially = true, "deferred"
		}},
		{"one column that does not state it is unique", `.*observed constraint "items_label_key" of table "items" covers column "label" alone, which does not state that it is unique`,
			func(_ *protocol.Description, s *protocol.Schema) {
				s.Tables[0].Columns[1].Observed.Unique = false
			}},
		{"a name a foreign key takes", `.*requires a name unique within its table`, func(_ *protocol.Description, s *protocol.Schema) {
			s.Tables[0].Constraints[2].Name = "items_owner_fk"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := uniqueInspection()
			test.mutate(d, snapshot)
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestObservedUniqueConstraintsDescription_FailurePath(t *testing.T) {
	t.Parallel()
	t.Run("the engine's feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := uniqueInspection()
		d.Features = withoutFeature(d.Features, provider.ObservedUniqueConstraintsFeature)
		err := provider.ValidateDescription(d, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*observed UNIQUE constraints require the provider's feature and observed constraints`)
	})
	t.Run("the engine's feature without observed constraints", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := uniqueInspection()
		d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ObservedConstraintsFeature)
		err := provider.ValidateDescription(d, nil)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(err, qt.ErrorMatches, `.*observed UNIQUE constraints require the provider's feature and observed constraints`)
	})
}
