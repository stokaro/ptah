package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func reservedTable() *protocol.ReservedObject {
	ref := &protocol.Reference{Engine: "fixture", Database: "db", Schema: "app", Kind: "table", Name: "history"}
	return &protocol.ReservedObject{Object: ref, NormalizedObject: proto.CloneOf(ref), Purpose: protocol.ReservedObject_HISTORY, ConflictKinds: []string{"table"}}
}

func TestInspectionReservationsPreserveUnrelatedObservations(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspection()
	snapshot.ReservedObjects = []*protocol.ReservedObject{reservedTable()}
	before := proto.CloneOf(snapshot)
	values, err := provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values.GetValues(), qt.HasLen, 1)
	c.Assert(proto.Equal(snapshot, before), qt.IsTrue)
}

func TestInspectionRefusesMalformedReservations(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*protocol.Schema)
	}{
		{"nil", func(s *protocol.Schema) { s.ReservedObjects[0] = nil }},
		{"missing normalized", func(s *protocol.Schema) { s.ReservedObjects[0].NormalizedObject = nil }},
		{"foreign engine", func(s *protocol.Schema) { s.ReservedObjects[0].Object.Engine = "foreign" }},
		{"changed kind", func(s *protocol.Schema) { s.ReservedObjects[0].NormalizedObject.Kind = "other" }},
		{"unknown purpose", func(s *protocol.Schema) { s.ReservedObjects[0].Purpose = 99 }},
		{"missing own kind", func(s *protocol.Schema) { s.ReservedObjects[0].ConflictKinds = nil }},
		{"duplicate kind", func(s *protocol.Schema) { s.ReservedObjects[0].ConflictKinds = []string{"table", "table"} }},
		{"unregistered model", func(s *protocol.Schema) { s.ReservedObjects[0].ConflictKinds = []string{"table", "foreign"} }},
		{"table path", func(s *protocol.Schema) {
			s.ReservedObjects[0].Object.Path = []string{"nested"}
			s.ReservedObjects[0].NormalizedObject.Path = []string{"nested"}
		}},
		{"overload", func(s *protocol.Schema) { s.ReservedObjects[0].Object.Overload = []string{"x"} }},
		{"invalid name", func(s *protocol.Schema) { s.ReservedObjects[0].Object.Name = "\x00" }},
		{"duplicate source", func(s *protocol.Schema) {
			s.ReservedObjects = append(s.ReservedObjects, proto.CloneOf(s.ReservedObjects[0]))
			s.ReservedObjects[1].NormalizedObject.Name = "other"
		}},
		{"duplicate normalized", func(s *protocol.Schema) {
			s.ReservedObjects = append(s.ReservedObjects, proto.CloneOf(s.ReservedObjects[0]))
			s.ReservedObjects[1].Object.Name = "OTHER"
		}},
		{"managed table", func(s *protocol.Schema) {
			r := s.ReservedObjects[0]
			s.Tables = []*protocol.Table{{Identity: proto.CloneOf(r.Object), NormalizedIdentity: proto.CloneOf(r.NormalizedObject)}}
		}},
		{"normalized table", func(s *protocol.Schema) {
			r := s.ReservedObjects[0]
			s.Tables = []*protocol.Table{{Identity: proto.CloneOf(r.Object), NormalizedIdentity: proto.CloneOf(r.NormalizedObject)}}
			s.Tables[0].Identity.Name = "HISTORY"
		}},
		{"too many", func(s *protocol.Schema) {
			for range 4096 {
				s.ReservedObjects = append(s.ReservedObjects, proto.CloneOf(s.ReservedObjects[0]))
			}
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, s := inspection()
			s.ReservedObjects = []*protocol.ReservedObject{reservedTable()}
			row.change(s)
			result, err := provider.InspectionValues(d, r, s)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestInspectionNativeReservationsRequireExactNegativeKnowledge(t *testing.T) {
	c := qt.New(t)
	d, r, s := inspection()
	value := s.NativeObjects[0]
	value.Owner.Overload = nil
	s.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(value)}
	reservation := &protocol.ReservedObject{Object: proto.CloneOf(value.Owner), NormalizedObject: proto.CloneOf(value.Owner), Purpose: protocol.ReservedObject_LOCK, ConflictKinds: []string{value.Kind}}
	s.ReservedObjects = []*protocol.ReservedObject{reservation}
	_, err := provider.InspectionValues(d, r, s)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	s.NativeObjects = nil
	_, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	s.Coverage.Subjects[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNREPRESENTABLE, Reason: "provider-owned lock storage"}
	_, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
	s.Coverage.Subjects[0].NormalizedSubject.Name = "other"
	_, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}

func TestInspectionReservationScopeAndComponents(t *testing.T) {
	c := qt.New(t)
	d, r, s := inspection()
	s.NativeObjects = nil
	s.Coverage = &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}
	s.ReservedObjects = []*protocol.ReservedObject{reservedTable()}
	r.Schemas = []string{"app"}
	s.ScopeBindings = []*protocol.InspectionScopeBinding{{Selector: "app", Namespaces: []*protocol.ModelNamespace{{Database: &protocol.NamespaceComponent{Source: "db", Normalized: "db"}, Schema: &protocol.NamespaceComponent{Source: "app", Normalized: "app"}}}}}
	_, err := provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
	s.ReservedObjects[0].NormalizedObject.Schema = "outside"
	_, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	r.Schemas = nil
	s.ScopeBindings = nil
	first := reservedTable()
	first.Object.Schema = "a.b"
	first.NormalizedObject.Schema = "a.b"
	first.Object.Name = "c"
	first.NormalizedObject.Name = "c"
	second := reservedTable()
	second.Object.Schema = "a"
	second.NormalizedObject.Schema = "a"
	second.Object.Name = "b.c"
	second.NormalizedObject.Name = "b.c"
	s.ReservedObjects = []*protocol.ReservedObject{first, second}
	_, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
}
