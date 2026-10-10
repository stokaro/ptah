package astrouteguard_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/astrouteguard"
)

// TestAccount_EveryBaselineKindIsDeclaredOrDeparted is the floor the extraction
// cannot lower. Each node kind core/ast held when the extraction began is either
// still declared or recorded with the owner payloads that carry its operations,
// and each of those payloads is a type the owner packages declare.
func TestAccount_EveryBaselineKindIsDeclaredOrDeparted(t *testing.T) {
	c := qt.New(t)
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	nodes, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)
	payloads, err := astrouteguard.ExtensionKinds(root)
	c.Assert(err, qt.IsNil)

	accounting := astrouteguard.Account(astrouteguard.Baseline(), astrouteguard.Departures(), nodes)

	c.Assert(accounting.Unaccounted, qt.HasLen, 0, qt.Commentf(
		"these node kinds left core/ast without a record of where their operations went; "+
			"add each to recordedDepartures in internal/astrouteguard/departures.go with the owner payloads that carry them"))
	c.Assert(accounting.Invalid, qt.HasLen, 0)
	c.Assert(accounting.Departed, qt.Not(qt.HasLen), 0)
	for _, successor := range accounting.Successors() {
		c.Assert(payloads, qt.Contains, successor, qt.Commentf("a departure names %s, which no owner package declares as a payload", successor))
	}
}

// TestBaseline_HappyPath pins the shape of the record: one row per kind,
// sorted, each kind with a marker core/ast declares or none. The departures are
// sorted the same way, so a new row lands where a reader looks for it.
func TestBaseline_HappyPath(t *testing.T) {
	c := qt.New(t)
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	markers, err := astrouteguard.Markers(root)
	c.Assert(err, qt.IsNil)

	known := append(slices.Clone(markers), astrouteguard.Unmarked)

	baseline := astrouteguard.Baseline()
	names := make([]string, 0, len(baseline))
	for _, kind := range baseline {
		names = append(names, kind.Name)
		c.Assert(known, qt.Contains, kind.Marker)
	}
	departed := make([]string, 0)
	for _, departure := range astrouteguard.Departures() {
		departed = append(departed, departure.Node)
	}

	c.Assert(names, qt.HasLen, 126)
	c.Assert(slices.IsSorted(names), qt.IsTrue)
	c.Assert(slices.Compact(slices.Clone(names)), qt.DeepEquals, names)
	c.Assert(slices.IsSorted(departed), qt.IsTrue)
}

// TestAccount_HappyPath drives the accounting over a synthetic baseline, so
// each outcome is measured on a corpus that holds exactly one example of it.
func TestAccount_HappyPath(t *testing.T) {
	c := qt.New(t)
	widget := astrouteguard.ExtensionKind{Package: "example.org/widget", Name: "Widget"}
	gadget := astrouteguard.ExtensionKind{Package: "example.org/gadget", Name: "Gadget"}
	baseline := []astrouteguard.BaselineKind{
		{Name: "CreateGadgetNode", Marker: astrouteguard.Unmarked},
		{Name: "CreateTableNode", Marker: astrouteguard.Unmarked},
		{Name: "SetWidgetOperation", Marker: astrouteguard.AlterOperationMarker},
	}
	departures := []astrouteguard.Departure{
		{Node: "SetWidgetOperation", Successors: []astrouteguard.ExtensionKind{widget, gadget}},
		{Node: "CreateGadgetNode", Successors: []astrouteguard.ExtensionKind{gadget}},
	}
	nodes := []astrouteguard.NodeKind{{Name: "CreateGadgetNode"}, {Name: "CreateTableNode"}, {Name: "ExtensionStatement"}}

	accounting := astrouteguard.Account(baseline, departures, nodes)

	c.Assert(accounting, qt.DeepEquals, astrouteguard.Accounting{
		Departed: []astrouteguard.Departure{{Node: "SetWidgetOperation", Marker: astrouteguard.AlterOperationMarker,
			Successors: []astrouteguard.ExtensionKind{widget, gadget}}},
		Pending: []astrouteguard.Departure{{Node: "CreateGadgetNode", Marker: astrouteguard.Unmarked,
			Successors: []astrouteguard.ExtensionKind{gadget}}},
	})
	c.Assert(accounting.Successors(), qt.DeepEquals, []astrouteguard.ExtensionKind{gadget, widget})
}

// TestAccount_FailurePath holds each way a record can fail to account for the
// corpus to its own report.
func TestAccount_FailurePath(t *testing.T) {
	widget := astrouteguard.ExtensionKind{Package: "example.org/widget", Name: "Widget"}
	baseline := []astrouteguard.BaselineKind{
		{Name: "CreateTableNode", Marker: astrouteguard.Unmarked},
		{Name: "SetWidgetOperation", Marker: astrouteguard.AlterOperationMarker},
	}
	tests := []struct {
		name            string
		departures      []astrouteguard.Departure
		nodes           []astrouteguard.NodeKind
		wantUnaccounted []astrouteguard.BaselineKind
		wantInvalid     []astrouteguard.Departure
	}{
		{
			name:            "a kind that left with no record",
			nodes:           []astrouteguard.NodeKind{{Name: "CreateTableNode"}},
			wantUnaccounted: []astrouteguard.BaselineKind{{Name: "SetWidgetOperation", Marker: astrouteguard.AlterOperationMarker}},
		},
		{
			name:            "an enumerator that found nothing",
			wantUnaccounted: baseline,
		},
		{
			name:        "a record naming no successor",
			departures:  []astrouteguard.Departure{{Node: "SetWidgetOperation"}},
			nodes:       []astrouteguard.NodeKind{{Name: "CreateTableNode"}},
			wantInvalid: []astrouteguard.Departure{{Node: "SetWidgetOperation", Marker: astrouteguard.AlterOperationMarker}},
		},
		{
			name: "a record of a kind the baseline never held",
			departures: []astrouteguard.Departure{
				{Node: "SetWidgetOperation", Successors: []astrouteguard.ExtensionKind{widget}},
				{Node: "DropWidgetOperation", Successors: []astrouteguard.ExtensionKind{widget}},
			},
			nodes:       []astrouteguard.NodeKind{{Name: "CreateTableNode"}},
			wantInvalid: []astrouteguard.Departure{{Node: "DropWidgetOperation", Successors: []astrouteguard.ExtensionKind{widget}}},
		},
		{
			name: "a kind recorded twice",
			departures: []astrouteguard.Departure{
				{Node: "SetWidgetOperation", Successors: []astrouteguard.ExtensionKind{widget}},
				{Node: "SetWidgetOperation", Successors: []astrouteguard.ExtensionKind{widget}},
			},
			nodes: []astrouteguard.NodeKind{{Name: "CreateTableNode"}},
			wantInvalid: []astrouteguard.Departure{{Node: "SetWidgetOperation", Marker: astrouteguard.AlterOperationMarker,
				Successors: []astrouteguard.ExtensionKind{widget}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			accounting := astrouteguard.Account(baseline, test.departures, test.nodes)

			c.Assert(accounting.Unaccounted, qt.DeepEquals, test.wantUnaccounted)
			c.Assert(accounting.Invalid, qt.DeepEquals, test.wantInvalid)
		})
	}
}

func TestExtensionKind_String(t *testing.T) {
	c := qt.New(t)
	kind := astrouteguard.ExtensionKind{Package: "ptah.run/dialect/ydb/ydbast", Name: "AddChangefeed"}
	c.Assert(kind.String(), qt.Equals, "ptah.run/dialect/ydb/ydbast.AddChangefeed")
}
