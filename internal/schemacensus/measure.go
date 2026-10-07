package schemacensus

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/ydbflags"
)

// Observation is what the census established about one field.
//
// Covered is empty when no fixture declares the field: nothing was measured,
// which is a different answer from "measured and nothing reads it" and is kept
// apart for that reason. Cells is empty when every ablation left every render
// byte-identical.
type Observation struct {
	Field   string
	Covered []string
	Cells   []string
}

// Observed reports whether removing the field changed a render anywhere.
func (o Observation) Observed() bool { return len(o.Cells) > 0 }

// Measure ablates each field out of every fixture that declares it, re-renders
// on every declared matrix cell, and reports the cells where the output moved.
//
// The cells come from [capabilityprobe.Cells] rather than from the dialect list,
// because a field can be a fact about a release line rather than about an
// engine: a NOT NULL constraint name is kept by PostgreSQL 18 and by nothing
// before it, and a census that rendered only the default preset would record it
// as a fact nothing reads.
//
// A refusal counts as a change. A target that says out loud that it cannot carry
// a declaration has read it, which is the disposition this package calls
// rendering-or-refusing rather than dropping; what it must not do is answer the
// same way with the field and without it.
func Measure() []Observation {
	return measure(renderOne)
}

// measure is the shared body of [Measure] and [MeasurePlan].
//
// The two surfaces are measured by one function on purpose: an agreement test
// comparing two loops that had drifted apart would report the drift as a
// disagreement between the surfaces.
func measure(surface func(schemamodel.Database, capabilityprobe.Cell) string) []Observation {
	fixtures := Fixtures()
	cells := measuredCells()

	baselines := make([]map[string]string, len(fixtures))
	for index, fixture := range fixtures {
		baselines[index] = everyCell(surface, fixture.Schema, fixture.Cells(cells))
	}

	fields := Fields()
	observations := make([]Observation, 0, len(fields))
	for _, field := range fields {
		observation := Observation{Field: field}
		for index, fixture := range fixtures {
			if !Populated(fixture.Schema, field) {
				continue
			}
			observation.Covered = append(observation.Covered, fixture.Name)
			ablated := everyCell(surface, Ablate(fixture.Schema, field), fixture.Cells(cells))
			for name, rendered := range ablated {
				if rendered != baselines[index][name] {
					observation.Cells = append(observation.Cells, name)
				}
			}
		}
		slices.Sort(observation.Cells)
		observation.Cells = slices.Compact(observation.Cells)
		observations = append(observations, observation)
	}
	return observations
}

// measuredCells are the declared release lines, and each YDB line again with
// every feature flag Ptah reads turned on, named with a `+flags` suffix.
//
// A YDB preset describes a cluster running its line's default flags, and a
// declaration a flag gates is refused on every one of them: a resource pool
// is, since EnableResourcePools is off by default on every line. Rendered only
// on the presets, every setting of a pool would read as a field nothing reads,
// because the refusal comes first and names none of them. [ydbflags.Flags.Refine]
// is how Ptah turns a flag the cluster reports into its key, and each gate's
// flag was measured on every line with the flag on, so the refined set is one
// Ptah plans for on such a cluster.
func measuredCells() []capabilityprobe.Cell {
	allOn := make(ydbflags.Flags)
	for _, gate := range ydbflags.Gates() {
		allOn[gate.Flag] = true
	}
	cells := slices.Clone(capabilityprobe.Cells)
	for _, cell := range capabilityprobe.Cells {
		if cell.Dialect != platform.YDB || cell.Preset == nil {
			continue
		}
		preset := cell.Preset
		cell.Line += "+flags"
		cell.Preset = func() capability.Capabilities { return allOn.Refine(preset()) }
		cells = append(cells, cell)
	}
	return cells
}

// everyCell applies one surface to one schema against every declared release
// line, keyed by cell name. A refusal is kept as its own text so an ablation
// that changes WHICH refusal answers still counts as a change.
func everyCell(
	surface func(schemamodel.Database, capabilityprobe.Cell) string,
	schema schemamodel.Database,
	cells []capabilityprobe.Cell,
) map[string]string {
	answers := make(map[string]string, len(cells))
	for _, cell := range cells {
		answers[CellName(cell)] = surface(schema, cell)
	}
	return answers
}

// renderOne is the shipping render path for one cell.
func renderOne(schema schemamodel.Database, cell capabilityprobe.Cell) string {
	statements, err := RenderStatements(schema, cell)
	if err != nil {
		return "refused: " + err.Error()
	}
	return strings.Join(statements, "\n")
}

// RenderStatements is the same render, answering with the statements rather
// than with their text.
//
// Exported because the emission guard reasons about statements and the
// observability census reasons about bytes, and they have to be the same
// render: two call sites building their own would let the guard measure a
// schema the census never renders.
func RenderStatements(
	schema schemamodel.Database, cell capabilityprobe.Cell,
) ([]string, error) {
	finalized := deepCopyDatabase(schema)
	schemamodel.Finalize(&finalized)
	return builtin.GetOrderedCreateStatementsWithCapabilities(
		&finalized,
		cell.Dialect,
		cell.Preset(),
	)
}

// CellName is how an observation names one declared release line.
func CellName(cell capabilityprobe.Cell) string { return cell.Dialect + "-" + cell.Line }
