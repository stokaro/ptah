package schemacensus

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	identifiers "ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/ydbflags"
	"ptah.run/migration/schemadiff"
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
func Measure(ctx context.Context, service renderer.SchemaService) ([]Observation, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return nil, err
	}
	measured, err := measure(func(schema schemamodel.Database, cell capabilityprobe.Cell) (string, error) {
		return renderOne(ctx, service, schema, cell)
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return measured, nil
}

// measure is the shared body of [Measure] and [MeasurePlan].
//
// The two surfaces are measured by one function on purpose: an agreement test
// comparing two loops that had drifted apart would report the drift as a
// disagreement between the surfaces.
func measure(surface func(schemamodel.Database, capabilityprobe.Cell) (string, error)) ([]Observation, error) {
	fixtures := Fixtures()
	cells := measuredCells()

	baselines := make([]map[string]string, len(fixtures))
	for index, fixture := range fixtures {
		baseline, err := everyCell(surface, fixture.Schema, fixture.Cells(cells))
		if err != nil {
			return nil, fmt.Errorf("baseline %s: %w", fixture.Name, err)
		}
		baselines[index] = baseline
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
			ablated, err := everyCell(surface, Ablate(fixture.Schema, field), fixture.Cells(cells))
			if err != nil {
				return nil, fmt.Errorf("ablate %s from %s: %w", field, fixture.Name, err)
			}
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
	return observations, nil
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
	surface func(schemamodel.Database, capabilityprobe.Cell) (string, error),
	schema schemamodel.Database,
	cells []capabilityprobe.Cell,
) (map[string]string, error) {
	answers := make(map[string]string, len(cells))
	for _, cell := range cells {
		answer, err := surface(schema, cell)
		if err != nil {
			return nil, fmt.Errorf("cell %s: %w", CellName(cell), err)
		}
		answers[CellName(cell)] = answer
	}
	return answers, nil
}

// renderOne is the shipping render path for one cell.
func renderOne(ctx context.Context, service renderer.SchemaService, schema schemamodel.Database, cell capabilityprobe.Cell) (string, error) {
	statements, err := RenderStatements(ctx, service, schema, cell)
	if err != nil {
		return measuredRefusal(err)
	}
	return strings.Join(statements, "\n"), nil
}

// RenderStatements is the same render, answering with the statements rather
// than with their text.
//
// Exported because the emission guard reasons about statements and the
// observability census reasons about bytes, and they have to be the same
// render: two call sites building their own would let the guard measure a
// schema the census never renders.
func RenderStatements(
	ctx context.Context,
	service renderer.SchemaService,
	schema schemamodel.Database, cell capabilityprobe.Cell,
) ([]string, error) {
	finalized := deepCopyDatabase(schema)
	schemamodel.Finalize(&finalized)
	rendered, err := renderer.RenderSchema(ctx, service, renderer.SchemaRequest{
		Target: cell.Dialect, Schema: &finalized, Capabilities: cell.Preset(), Identifiers: identifiers.ForDialect(cell.Dialect),
	})
	return rendered.Statements, err
}

// CellName is how an observation names one declared release line.
func CellName(cell capabilityprobe.Cell) string { return cell.Dialect + "-" + cell.Line }

// completedSchemaRefusal separates a declaration the selected implementation cannot
// interpret from a failed measurement. Unknown model codecs are an intentional
// fixture: the host refuses them before it dispatches to a rendering provider.
// Both census surfaces use this predicate so an operational failure cannot count
// as a render-or-refuse observation on only one of them.
func completedSchemaRefusal(err error) bool {
	// An errors.As search accepts a receipt anywhere in a joined error, even
	// when a sibling is a provider failure. Every joined cause must complete.
	switch refusal := err.(type) {
	case *schemavalidation.RefusalError:
		return refusal != nil && len(refusal.Diagnostics()) != 0
	case *schemadiff.RefusalError:
		return refusal != nil && refusal.Unwrap() != nil
	case *schemaext.UnknownCodecError:
		if refusal == nil || !refusal.Kind.Valid() {
			return false
		}
		switch refusal.Representation {
		case schemaext.Desired, schemaext.Observed, schemaext.Change, schemaext.Operation:
			return true
		}
	case interface{ Unwrap() []error }:
		causes := refusal.Unwrap()
		return len(causes) != 0 && !slices.ContainsFunc(causes, func(cause error) bool {
			return !completedSchemaRefusal(cause)
		})
	case interface{ Unwrap() error }:
		return completedSchemaRefusal(refusal.Unwrap())
	}
	return false
}

func measuredRefusal(err error) (string, error) {
	if !completedSchemaRefusal(err) {
		return "", err
	}
	return "refused: " + err.Error(), nil
}
