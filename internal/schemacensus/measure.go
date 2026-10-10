package schemacensus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

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
	measured, err := measure(renderSurface(ctx, service))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return measured, nil
}

// surface is one way to answer for a schema. The answer function it returns
// answers for one declared release line at a time, and done reports anything
// the answers broke, once every cell has been asked. A surface may prepare the
// schema once rather than once per cell, which is most of a fixture's cost on a
// matrix of many cells.
type surface func(schemamodel.Database) (answer func(capabilityprobe.Cell) (string, error), done func() error)

// renderSurface answers with the shipping render, from one finalized copy of
// the schema rendered on every cell; see [finalizedRender].
func renderSurface(ctx context.Context, service renderer.SchemaService) surface {
	return func(schema schemamodel.Database) (func(capabilityprobe.Cell) (string, error), func() error) {
		render := newFinalizedRender(ctx, service, schema)
		answer := func(cell capabilityprobe.Cell) (string, error) {
			statements, err := render.statements(cell)
			if err != nil {
				return measuredRefusal(err)
			}
			return strings.Join(statements, "\n"), nil
		}
		return answer, render.verify
	}
}

// measure is the shared body of [Measure] and [MeasurePlan].
//
// The two surfaces are measured by one function on purpose: an agreement test
// comparing two loops that had drifted apart would report the drift as a
// disagreement between the surfaces.
//
// Every fixture and every field is an independent measurement, so they run
// side by side; the result does not depend on the order they finish in. The
// first fixture's baseline runs alone, before anything else starts: a surface
// that cannot answer at all, or a context that is canceled, then fails once
// rather than once per worker.
func measure(surface surface) ([]Observation, error) {
	fixtures := Fixtures()
	cells := measuredCells()
	fields := Fields()

	baselines := make([]map[string]string, len(fixtures))
	baseline := func(index int) error {
		fixture := fixtures[index]
		answers, err := everyCell(surface, fixture.Schema, fixture.Cells(cells))
		if err != nil {
			return fmt.Errorf("baseline %s: %w", fixture.Name, err)
		}
		baselines[index] = answers
		return nil
	}
	if len(fixtures) > 0 {
		if err := baseline(0); err != nil {
			return nil, err
		}
	}
	if err := inParallel(len(fixtures)-1, func(index int) error { return baseline(index + 1) }); err != nil {
		return nil, err
	}

	observations := make([]Observation, len(fields))
	err := inParallel(len(fields), func(index int) error {
		observation, err := observe(surface, fields[index], fixtures, baselines, cells)
		observations[index] = observation
		return err
	})
	if err != nil {
		return nil, err
	}
	return observations, nil
}

// observe ablates one field from every fixture that declares it and names the
// cells whose answer moved.
func observe(
	surface surface,
	field string,
	fixtures []Fixture,
	baselines []map[string]string,
	cells []capabilityprobe.Cell,
) (Observation, error) {
	observation := Observation{Field: field}
	for index, fixture := range fixtures {
		if !Populated(fixture.Schema, field) {
			continue
		}
		observation.Covered = append(observation.Covered, fixture.Name)
		ablated, err := everyCell(surface, Ablate(fixture.Schema, field), fixture.Cells(cells))
		if err != nil {
			return Observation{}, fmt.Errorf("ablate %s from %s: %w", field, fixture.Name, err)
		}
		for name, rendered := range ablated {
			if rendered != baselines[index][name] {
				observation.Cells = append(observation.Cells, name)
			}
		}
	}
	slices.Sort(observation.Cells)
	observation.Cells = slices.Compact(observation.Cells)
	return observation, nil
}

// inParallel calls work once for every index below n, on as many goroutines as
// GOMAXPROCS allows. After a failure no further index starts, and the calls
// already running finish. The failure at the lowest index is returned.
func inParallel(n int, work func(index int) error) error {
	var (
		next    atomic.Int64
		stopped atomic.Bool
		mu      sync.Mutex
		failed  = -1
		failure error
		wg      sync.WaitGroup
	)
	for range min(runtime.GOMAXPROCS(0), n) {
		wg.Go(func() {
			for !stopped.Load() {
				index := int(next.Add(1) - 1)
				if index >= n {
					return
				}
				if err := work(index); err != nil {
					stopped.Store(true)
					mu.Lock()
					if failed < 0 || index < failed {
						failed, failure = index, err
					}
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	return failure
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
	surface surface,
	schema schemamodel.Database,
	cells []capabilityprobe.Cell,
) (map[string]string, error) {
	answers := make(map[string]string, len(cells))
	answerFor, done := surface(schema)
	for _, cell := range cells {
		answer, err := answerFor(cell)
		if err != nil {
			return nil, fmt.Errorf("cell %s: %w", CellName(cell), err)
		}
		answers[CellName(cell)] = answer
	}
	if err := done(); err != nil {
		return nil, err
	}
	return answers, nil
}

// finalizedRender renders one schema on many cells from a single finalized
// copy. Rendering reads a schema and never changes it, which is the
// [renderer.SchemaService] contract, so one copy serves every cell; verify
// fails the measurement when a render broke that contract, because every later
// cell would then measure a schema the fixture never declared.
type finalizedRender struct {
	ctx       context.Context
	service   renderer.SchemaService
	schema    schemamodel.Database
	finalized schemamodel.Database
}

func newFinalizedRender(ctx context.Context, service renderer.SchemaService, schema schemamodel.Database) *finalizedRender {
	return &finalizedRender{ctx: ctx, service: service, schema: schema, finalized: finalizedCopy(schema)}
}

// statements is the shipping render for one cell.
func (r *finalizedRender) statements(cell capabilityprobe.Cell) ([]string, error) {
	return renderFinalized(r.ctx, r.service, &r.finalized, cell)
}

// verify reports a render that changed the shared finalized copy.
func (r *finalizedRender) verify() error {
	if !reflect.DeepEqual(r.finalized, finalizedCopy(r.schema)) {
		return errors.New("rendering changed the schema it was given; the census renders one copy on every cell")
	}
	return nil
}

// finalizedCopy is the schema a render reads: a copy, so the fixture is left
// alone, finalized the way a loaded schema is.
func finalizedCopy(schema schemamodel.Database) schemamodel.Database {
	finalized := deepCopyDatabase(schema)
	schemamodel.Finalize(&finalized)
	return finalized
}

// renderFinalized renders a finalized schema on one cell without changing it.
func renderFinalized(
	ctx context.Context,
	service renderer.SchemaService,
	finalized *schemamodel.Database, cell capabilityprobe.Cell,
) ([]string, error) {
	rendered, err := renderer.RenderSchema(ctx, service, renderer.SchemaRequest{
		Target: cell.Dialect, Schema: finalized, Capabilities: cell.Preset(), Identifiers: identifiers.ForDialect(cell.Dialect),
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
	case *schemaext.InvalidModelError:
		return refusal != nil && refusal.Kind.Valid() && strings.TrimSpace(refusal.Message) != "" &&
			slices.Contains([]schemaext.Representation{schemaext.Desired, schemaext.Observed, schemaext.Change, schemaext.Operation}, refusal.Representation)
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
