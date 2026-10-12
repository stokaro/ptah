package schemacensus

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	identifiers "ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// Direction is one half of the round trip between the declared model and the
// catalog shape a comparison reads.
type Direction string

const (
	// ToCatalog compares a desired schema with the catalog goschematodb makes
	// of it: Diff(S, ToCatalog(S)). It is how `ptah schema diff --from <file>`
	// reads its current side.
	ToCatalog Direction = "to-catalog"

	// FromCatalog compares what dbschematogo makes of a catalog with that
	// catalog: Diff(FromCatalog(C), C). Every command that turns a database
	// back into a declaration takes this path. C is ToCatalog(S) of a census
	// fixture, so the catalog shapes it covers are the ones the other
	// direction produces.
	FromCatalog Direction = "from-catalog"
)

// RoundTripDirections is both directions, in the order a reader meets them.
func RoundTripDirections() []Direction { return []Direction{ToCatalog, FromCatalog} }

// Pair is one census fixture on one dialect.
type Pair struct {
	Fixture string
	Dialect string
}

// Loss is a pair whose round trip planned a change: a schema compared with
// its own conversion is not in sync, which is a false migration.
type Loss struct {
	Pair
	Direction Direction
	// Plan is the statements the comparison planned, or its refusal.
	Plan string
}

// Reach is what the round trip established about one compared field on one
// dialect, in one direction.
//
// Dialect is empty for a [Comparison] field: no statement writes one, so no
// render says which dialects read it, and one dialect whose comparison reads
// it is the claim its disposition makes.
type Reach struct {
	Field     string
	Dialect   string
	Direction Direction
	// Covered names the fixtures the requirement was measured on: they
	// declare the field, their round trip there is clean in this direction,
	// and for a [DDL] field the render's statements on the dialect read it.
	Covered []string
	// Reached is true when changing the field on the converted side changed
	// what the comparison planned, without a refusal.
	Reached bool
	// Lost says where the change went when it did not reach the plan:
	// "goschematodb" or "dbschematogo" when a conversion produced the same
	// shape with and without it, "comparison" when both conversions carried
	// it and the comparison planned nothing, and "refused" when every change
	// was refused before anything was compared.
	Lost string
	// Refusal is the last refusal the first fixture gave, when every change
	// of the field was refused.
	Refusal string
}

// RoundTrip is the result of [MeasureRoundTrip].
type RoundTrip struct {
	// Pairs is every fixture and dialect in scope: the render accepts the
	// fixture on the dialect's preset.
	Pairs []Pair
	// Losses is every pair and direction whose round trip was not empty.
	Losses []Loss
	// Reach is one entry per compared field, required dialect and direction.
	Reach []Reach
	// Unmeasured is every compared field with no requirement anywhere, and
	// why.
	Unmeasured []Unmeasured
}

// Unmeasured is a compared field the round trip could not ask about.
type Unmeasured struct {
	Field string
	Why   string
}

// Why an [Unmeasured] field could not be asked about.
const (
	// UnmeasuredUndeclared: no census fixture declares the field.
	UnmeasuredUndeclared = "declared by no fixture"
	// UnmeasuredRefused: every target refuses every fixture declaring it.
	UnmeasuredRefused = "no target accepts a fixture declaring it"
	// UnmeasuredLost: every pair whose statements read it has a [Loss] in
	// the direction asked, so it is measured once that loss is repaired.
	UnmeasuredLost = "every pair that reads it is lost"
	// UnmeasuredUnread: wherever it could be measured, the render's
	// statements are the same without it. A field written only into a comment
	// line is one.
	UnmeasuredUnread = "no statement reads it"
)

// MeasureRoundTrip asks whether the conversions between the declared model and
// the catalog keep every fact the comparison reads.
//
// When a conversion drops a field, a schema compared with its own conversion
// plans a migration nobody asked for. Each of those was found one at a time:
// a range's subtype attributes, a selected column's domain, a constraint's
// comment. This measurement asks it of every field at once, in two parts.
//
// Preservation: every census fixture, on every dialect whose render accepts
// it, is compared with its own conversion in both [Direction]s, and the
// comparison must plan nothing.
//
// Discrimination: an empty plan proves nothing about a field both the
// conversion and the comparison drop. So every field disposed [DDL] or
// [Comparison] is changed on the converted side alone -- removed, perturbed,
// or set to its entry in the alternates table -- and the comparison must plan
// something. A [DDL] field is required on each dialect whose render reads it
// there, measured the way [Measure] measures it but without comment lines, so
// a field a target drops is required only where it reaches a statement. A
// field holding a struct is reached when one of its own fields is, since
// every one of those is a census field too.
//
// The dialects are the matrix's, each on the declared preset with the most
// capabilities and every YDB feature flag on, and the comparison is
// [schemadiff.CompareWithDatabaseInfo] given that preset: the comparison a
// command connected to such a server runs. The offline entry points read the
// dialect's default preset, which turns no YDB flag on, so a declaration a
// flag or a newer line gates could not be asked about through them. Pairs and
// fields run in parallel; the answer does not depend on the order they finish
// in.
func MeasureRoundTrip(ctx context.Context, runtime engine.SchemaRuntime) (RoundTrip, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return RoundTrip{}, err
	}
	trip := &roundTrip{ctx: ctx, runtime: runtime, fixtures: Fixtures(), dialects: roundTripDialects()}
	trip.presets = richestPresets(trip.dialects)
	trip.fields = comparedFields()
	if err := trip.measurePairs(); err != nil {
		return RoundTrip{}, err
	}
	reach, err := trip.measureReach()
	if err != nil {
		return RoundTrip{}, err
	}
	if err := ctx.Err(); err != nil {
		return RoundTrip{}, err
	}
	result := RoundTrip{Reach: reach, Unmeasured: trip.unmeasured(reach)}
	for _, pair := range trip.pairs {
		if pair == nil {
			continue
		}
		result.Pairs = append(result.Pairs, pair.pair)
		result.Losses = append(result.Losses, pair.losses...)
	}
	return result, nil
}

// comparedFields is every field whose disposition says a comparison reads it.
//
// [DDL] is here because a fact that reaches a statement is a fact a
// comparison must keep: a field that renders and is not compared is changed
// only by recreating its object. [Comparison] is here by definition. The
// others are not: [Planning] orders statements, [Derived] is recomputed from
// the fields that are here, [SourceOrigin] names source text, [Export] reaches
// a document and [Data] reaches rows, and none of them is a fact about the
// database a comparison could find different.
func comparedFields() []compared {
	types := fieldTypes()
	var fields []compared
	for _, entry := range Registry() {
		if entry.Disposition != DDL && entry.Disposition != Comparison {
			continue
		}
		fields = append(fields, compared{path: entry.Field, disposition: entry.Disposition, typ: types[entry.Field]})
	}
	return fields
}

type compared struct {
	path        string
	disposition Disposition
	typ         reflect.Type
}

// leaves is the prefix every field of the struct this field holds starts
// with, or empty when it holds no struct of this module.
func (c compared) leaves() string {
	if c.typ == nil {
		return ""
	}
	element := elementType(c.typ)
	if element.Kind() != reflect.Struct || !descends(element) {
		return ""
	}
	return typeName(element) + "."
}

// roundTripDialects are the distinct dialects of the declared release lines,
// in the order the matrix declares them.
func roundTripDialects() []string {
	var dialects []string
	for _, cell := range capabilityprobe.Cells {
		if !slices.Contains(dialects, cell.Dialect) {
			dialects = append(dialects, cell.Dialect)
		}
	}
	return dialects
}

type roundTrip struct {
	ctx      context.Context
	runtime  engine.SchemaRuntime
	fixtures []Fixture
	dialects []string
	presets  map[string]capability.Capabilities
	fields   []compared
	// pairs is indexed fixture-major; nil marks a pair out of scope.
	pairs []*pairState
}

// pairState is what the first pass learned about one fixture on one dialect.
type pairState struct {
	pair    Pair
	fixture Fixture
	losses  []Loss
	clean   map[Direction]bool
	// returned is the fixture converted to a catalog and back, the desired
	// side of every comparison in the FromCatalog direction.
	returned *schemamodel.Database
	// reads holds the fields whose removal changes the render's statements.
	reads map[string]bool
}

func (t *roundTrip) measurePairs() error {
	t.pairs = make([]*pairState, len(t.fixtures)*len(t.dialects))
	return inParallel(len(t.pairs), func(index int) error {
		fixture := t.fixtures[index/len(t.dialects)]
		dialect := t.dialects[index%len(t.dialects)]
		state, err := t.measurePair(fixture, dialect)
		if err != nil {
			return fmt.Errorf("%s on %s: %w", fixture.Name, dialect, err)
		}
		t.pairs[index] = state
		return nil
	})
}

// measurePair answers preservation for one pair and records which compared
// fields its render reads. A pair the render refuses is out of scope.
func (t *roundTrip) measurePair(fixture Fixture, dialect string) (*pairState, error) {
	baseline, accepted, err := t.statements(fixture.Schema, dialect)
	if err != nil || !accepted {
		return nil, err
	}
	state := &pairState{
		pair: Pair{Fixture: fixture.Name, Dialect: dialect}, fixture: fixture,
		clean: make(map[Direction]bool), reads: make(map[string]bool),
	}
	answers := map[Direction]answer{ToCatalog: t.forward(fixture.Schema, fixture.Schema, dialect)}
	returned, err := t.back(fixture.Schema, dialect)
	answers[FromCatalog] = answer{err: err}
	if err == nil {
		state.returned = returned
		answers[FromCatalog] = t.reverse(returned, fixture.Schema, dialect)
	}
	for _, direction := range RoundTripDirections() {
		if answers[direction].empty() {
			state.clean[direction] = true
			continue
		}
		state.losses = append(state.losses, Loss{Pair: state.pair, Direction: direction, Plan: t.describe(answers[direction], dialect)})
	}
	for _, field := range t.fields {
		if field.disposition != DDL || !fixture.declares(field.path) {
			continue
		}
		rendered, _, err := t.statements(Ablate(fixture.Schema, field.path), dialect)
		if err != nil {
			return nil, fmt.Errorf("without %s: %w", field.path, err)
		}
		if rendered != baseline {
			state.reads[field.path] = true
		}
	}
	return state, nil
}

// measureReach probes every compared field on every dialect that requires it.
func (t *roundTrip) measureReach() ([]Reach, error) {
	perField := make([][]Reach, len(t.fields))
	err := inParallel(len(t.fields), func(index int) error {
		perField[index] = t.reachOf(t.fields[index])
		return t.ctx.Err()
	})
	if err != nil {
		return nil, err
	}
	reach := slices.Concat(perField...)
	return creditContainers(t.fields, reach, t.fixtures), nil
}

// reachOf is one field's requirements and whether each was met.
func (t *roundTrip) reachOf(field compared) []Reach {
	var reach []Reach
	scopes := t.dialects
	if field.disposition == Comparison {
		scopes = []string{""}
	}
	for _, dialect := range scopes {
		for _, direction := range RoundTripDirections() {
			candidates := t.candidates(field, dialect, direction)
			if len(candidates) == 0 {
				continue
			}
			entry := Reach{Field: field.path, Dialect: dialect, Direction: direction}
			for _, pair := range candidates {
				entry.Covered = append(entry.Covered, pair.pair.Fixture)
			}
			slices.Sort(entry.Covered)
			entry.Covered = slices.Compact(entry.Covered)
			entry.Reached, entry.Lost, entry.Refusal = t.probe(field, candidates, direction)
			reach = append(reach, entry)
		}
	}
	return reach
}

// unmeasured names every compared field no requirement was measured for.
func (t *roundTrip) unmeasured(reach []Reach) []Unmeasured {
	measured := make(map[string]bool)
	for _, entry := range reach {
		measured[entry.Field] = true
	}
	var unmeasured []Unmeasured
	for _, field := range t.fields {
		if measured[field.path] {
			continue
		}
		declared, accepted, read := false, false, false
		for index, pair := range t.pairs {
			if !t.fixtures[index/len(t.dialects)].declares(field.path) {
				continue
			}
			declared = true
			if pair == nil {
				continue
			}
			accepted = true
			read = read || field.disposition == Comparison || pair.reads[field.path]
		}
		why := UnmeasuredUnread
		switch {
		case !declared:
			why = UnmeasuredUndeclared
		case !accepted:
			why = UnmeasuredRefused
		case read:
			why = UnmeasuredLost
		}
		unmeasured = append(unmeasured, Unmeasured{Field: field.path, Why: why})
	}
	return unmeasured
}

// candidates is every in-scope pair the requirement is measured on.
func (t *roundTrip) candidates(field compared, dialect string, direction Direction) []*pairState {
	var candidates []*pairState
	for _, pair := range t.pairs {
		if pair == nil || !pair.clean[direction] || (dialect != "" && pair.pair.Dialect != dialect) {
			continue
		}
		if field.disposition == DDL && !pair.reads[field.path] {
			continue
		}
		if field.disposition == Comparison && !pair.fixture.declares(field.path) {
			continue
		}
		candidates = append(candidates, pair)
	}
	return candidates
}

// probe changes the field on the converted side of each candidate in turn
// until a comparison plans something. Lost is decided on the first candidate,
// which keeps it independent of scheduling.
func (t *roundTrip) probe(field compared, candidates []*pairState, direction Direction) (reached bool, lost, refusal string) {
	for index, pair := range candidates {
		var last error
		refused := true
		for _, changed := range changes(pair.fixture.Schema, field.path) {
			var result answer
			if direction == ToCatalog {
				result = t.forward(pair.fixture.Schema, changed, pair.pair.Dialect)
			} else {
				result = t.reverse(pair.returned, changed, pair.pair.Dialect)
			}
			if result.err == nil && result.changed {
				return true, "", ""
			}
			if result.err == nil {
				refused = false
			}
			last = result.err
		}
		if index == 0 && refused {
			lost = "refused"
			if last != nil {
				refusal = last.Error()
			}
		}
		if index == 0 && !refused {
			lost = t.lostIn(pair, field, direction)
		}
	}
	return false, lost, refusal
}

// changes is every change the probe makes to the field: removed, perturbed,
// and set to its alternate. A change that leaves the schema as it was is
// left out.
func changes(schema schemamodel.Database, path string) []schemamodel.Database {
	changed := []schemamodel.Database{Ablate(schema, path)}
	if perturbed, ok := Perturb(schema, path); ok {
		changed = append(changed, perturbed)
	}
	if alternate, ok := Alternate(schema, path); ok {
		changed = append(changed, alternate)
	}
	return changed
}

// lostIn names where a change of the field went on one pair whose comparison
// answered without a refusal.
func (t *roundTrip) lostIn(pair *pairState, field compared, direction Direction) string {
	changed := changes(pair.fixture.Schema, field.path)[0]
	dialect := pair.pair.Dialect
	before, errBefore := t.toCatalog(pair.fixture.Schema, dialect)
	after, errAfter := t.toCatalog(changed, dialect)
	if errBefore != nil || errAfter != nil || reflect.DeepEqual(before, after) {
		return "goschematodb"
	}
	if direction == ToCatalog {
		return "comparison"
	}
	backBefore, errBefore := dbschematogo.ConvertDBSchemaToGoSchema(t.ctx, before, dialect, t.runtime)
	backAfter, errAfter := dbschematogo.ConvertDBSchemaToGoSchema(t.ctx, after, dialect, t.runtime)
	if errBefore != nil || errAfter != nil || reflect.DeepEqual(backBefore, backAfter) {
		return "dbschematogo"
	}
	return "comparison"
}

// creditContainers marks a field holding a struct reached wherever one of
// that struct's fields is. Changing the container changes its fields, and each
// of those carries its own requirement, so the container's own probe -- which
// can only remove it, and is usually refused for that -- is not the evidence.
//
// A feature container holds owner models rather than one struct type, so its
// fields are the fields of whichever models the corpus attaches there.
func creditContainers(fields []compared, reach []Reach, fixtures []Fixture) []Reach {
	type key struct {
		dialect   string
		direction Direction
	}
	reached := make(map[key][]string)
	for _, entry := range reach {
		if entry.Reached {
			for _, dialect := range []string{entry.Dialect, ""} {
				k := key{dialect, entry.Direction}
				reached[k] = append(reached[k], entry.Field)
			}
		}
	}
	prefixes := make(map[string][]string)
	for _, field := range fields {
		if prefix := field.leaves(); prefix != "" {
			prefixes[field.path] = []string{prefix}
		}
		if isFeatureContainer(field.typ) {
			prefixes[field.path] = featureLeaves(fixtures, field.path)
		}
	}
	for index, entry := range reach {
		if entry.Reached {
			continue
		}
		for _, prefix := range prefixes[entry.Field] {
			if slices.ContainsFunc(reached[key{entry.Dialect, entry.Direction}], func(field string) bool {
				return strings.HasPrefix(field, prefix)
			}) {
				reach[index].Reached, reach[index].Lost, reach[index].Refusal = true, "", ""
				break
			}
		}
	}
	return reach
}

// isFeatureContainer reports whether a field holds owner models.
func isFeatureContainer(t reflect.Type) bool {
	return t != nil && (t == facetsType || t == objectsType)
}

// featureLeaves is the field prefix of every owner model the corpus attaches
// to the field named by path.
func featureLeaves(fixtures []Fixture, path string) []string {
	var prefixes []string
	add := func(value any) {
		if prefix := typeName(elementType(reflect.TypeOf(value))) + "."; !slices.Contains(prefixes, prefix) {
			prefixes = append(prefixes, prefix)
		}
	}
	for _, fixture := range fixtures {
		visitField(reflect.ValueOf(fixture.Schema), path, readOnly, func(field reflect.Value) {
			switch container := field.Interface().(type) {
			case schemaext.Facets:
				values, err := container.Values()
				if err == nil {
					for _, value := range values {
						add(value)
					}
				}
			case schemaext.Objects:
				objects, err := container.All()
				if err == nil {
					for _, object := range objects {
						add(object.Value)
					}
				}
			}
		})
	}
	slices.Sort(prefixes)
	return prefixes
}

// answer is what one comparison said: whether it planned anything, or why it
// refused.
type answer struct {
	changed bool
	err     error
	diff    *difftypes.SchemaDiff
}

func (a answer) empty() bool { return a.err == nil && !a.changed }

// statements renders schema on the dialect's preset and keeps the statements
// without their comment lines: a target that drops a declaration says so in
// a comment, and that note is not a fact a catalog could hold. accepted is
// false when the render refused the schema; the refusal is then the answer.
func (t *roundTrip) statements(schema schemamodel.Database, dialect string) (string, bool, error) {
	finalized := finalizedCopy(schema)
	rendered, err := renderer.RenderSchema(t.ctx, t.runtime, renderer.SchemaRequest{
		Target: dialect, Schema: &finalized, Capabilities: t.presets[dialect],
		Identifiers: identifiers.ForDialect(dialect),
	})
	if err != nil {
		refusal, err := measuredRefusal(err)
		return refusal, false, err
	}
	var kept []string
	for _, statement := range rendered.Statements {
		for line := range strings.SplitSeq(statement, "\n") {
			if trimmed := strings.TrimSpace(line); trimmed != "" && !strings.HasPrefix(trimmed, "--") {
				kept = append(kept, line)
			}
		}
	}
	return strings.Join(kept, "\n"), true, nil
}

// richestPresets is, for each dialect, the declared release line whose preset
// holds the most capabilities, with every YDB feature flag Ptah reads turned
// on as [measuredCells] turns them on. A declaration one release line or one
// flag gates is then in scope wherever the matrix says some server accepts it.
func richestPresets(dialects []string) map[string]capability.Capabilities {
	presets := make(map[string]capability.Capabilities, len(dialects))
	for _, cell := range measuredCells() {
		if cell.Preset == nil {
			continue
		}
		preset := cell.Preset()
		if held, ok := presets[cell.Dialect]; !ok || enabled(preset) > enabled(held) {
			presets[cell.Dialect] = preset
		}
	}
	for _, dialect := range dialects {
		if _, ok := presets[dialect]; !ok {
			presets[dialect] = capability.ForDialect(dialect)
		}
	}
	return presets
}

// enabled counts the capabilities a preset turns on.
func enabled(preset capability.Capabilities) int {
	count := 0
	for _, on := range preset.All() {
		if on {
			count++
		}
	}
	return count
}

// forward compares desired with the catalog goschematodb makes of current,
// through the comparison a command with a connection to such a server runs.
func (t *roundTrip) forward(desired, current schemamodel.Database, dialect string) answer {
	converted, err := t.toCatalog(current, dialect)
	if err != nil {
		return answer{err: err}
	}
	d := finalizedCopy(desired)
	return t.compare(&d, converted, dialect)
}

// compare is the comparison every answer comes from.
func (t *roundTrip) compare(desired *schemamodel.Database, current *catalog.Database, dialect string) answer {
	diff, err := schemadiff.CompareWithDatabaseInfo(t.ctx, desired, current,
		catalog.ServerInfo{Dialect: dialect, Capabilities: t.presets[dialect]}, nil, t.runtime)
	if err != nil {
		return answer{err: err}
	}
	return answer{changed: diff.HasChanges(), diff: diff}
}

// back converts a schema to a catalog and the catalog back to a schema.
func (t *roundTrip) back(schema schemamodel.Database, dialect string) (*schemamodel.Database, error) {
	converted, err := t.toCatalog(schema, dialect)
	if err != nil {
		return nil, err
	}
	return dbschematogo.ConvertDBSchemaToGoSchema(t.ctx, converted, dialect, t.runtime)
}

// reverse compares a converted-back schema with the catalog goschematodb
// makes of current.
func (t *roundTrip) reverse(returned *schemamodel.Database, current schemamodel.Database, dialect string) answer {
	converted, err := t.toCatalog(current, dialect)
	if err != nil {
		return answer{err: err}
	}
	desired := deepCopyDatabase(*returned)
	return t.compare(&desired, converted, dialect)
}

func (t *roundTrip) toCatalog(schema schemamodel.Database, dialect string) (*catalog.Database, error) {
	finalized := finalizedCopy(schema)
	return goschematodb.ToDBSchema(t.ctx, &finalized, dialect, t.runtime)
}

// describe is what a loss reports: the statements a user would be shown, or
// the refusal they would get.
func (t *roundTrip) describe(result answer, dialect string) string {
	if result.err != nil {
		return "refused: " + result.err.Error()
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.ctx, t.runtime, result.diff, dialect,
		planner.Options{Capabilities: t.presets[dialect]})
	if err != nil {
		return "planned a change the planner refused: " + err.Error()
	}
	return strings.Join(statements, "\n")
}

// Perturb returns a copy of schema with every non-zero instance of the field
// named by path changed to another value of its type, and whether any instance
// changed. A string gains a suffix, a number grows by one, a pointer to either
// points at the changed value, and every string of a string slice gains the
// suffix. Other kinds are left alone: removing them is [Ablate]'s job.
func Perturb(schema schemamodel.Database, path string) (schemamodel.Database, bool) {
	perturbed := deepCopyDatabase(schema)
	changed := false
	visitField(reflect.ValueOf(&perturbed).Elem(), path, readWrite, func(field reflect.Value) {
		if !field.CanSet() || field.IsZero() {
			return
		}
		if next, ok := perturbValue(field); ok {
			field.Set(next)
			changed = true
		}
	})
	return perturbed, changed
}

// perturbValue is one value changed to another of its type.
func perturbValue(value reflect.Value) (reflect.Value, bool) {
	next := reflect.New(value.Type()).Elem()
	switch value.Kind() {
	case reflect.String:
		next.SetString(value.String() + "_rt")
		return next, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		next.SetInt(value.Int() + 1)
		return next, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		next.SetUint(value.Uint() + 1)
		return next, true
	case reflect.Pointer:
		if value.IsNil() {
			return value, false
		}
		inner, ok := perturbValue(value.Elem())
		if !ok {
			return value, false
		}
		pointer := reflect.New(value.Type().Elem())
		pointer.Elem().Set(inner)
		return pointer, true
	case reflect.Slice:
		if value.Type().Elem().Kind() != reflect.String || value.Len() == 0 {
			return value, false
		}
		next = reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := range value.Len() {
			inner, _ := perturbValue(value.Index(index))
			next.Index(index).Set(inner)
		}
		return next, true
	default:
		return value, false
	}
}

// Alternate returns a copy of schema with the non-zero instances of the field
// named by path set to its alternate values in turn, and whether the field has
// alternates and any instance changed. A value that does not fit the field's
// type is skipped; [AlternateFits] is the gate that keeps there being none.
func Alternate(schema schemamodel.Database, path string) (schemamodel.Database, bool) {
	values, ok := alternates()[path]
	if !ok || len(values) == 0 {
		return schema, false
	}
	alternated := deepCopyDatabase(schema)
	changed := false
	next := 0
	visitField(reflect.ValueOf(&alternated).Elem(), path, readWrite, func(field reflect.Value) {
		if !field.CanSet() || field.IsZero() {
			return
		}
		value := values[next%len(values)]
		next++
		replacement := reflect.ValueOf(value)
		if !replacement.Type().AssignableTo(field.Type()) || reflect.DeepEqual(field.Interface(), value) {
			return
		}
		field.Set(replacement)
		changed = true
	})
	return alternated, changed
}

// AlternateFits reports whether every alternate value of the field named by
// path can be assigned to it. A field the census does not have fits nothing.
func AlternateFits(path string) bool {
	typ, ok := fieldTypes()[path]
	if !ok {
		return false
	}
	return !slices.ContainsFunc(alternates()[path], func(value any) bool {
		return value == nil || !reflect.TypeOf(value).AssignableTo(typ)
	})
}
