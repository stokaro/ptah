package embedcutover

// White-box testing required: the binding ratchet enumerates Plan's own fields
// against digestComponents, and that list is not reachable from outside the
// package.

import (
	"reflect"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestPlan_EveryFieldIsBound is the ratchet.
//
// An approval binds to a plan's digest, so whatever the digest does not cover
// is whatever an approval does not cover. A field added to Plan or to Evidence
// without joining digestComponents widens what one approval authorizes, and
// nothing about that failure is visible: the approval still matches, the
// cutover still runs, and the thing that changed was never looked at.
//
// So the struct is enumerated and every leaf field has to appear
// (stokaro/ptah#2068).
func TestPlan_EveryFieldIsBound(t *testing.T) {
	c := qt.New(t)

	c.Assert(unboundFields(), qt.HasLen, 0, qt.Commentf(
		"each of these Plan fields must appear in digestComponents, or an approval given for one "+
			"plan authorizes a different one"))
}

// TestPlan_TheDigestCoversEveryFieldSeparately is the ratchet's other half.
//
// A label in digestComponents proves the field was thought about; it does not
// prove the field's VALUE reached the digest. A component list that wrote the
// label and then the wrong variable would satisfy the enumeration above and
// bind an approval to a plan it does not describe, so every field is moved on
// its own and the digest has to move with it.
func TestPlan_TheDigestCoversEveryFieldSeparately(t *testing.T) {
	base := samplePlan()
	tests := mutatedPlans(base)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.plan.Digest(), qt.Not(qt.Equals), base.Digest(),
				qt.Commentf("changing %s left the plan digest alone", test.name))
		})
	}
}

// samplePlan is a plan with every field set to something distinguishable.
func samplePlan() Plan {
	return Plan{
		Generation: "gen-new",
		Previous:   "gen-old",
		Schema:     "public",
		Table:      "articles",
		Column:     "embedding_new",
		Evidence: Evidence{
			VerificationDigest:   "report-1",
			VerificationPassed:   true,
			AcceptedFindings:     []string{"a finding"},
			ConsistencyMode:      "outbox",
			ConsistencyWatermark: "lsn-42",
			IndexReady:           true,
			SourceMutable:        true,
		},
		PreparedAt: time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC),
	}
}

// mutatedPlan is one field moved and the name of the field.
type mutatedPlan struct {
	name string
	plan Plan
}

// mutatedPlans produces one plan per leaf field, each differing from the base
// in exactly that field.
func mutatedPlans(base Plan) []mutatedPlan {
	return []mutatedPlan{
		{name: "Generation", plan: withPlan(base, func(p *Plan) { p.Generation = "gen-other" })},
		{name: "Previous", plan: withPlan(base, func(p *Plan) { p.Previous = "gen-other" })},
		{name: "Schema", plan: withPlan(base, func(p *Plan) { p.Schema = "other" })},
		{name: "Table", plan: withPlan(base, func(p *Plan) { p.Table = "other" })},
		{name: "Column", plan: withPlan(base, func(p *Plan) { p.Column = "other" })},
		{
			name: "Evidence.VerificationDigest",
			plan: withPlan(base, func(p *Plan) { p.Evidence.VerificationDigest = "report-2" }),
		},
		{
			name: "Evidence.VerificationPassed",
			plan: withPlan(base, func(p *Plan) { p.Evidence.VerificationPassed = false }),
		},
		{
			name: "Evidence.AcceptedFindings",
			plan: withPlan(base, func(p *Plan) { p.Evidence.AcceptedFindings = []string{"another finding"} }),
		},
		{
			name: "Evidence.AcceptedFindings count",
			plan: withPlan(base, func(p *Plan) {
				p.Evidence.AcceptedFindings = []string{"a finding", "another finding"}
			}),
		},
		{
			name: "Evidence.ConsistencyMode",
			plan: withPlan(base, func(p *Plan) { p.Evidence.ConsistencyMode = "snapshot" }),
		},
		{
			name: "Evidence.ConsistencyWatermark",
			plan: withPlan(base, func(p *Plan) { p.Evidence.ConsistencyWatermark = "lsn-43" }),
		},
		{name: "Evidence.IndexReady", plan: withPlan(base, func(p *Plan) { p.Evidence.IndexReady = false })},
		{name: "Evidence.SourceMutable", plan: withPlan(base, func(p *Plan) { p.Evidence.SourceMutable = false })},
		{
			name: "PreparedAt",
			plan: withPlan(base, func(p *Plan) { p.PreparedAt = p.PreparedAt.Add(time.Nanosecond) }),
		},
	}
}

// withPlan copies a plan and applies one change.
func withPlan(base Plan, change func(*Plan)) Plan {
	copied := base
	copied.Evidence.AcceptedFindings = append([]string(nil), base.Evidence.AcceptedFindings...)
	change(&copied)
	return copied
}

// unboundFields lists the Plan fields digestComponents does not carry.
//
// The walk lives here rather than in the test body because a test asserts and
// does not branch, which is the rule scripts/check-test-style.sh enforces.
func unboundFields() []string {
	bound := boundComponentKeys()
	var unbound []string
	for _, field := range leafFieldPaths(reflect.TypeFor[Plan](), "") {
		if bound[componentKeyFor(field)] {
			continue
		}
		unbound = append(unbound, field)
	}
	return unbound
}

// boundComponentKeys lists the component labels digestComponents writes.
func boundComponentKeys() map[string]bool {
	keys := make(map[string]bool)
	for _, component := range (Plan{}).digestComponents() {
		keys[component] = true
	}
	return keys
}

// componentKeyFor maps a Go field path onto the component label the digest uses
// for it.
func componentKeyFor(fieldPath string) string {
	lowered := strings.Replace(fieldPath, "Evidence.", "evidence.", 1)
	parts := strings.SplitN(lowered, ".", 2)
	if len(parts) != 2 {
		return snakeCase(lowered)
	}
	return parts[0] + "." + snakeCase(parts[1])
}

// snakeCase renders a Go field name the way the component labels spell it.
func snakeCase(name string) string {
	var b strings.Builder
	for index, symbol := range name {
		if symbol >= 'A' && symbol <= 'Z' {
			if index > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(symbol - 'A' + 'a')
			continue
		}
		b.WriteRune(symbol)
	}
	return b.String()
}

// leafFieldPaths lists the dotted paths of every leaf field, descending into
// the struct-valued ones so a field added to Evidence is enumerated too.
//
// time.Time is a leaf: it is a value the digest formats, not a group of fields
// to bind one by one.
func leafFieldPaths(typ reflect.Type, prefix string) []string {
	var paths []string
	for field := range typ.Fields() {
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeFor[time.Time]() {
			paths = append(paths, leafFieldPaths(field.Type, path)...)
			continue
		}
		paths = append(paths, path)
	}
	return paths
}
