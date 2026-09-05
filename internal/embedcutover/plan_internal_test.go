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

// TestPlan_TheSignedFileCoversEveryFieldSeparately is the digest test above,
// asked of the thing a person actually reads.
//
// An approval binds to the digest, so any fact the digest distinguishes and the
// FILE does not is a fact the approver signed for and could not have checked.
// stokaro/ptah#2739 is what that looks like: two plans differing only in
// whether both blocking findings were accepted rendered byte-identical apart
// from the digest, and the line naming the verification report read as evidence
// that verification passed.
//
// It reuses mutatedPlans deliberately. Two lists of what a plan binds would
// drift, and the one that drifts is this one -- the digest has its own
// enumeration test and the file had nothing.
func TestPlan_TheSignedFileCoversEveryFieldSeparately(t *testing.T) {
	base := samplePlan()
	tests := mutatedPlans(base)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.plan.IdentityLines(), qt.Not(qt.DeepEquals), base.IdentityLines(),
				qt.Commentf("changing %s left the signed file identical", test.name))
		})
	}
}

// TestPlan_AnEmptyEvidenceListStillSaysSo is the half a mutation cannot reach.
//
// Every mutation above changes a value, so a renderer that dropped empty lists
// entirely would pass all of them. What it would produce is a file that says
// nothing about accepted findings -- indistinguishable, to the person signing
// it, from one whose author had nothing to say.
func TestPlan_AnEmptyEvidenceListStillSaysSo(t *testing.T) {
	c := qt.New(t)

	lines := Plan{}.IdentityLines()

	c.Assert(lines, qt.Contains, "accepts blocking findings: none")
	c.Assert(lines, qt.Contains, "UNACCEPTED blocking findings: none")
	c.Assert(lines, qt.Contains, "consistency blockers: none")
}

// TestPlan_AcceptedAndUnacceptedFindingsAreToldApart is the report the issue
// opened on: the operator accepting both findings and the operator accepting
// neither must not sign the same sentences.
func TestPlan_AcceptedAndUnacceptedFindingsAreToldApart(t *testing.T) {
	c := qt.New(t)
	blocking := []string{"stale rows exceed the policy", "the index is not ready"}

	accepted := samplePlan()
	accepted.Evidence.AcceptedFindings = blocking
	accepted.Evidence.UnacceptedFindings = nil

	refused := samplePlan()
	refused.Evidence.AcceptedFindings = nil
	refused.Evidence.UnacceptedFindings = blocking

	c.Assert(accepted.IdentityLines(), qt.Not(qt.DeepEquals), refused.IdentityLines())
	c.Assert(accepted.IdentityLines(), qt.Contains, "accepts blocking finding: the index is not ready")
	c.Assert(refused.IdentityLines(), qt.Contains, "UNACCEPTED blocking finding: the index is not ready")
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
//
// Derived from the struct rather than written out. The hand-written list this
// replaces had fallen behind it: Evidence.ConsistencyBlockers joined Plan in
// stokaro/ptah#2646 and never got a row, so the property this test exists for
// -- that the field's VALUE reaches the digest -- was unproven for it while the
// test reported full coverage. A list that has to be extended by hand records
// the coverage somebody remembered, and the field that escapes is by definition
// the one nobody was thinking about.
//
// The label ratchet above cannot stand in for this. It proves a component with
// the right NAME is written; a component list that wrote the label and then the
// wrong variable satisfies it and binds an approval to a plan it does not
// describe.
func mutatedPlans(base Plan) []mutatedPlan {
	var mutated []mutatedPlan
	for _, field := range leafFieldPaths(reflect.TypeFor[Plan](), "") {
		mutated = append(mutated, mutatedPlan{
			name: field,
			plan: withPlan(base, func(p *Plan) { mutateLeaf(p, field) }),
		})
	}
	// One case reflection cannot reach: a list of one and a list of two are
	// different plans, and an encoding that dropped the elements after the
	// first would still move the digest for the mutation above.
	return append(mutated, mutatedPlan{
		name: "Evidence.AcceptedFindings count",
		plan: withPlan(base, func(p *Plan) {
			p.Evidence.AcceptedFindings = []string{"a finding", "another finding"}
		}),
	})
}

// mutateLeaf changes one leaf field to a value it did not hold.
//
// Each kind gets a change that cannot coincide with the base: a string gains a
// suffix, a bool flips, a list gains an element, an instant moves by the
// smallest unit the encoding records.
func mutateLeaf(plan *Plan, path string) {
	field := reflect.ValueOf(plan).Elem()
	for part := range strings.SplitSeq(path, ".") {
		field = field.FieldByName(part)
	}
	switch value := field.Interface().(type) {
	case string:
		field.SetString(value + "-mutated")
	case bool:
		field.SetBool(!value)
	case int:
		field.SetInt(int64(value) + 1)
	case []string:
		field.Set(reflect.ValueOf(append(append([]string(nil), value...), "a finding nobody named")))
	case time.Time:
		field.Set(reflect.ValueOf(value.Add(time.Nanosecond)))
	default:
		panic("mutateLeaf has no change for " + path + ": add one rather than leaving it unmutated")
	}
}

// withPlan copies a plan and applies one change.
func withPlan(base Plan, change func(*Plan)) Plan {
	copied := base
	copied.Evidence.AcceptedFindings = append([]string(nil), base.Evidence.AcceptedFindings...)
	copied.Evidence.UnacceptedFindings = append([]string(nil), base.Evidence.UnacceptedFindings...)
	copied.Evidence.ConsistencyBlockers = append([]string(nil), base.Evidence.ConsistencyBlockers...)
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
