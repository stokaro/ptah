package coverage_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
)

// allReasons is every reason this build understands, written out rather than
// read from the package, so a constant added without being added to the closed
// list is a change to this file too.
var allReasons = []coverage.Reason{
	coverage.NotInspected,
	coverage.OutsideScope,
	coverage.Unsupported,
	coverage.SuppressedByPolicy,
	coverage.Unresolved,
}

// allProvenances is every provenance this build understands, written out for the
// reason [allReasons] is.
var allProvenances = []coverage.Provenance{
	coverage.Declared,
	coverage.Observed,
	coverage.DerivedFromTarget,
	coverage.DerivedFromFact,
	coverage.Configured,
	coverage.Defaulted,
	coverage.Inferred,
	coverage.Unavailable,
}

// TestEveryReasonIsInTheClosedList is the guard on the list a directive is
// parsed against. A reason declared as a constant but left out of that list
// serializes into a document no build can read back, which is the failure the
// whole package is built to prevent, arriving from the encoder's side.
func TestEveryReasonIsInTheClosedList(t *testing.T) {
	for _, reason := range allReasons {
		t.Run(string(reason), func(t *testing.T) {
			c := qt.New(t)

			parsed, err := coverage.ParseReason(string(reason))

			c.Assert(err, qt.IsNil)
			c.Assert(parsed, qt.Equals, reason)
			c.Assert(reason.Valid(), qt.IsTrue)
		})
	}
}

// TestEveryProvenanceIsInTheClosedList is [TestEveryReasonIsInTheClosedList] for
// the other axis.
func TestEveryProvenanceIsInTheClosedList(t *testing.T) {
	for _, provenance := range allProvenances {
		t.Run(string(provenance), func(t *testing.T) {
			c := qt.New(t)

			parsed, err := coverage.ParseProvenance(string(provenance))

			c.Assert(err, qt.IsNil)
			c.Assert(parsed, qt.Equals, provenance)
			c.Assert(provenance.Valid(), qt.IsTrue)
		})
	}
}

// TestUnspecifiedIsValidButUnparseable pins the asymmetry the zero values sit
// on. A record may decline to give a reason -- that is what a hand-authored
// directive is -- but the empty token is spelled by leaving the attribute out,
// never by writing `reason=`, so parsing one is an error.
func TestUnspecifiedIsValidButUnparseable(t *testing.T) {
	c := qt.New(t)

	c.Assert(coverage.ReasonUnspecified.Valid(), qt.IsTrue)
	c.Assert(coverage.ProvenanceUnspecified.Valid(), qt.IsTrue)

	_, reasonErr := coverage.ParseReason("")
	c.Assert(reasonErr, qt.ErrorMatches, `unknown coverage reason "": valid reasons are .*`)

	_, provenanceErr := coverage.ParseProvenance("")
	c.Assert(provenanceErr, qt.ErrorMatches, `unknown coverage provenance "": valid provenances are .*`)
}

// TestParseRefusesUnknownTokens is the same contract [coverage.ParseKind]
// carries, on the two axes added beside it: a token nothing understands is
// refused rather than dropped, because dropping it turns a record that said
// WHY into one that says nothing.
func TestParseRefusesUnknownTokens(t *testing.T) {
	tests := []struct {
		name    string
		parse   func(string) error
		token   string
		wantErr string
	}{
		{
			name:    "an invented reason",
			parse:   func(token string) error { _, err := coverage.ParseReason(token); return err },
			token:   "probably-fine",
			wantErr: `unknown coverage reason "probably-fine": valid reasons are not-inspected, outside-scope, unsupported, suppressed, unresolved`,
		},
		{
			name:    "a provenance spelled as a reason",
			parse:   func(token string) error { _, err := coverage.ParseReason(token); return err },
			token:   "observed",
			wantErr: `unknown coverage reason "observed": valid reasons are .*`,
		},
		{
			name:    "an invented provenance",
			parse:   func(token string) error { _, err := coverage.ParseProvenance(token); return err },
			token:   "vibes",
			wantErr: `unknown coverage provenance "vibes": valid provenances are declared, observed, derived-from-target, derived-from-fact, configured, defaulted, inferred, unavailable`,
		},
		{
			name:    "a reason spelled as a provenance",
			parse:   func(token string) error { _, err := coverage.ParseProvenance(token); return err },
			token:   "outside-scope",
			wantErr: `unknown coverage provenance "outside-scope": valid provenances are .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.parse(test.token)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestProvenanceDirect pins the certainty axis. It is derived from provenance
// rather than stored beside it, so the two can never disagree, and the split is
// asserted here rather than inferred from the constant names.
func TestProvenanceDirect(t *testing.T) {
	tests := []struct {
		provenance coverage.Provenance
		want       bool
	}{
		{provenance: coverage.Declared, want: true},
		{provenance: coverage.Observed, want: true},
		{provenance: coverage.Configured, want: true},
		{provenance: coverage.DerivedFromTarget, want: false},
		{provenance: coverage.DerivedFromFact, want: false},
		{provenance: coverage.Defaulted, want: false},
		{provenance: coverage.Inferred, want: false},
		{provenance: coverage.Unavailable, want: false},
		{provenance: coverage.ProvenanceUnspecified, want: false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s is direct: %t", test.provenance, test.want), func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.provenance.Direct(), qt.Equals, test.want)
		})
	}
}

// TestEveryReasonExplainsItself is what keeps the taxonomy from growing a state
// no surface can print. A reason that reaches a user as an empty clause is
// indistinguishable from the coarse record it replaced, so every reason in the
// closed list must produce a sentence, and the one that gives no reason must
// produce none.
func TestEveryReasonExplainsItself(t *testing.T) {
	for _, reason := range allReasons {
		t.Run(string(reason), func(t *testing.T) {
			c := qt.New(t)

			clause := coverage.Object{Kind: coverage.Extension, Reason: reason}.Explain()

			c.Assert(clause, qt.Not(qt.Equals), "")
			c.Assert(strings.TrimSpace(clause), qt.Equals, clause)
			c.Assert(strings.HasSuffix(clause, "."), qt.IsFalse)
		})
	}

	c := qt.New(t)
	c.Assert(coverage.Object{Kind: coverage.Extension}.Explain(), qt.Equals, "")
}

// TestExplainDistinguishesTheReasonsThatMatter is the point of the taxonomy,
// asserted as the thing a user actually receives. Four records that
// `ptah:not-described extension` flattens into one line here produce four
// different sentences, and the two that share a reason but differ in provenance
// are two of them.
func TestExplainDistinguishesTheReasonsThatMatter(t *testing.T) {
	tests := []struct {
		name   string
		object coverage.Object
		want   string
	}{
		{
			name:   "a refused catalog",
			object: coverage.Refused(coverage.Role),
			want:   "the read was refused the catalog that would have listed them",
		},
		{
			name:   "a kind nothing looked for",
			object: coverage.Object{Kind: coverage.Role, Reason: coverage.NotInspected},
			want:   "nothing in this run looked for them",
		},
		{
			name: "a selection that ruled the kind out",
			object: coverage.Object{
				Kind:       coverage.Extension,
				Reason:     coverage.OutsideScope,
				Provenance: coverage.Configured,
			},
			want: "the selection this run was given put them outside it",
		},
		{
			name: "a target that has no such objects",
			object: coverage.Object{
				Kind:       coverage.Extension,
				Reason:     coverage.Unsupported,
				Provenance: coverage.DerivedFromTarget,
			},
			want: "this target cannot report them",
		},
		{
			name: "a format that cannot express the kind",
			object: coverage.Object{
				Kind:       coverage.VirtualTable,
				Reason:     coverage.Unsupported,
				Provenance: coverage.DerivedFromFact,
			},
			want: "the format that description is written in cannot express them",
		},
		{
			name: "a policy that left the kind out",
			object: coverage.Object{
				Kind:       coverage.Sequence,
				Reason:     coverage.SuppressedByPolicy,
				Provenance: coverage.Defaulted,
			},
			want: "a compatibility policy left them out of the description",
		},
		{
			name:   "a reference that never resolved",
			object: coverage.Object{Kind: coverage.Schema, Reason: coverage.Unresolved},
			want:   "a reference the description depends on never resolved",
		},
	}

	seen := make(map[string]string, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.object.Explain(), qt.Equals, test.want)
		})
		seen[test.want] = test.name
	}

	c := qt.New(t)
	c.Assert(seen, qt.HasLen, len(tests))
}

// TestDirectivesCarryReasonAndProvenanceAcrossTheProcessBoundary is the whole
// point of putting the two axes in the grammar rather than only in memory.
// `schema inspect` and `schema apply` are two processes, so a reason that lives
// only in the reader's Set is a reason the comparator never sees.
func TestDirectivesCarryReasonAndProvenanceAcrossTheProcessBoundary(t *testing.T) {
	for _, reason := range allReasons {
		for _, provenance := range allProvenances {
			name := fmt.Sprintf("%s/%s", reason, provenance)
			t.Run(name, func(t *testing.T) {
				c := qt.New(t)
				set := coverage.Set{}.With(
					coverage.Object{Kind: coverage.Extension, Reason: reason, Provenance: provenance},
					coverage.Object{
						Kind:       coverage.Schema,
						Name:       "extra reports",
						Reason:     reason,
						Provenance: provenance,
					},
				)

				directives := set.Directives()
				c.Assert(directives, qt.HasLen, 2)

				var document strings.Builder
				for _, directive := range directives {
					fmt.Fprintf(&document, "// %s\n", directive)
				}

				decoded, err := coverage.DecodeHeader(document.String())
				c.Assert(err, qt.IsNil)
				c.Assert(decoded, qt.DeepEquals, set)

				// What came back still protects what the writer meant to
				// protect, and can still say why.
				c.Assert(decoded.Describes(coverage.Extension, "citext"), qt.IsFalse)
				limit, ok := decoded.Limit(coverage.Extension, "citext")
				c.Assert(ok, qt.IsTrue)
				c.Assert(limit.Reason, qt.Equals, reason)
				c.Assert(limit.Provenance, qt.Equals, provenance)
			})
		}
	}
}

// TestDirectivesOmitUnspecifiedAttributes keeps the coarse line the coarse line.
// A hand-authored document naming only a kind must round-trip through a Ptah
// process unchanged, or Ptah would be rewriting a claim its author did not make.
func TestDirectivesOmitUnspecifiedAttributes(t *testing.T) {
	tests := []struct {
		name string
		set  coverage.Set
		want []string
	}{
		{
			name: "no reason and no provenance",
			set:  coverage.Set{}.WithKind(coverage.Extension),
			want: []string{"ptah:not-described extension"},
		},
		{
			name: "a reason with no provenance",
			set: coverage.Set{}.With(
				coverage.Object{Kind: coverage.Extension, Reason: coverage.OutsideScope}),
			want: []string{"ptah:not-described extension reason=outside-scope"},
		},
		{
			name: "a provenance with no reason",
			set: coverage.Set{}.With(
				coverage.Object{Kind: coverage.Extension, Provenance: coverage.Observed}),
			want: []string{"ptah:not-described extension provenance=observed"},
		},
		{
			name: "both, on a named object",
			set: coverage.Set{}.With(coverage.Object{
				Kind:       coverage.Schema,
				Name:       "extra",
				Reason:     coverage.NotInspected,
				Provenance: coverage.Observed,
			}),
			want: []string{`ptah:not-described schema reason=not-inspected provenance=observed "extra"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.set.Directives(), qt.DeepEquals, test.want)

			var document strings.Builder
			for _, directive := range test.set.Directives() {
				fmt.Fprintf(&document, "// %s\n", directive)
			}
			decoded, err := coverage.DecodeHeader(document.String())
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.set)
		})
	}
}

// TestDecodeHeaderRefusesMalformedAttributes pins the refusal side of the
// attribute grammar. Every row here is a line whose safety claim this build
// cannot read, and reading it as a claim-free directive is the failure mode the
// package exists to prevent.
func TestDecodeHeaderRefusesMalformedAttributes(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "an unknown attribute key",
			document: "// ptah:not-described extension certainty=high\n",
			wantErr:  `unknown ptah:not-described attribute "certainty": valid attributes are reason, provenance`,
		},
		{
			name:     "an unknown reason value",
			document: "// ptah:not-described extension reason=probably-fine\n",
			wantErr:  `unknown coverage reason "probably-fine": valid reasons are .*`,
		},
		{
			name:     "an unknown provenance value",
			document: "// ptah:not-described extension provenance=vibes\n",
			wantErr:  `unknown coverage provenance "vibes": valid provenances are .*`,
		},
		{
			name:     "a reason given twice",
			document: "// ptah:not-described extension reason=unsupported reason=outside-scope\n",
			wantErr:  `malformed ptah:not-described directive .*: reason given twice`,
		},
		{
			name:     "a provenance given twice",
			document: "// ptah:not-described extension provenance=observed provenance=defaulted\n",
			wantErr:  `malformed ptah:not-described directive .*: provenance given twice`,
		},
		{
			name:     "an attribute after the name",
			document: `// ptah:not-described schema "extra" reason=outside-scope` + "\n",
			wantErr:  `malformed ptah:not-described directive .*: name must be a quoted string`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := coverage.DecodeHeader(test.document)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.IsZero(), qt.IsTrue)
		})
	}
}

// TestLimitExplainsWhatDescribesRefuses pins the pair. Describes is written in
// terms of Limit, so a comparator's decision and the sentence a surface prints
// about that decision can never come from different records.
func TestLimitExplainsWhatDescribesRefuses(t *testing.T) {
	refusedRoles := coverage.Set{}.With(coverage.Refused(coverage.Role))
	unreadSchema := coverage.Set{}.With(coverage.Object{
		Kind:       coverage.Schema,
		Name:       "extra",
		Reason:     coverage.OutsideScope,
		Provenance: coverage.Configured,
	})

	tests := []struct {
		name       string
		set        coverage.Set
		kind       coverage.Kind
		schema     string
		names      []string
		wantLimit  bool
		wantReason coverage.Reason
	}{
		{
			name:       "a whole kind the read was refused",
			set:        refusedRoles,
			kind:       coverage.Role,
			names:      []string{"admin_user"},
			wantLimit:  true,
			wantReason: coverage.NotInspected,
		},
		{
			name:      "a kind the same set does describe",
			set:       refusedRoles,
			kind:      coverage.Sequence,
			names:     []string{"order_seq"},
			wantLimit: false,
		},
		{
			// A schema nobody read explains everything in it, so the sequence
			// gets the SCHEMA's reason rather than none of its own.
			name:       "an object owned by an unread schema",
			set:        unreadSchema,
			kind:       coverage.Sequence,
			schema:     "extra",
			names:      []string{"extra.order_seq"},
			wantLimit:  true,
			wantReason: coverage.OutsideScope,
		},
		{
			name:      "the same kind in a schema that was read",
			set:       unreadSchema,
			kind:      coverage.Sequence,
			schema:    "public",
			names:     []string{"public.order_seq"},
			wantLimit: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			limit, ok := test.set.LimitIn(test.kind, test.schema, test.names...)

			c.Assert(ok, qt.Equals, test.wantLimit)
			c.Assert(limit.Reason, qt.Equals, test.wantReason)
			c.Assert(test.set.DescribesIn(test.kind, test.schema, test.names...), qt.Equals, !test.wantLimit)
		})
	}
}

// TestWholeKindLimitWinsOverANamedOne pins which record explains an object two
// records both cover. The whole-kind record is the broader statement, and it is
// the one a user needs: "the read was refused the role catalog" says more than
// "this one role was not described".
//
// The set is a literal rather than a builder chain, deliberately. A set the
// builders produced is normalized, and normalization sorts the whole-kind
// record first, so taking the first match would be right by accident and the
// preference would be untested. A Set does not always arrive normalized: a
// catalog.Database decoded from JSON carries whatever order the document had, and that
// is the case this asserts.
func TestWholeKindLimitWinsOverANamedOne(t *testing.T) {
	c := qt.New(t)
	set := coverage.Set{Objects: []coverage.Object{
		{Kind: coverage.Role, Name: "admin_user", Reason: coverage.OutsideScope},
		coverage.Refused(coverage.Role),
	}}

	limit, ok := set.Limit(coverage.Role, "admin_user")

	c.Assert(ok, qt.IsTrue)
	c.Assert(limit.WholeKind(), qt.IsTrue)
	c.Assert(limit.Reason, qt.Equals, coverage.NotInspected)
	c.Assert(limit.Provenance, qt.Equals, coverage.Observed)
}

// TestValidate_HappyPath is the check a producer runs before it writes: a set
// whose every axis is inside its closed list serializes into a line every later
// process can read back.
func TestValidate_HappyPath(t *testing.T) {
	c := qt.New(t)
	set := coverage.Set{}.With(
		coverage.Refused(coverage.Role),
		coverage.Object{Kind: coverage.Extension},
		coverage.Object{
			Kind:       coverage.Schema,
			Name:       "extra",
			Reason:     coverage.OutsideScope,
			Provenance: coverage.Configured,
		},
	)

	c.Assert(set.Validate(), qt.IsNil)
}

// TestValidate_FailurePath pins the refusal. A record carrying a token outside
// the closed lists serializes into a line every later process refuses, so the
// set is refused here instead, where the producer can still be pointed at.
func TestValidate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		set     coverage.Set
		wantErr string
	}{
		{
			name:    "a kind outside the list",
			set:     coverage.Set{}.With(coverage.Object{Kind: coverage.Kind("publication")}),
			wantErr: `unknown coverage kind "publication": valid kinds are .*`,
		},
		{
			name: "a reason outside the list",
			set: coverage.Set{}.With(
				coverage.Object{Kind: coverage.Role, Reason: coverage.Reason("probably-fine")}),
			wantErr: `unknown coverage reason "probably-fine": valid reasons are .*`,
		},
		{
			name: "a provenance outside the list",
			set: coverage.Set{}.With(
				coverage.Object{Kind: coverage.Role, Provenance: coverage.Provenance("vibes")}),
			wantErr: `unknown coverage provenance "vibes": valid provenances are .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.set.Validate()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestMergeKeepsBothExplanations pins what happens when two descriptions decline
// the same kind for different reasons. Both are true -- a read was refused it
// AND a policy omitted it -- and collapsing them would pick one sentence to
// print and discard the other.
func TestMergeKeepsBothExplanations(t *testing.T) {
	c := qt.New(t)
	refused := coverage.Set{}.With(coverage.Refused(coverage.Role))
	suppressed := coverage.Set{}.With(coverage.Object{
		Kind:       coverage.Role,
		Reason:     coverage.SuppressedByPolicy,
		Provenance: coverage.Defaulted,
	})

	merged := refused.Merge(suppressed)

	c.Assert(merged.Objects, qt.DeepEquals, []coverage.Object{
		{Kind: coverage.Role, Reason: coverage.NotInspected, Provenance: coverage.Observed},
		{Kind: coverage.Role, Reason: coverage.SuppressedByPolicy, Provenance: coverage.Defaulted},
	})
	c.Assert(merged.Describes(coverage.Role, "admin_user"), qt.IsFalse)
	c.Assert(merged.Directives(), qt.DeepEquals, []string{
		"ptah:not-described role reason=not-inspected provenance=observed",
		"ptah:not-described role reason=suppressed provenance=defaulted",
	})
}
