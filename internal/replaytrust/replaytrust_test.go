package replaytrust_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// TestDeclared_HappyPath pins the states that parse: an absent variable trusts
// nothing, and a value strconv.ParseBool reads is the answer.
func TestDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		setup func(testing.TB)
		want  bool
	}{
		{"absent", envbooltest.Unset(replaytrust.EnvVar), false},
		{"one", envbooltest.Set(replaytrust.EnvVar, "1"), true},
		{"true", envbooltest.Set(replaytrust.EnvVar, "true"), true},
		{"zero", envbooltest.Set(replaytrust.EnvVar, "0"), false},
		{"false", envbooltest.Set(replaytrust.EnvVar, "false"), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.setup(t)
			got, err := replaytrust.Declared()
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDeclared_FailurePath pins that a present value which is not a boolean is
// a configuration error, never the default: an empty value is a different
// state from an absent one.
func TestDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"empty", "", `invalid boolean value "" for PTAH_TRUST_REPLAY_SQL`},
		{"a word", "yes-please", `invalid boolean value "yes-please" for PTAH_TRUST_REPLAY_SQL`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(replaytrust.EnvVar, test.value)(t)
			got, err := replaytrust.Declared()
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsFalse)
		})
	}
}

// noContext is a nil context, which [replaytrust.With] and [replaytrust.From]
// both accept.
var noContext context.Context

// TestFrom pins the context carrier: a context without an answer, and a nil
// one, trust nothing, so a path that forgot to carry the answer keeps the
// strict default.
func TestFrom(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{"no answer", context.Background(), false},
		{"nil", noContext, false},
		{"trusted", replaytrust.With(context.Background(), true), true},
		{"not trusted", replaytrust.With(context.Background(), false), false},
		{"trust withdrawn", replaytrust.With(replaytrust.With(context.Background(), true), false), false},
		{"trusted from nil", replaytrust.With(noContext, true), true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(replaytrust.From(test.ctx), qt.Equals, test.want)
		})
	}
}

// TestRecord_NestedRunsKeepTheOuterTrust pins that the record counts: one
// command can resolve the same URL twice, one call inside the other, and the
// inner release must not end the outer one's trust. A release is safe to call
// twice, and the URL is read trimmed of surrounding space.
func TestRecord_NestedRunsKeepTheOuterTrust(t *testing.T) {
	const url = "postgres://dev@record-nested:5432/app"
	c := qt.New(t)
	c.Assert(replaytrust.Trusted(url), qt.IsFalse)

	outer := replaytrust.Record(" " + url + "\n")
	inner := replaytrust.Record(url)
	c.Assert(replaytrust.Trusted(url), qt.IsTrue)

	inner()
	inner()
	c.Assert(replaytrust.Trusted(url), qt.IsTrue)
	c.Assert(replaytrust.Trusted("  "+url), qt.IsTrue)

	outer()
	c.Assert(replaytrust.Trusted(url), qt.IsFalse)
}

// TestRecord_IsPerURL pins that trust recorded for one dev URL says nothing
// about another, which a concurrent run may be replaying on.
func TestRecord_IsPerURL(t *testing.T) {
	c := qt.New(t)
	release := replaytrust.Record("postgres://dev@record-one:5432/app")
	t.Cleanup(release)
	c.Assert(replaytrust.Trusted("postgres://dev@record-one:5432/app"), qt.IsTrue)
	c.Assert(replaytrust.Trusted("postgres://dev@record-two:5432/app"), qt.IsFalse)
}
