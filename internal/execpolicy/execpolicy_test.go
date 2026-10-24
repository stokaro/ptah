package execpolicy_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/execpolicy"
)

func TestAuthorize_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		origin string
	}{
		{name: "a program the operator named, under no policy", ctx: context.Background(), origin: ""},
		{name: "a program the operator named, under a refusing policy", ctx: execpolicy.With(context.Background(), execpolicy.Policy{}), origin: ""},
		{name: "a configured program, as Atlas runs it", ctx: execpolicy.With(context.Background(), execpolicy.Policy{Mode: execpolicy.RunAsAtlas}), origin: "ptah.yaml external_schema"},
		{name: "a configured program the operator allowed", ctx: execpolicy.With(context.Background(), execpolicy.Policy{Mode: execpolicy.RunSelected}), origin: "ptah.yaml external_schema"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qt.New(t).Assert(execpolicy.Authorize(test.ctx, test.origin, []string{"./export.sh", "--token", "secret"}), qt.IsNil)
		})
	}
}

func TestAuthorize_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{
			// A path that forgot to carry a policy fails closed.
			name: "no policy",
			ctx:  context.Background(),
			want: `ptah.yaml external_schema runs export.sh: a program named in a project file does not run without permission`,
		},
		{
			name: "a refusing policy that names its opt-in",
			ctx:  execpolicy.With(context.Background(), execpolicy.Policy{OptIn: "pass --allow-config-exec"}),
			want: `ptah.yaml external_schema runs export.sh: a program named in a project file does not run without permission; pass --allow-config-exec to run it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := execpolicy.Authorize(test.ctx, "ptah.yaml external_schema", []string{"./bin/export.sh", "--token", "secret"})

			c.Assert(err, qt.ErrorIs, execpolicy.ErrRefused)
			// The executable is named; its arguments, which can carry
			// credentials, are not.
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestPolicy_EvaluatesEveryLocal(t *testing.T) {
	c := qt.New(t)

	c.Assert(execpolicy.Policy{Mode: execpolicy.RunAsAtlas}.EvaluatesEveryLocal(), qt.IsTrue)
	c.Assert(execpolicy.Policy{Mode: execpolicy.RunSelected}.EvaluatesEveryLocal(), qt.IsFalse)
	c.Assert(execpolicy.Policy{}.EvaluatesEveryLocal(), qt.IsFalse)
}
