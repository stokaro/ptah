// Package execpolicy decides whether a program that a project file or a test
// file names may run.
//
// A file in a repository can name a program: an atlas.hcl `data "external"` or
// `data "external_schema"`, a ptah.yaml `external_schema` block or
// `migration.pre_up_hook`, an `external` step in a `.test.hcl` case. Reading
// the file must not be enough to run it. The two binaries answer differently,
// and the answer belongs to the binary rather than to the code they share:
//
//   - ptah-compat runs such a program as Atlas does, because a project that
//     switched from Atlas expects its atlas.hcl to behave the same way.
//   - Native ptah refuses one unless the operator allowed it with
//     --allow-config-exec, and reads a project file lazily, so a program that
//     the selected env does not need never runs even when allowed.
//
// A program the operator names on the command line, such as --schema-cmd or
// --pre-up-hook, is not a configured one and is never refused here: naming it
// is the permission.
//
// Each binary attaches its policy to the command's context once, with [With],
// and every place that is about to start a configured program asks
// [Authorize] with that context. A context with no policy refuses, so a path
// that forgot to carry one fails closed.
//
// This is a permission check, not a sandbox: an allowed program runs with the
// permissions of the user who runs ptah.
package execpolicy

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// Mode is what a policy does with a configured program.
type Mode uint8

const (
	// Refuse refuses a configured program. It is the zero value.
	Refuse Mode = iota
	// RunSelected runs a configured program that the selected configuration
	// needs. Values the selection does not reach are not evaluated.
	RunSelected
	// RunAsAtlas runs a configured program the way Atlas does: every local of
	// an atlas.hcl is evaluated, so a program a local reads runs whichever env
	// is selected.
	RunAsAtlas
)

// Policy is one binary's answer for configured programs.
type Policy struct {
	// Mode is what happens to a configured program.
	Mode Mode
	// OptIn is how an operator allows a refused program, as a refusal says it,
	// for example "pass --allow-config-exec". Empty leaves it out.
	OptIn string
}

// ErrRefused is the error [Authorize] wraps when it refuses a program.
var ErrRefused = errors.New("a program named in a project file does not run without permission")

type contextKey struct{}

// With returns ctx carrying policy.
func With(ctx context.Context, policy Policy) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, policy)
}

// From returns the policy ctx carries, or a refusing one.
func From(ctx context.Context) Policy {
	if ctx == nil {
		return Policy{}
	}
	policy, _ := ctx.Value(contextKey{}).(Policy)
	return policy
}

// EvaluatesEveryLocal reports whether a project file is read the way Atlas
// reads it, every local evaluated, rather than only the values the selection
// reaches.
func (p Policy) EvaluatesEveryLocal() bool {
	return p.Mode == RunAsAtlas
}

// Authorize reports whether the program in program may start. origin names
// the construct that supplied it, such as `atlas.hcl data "external" "probe"`;
// an empty origin is a program the operator named and is always allowed.
//
// The refusal names the construct and the executable, not its arguments, which
// can carry credentials.
func Authorize(ctx context.Context, origin string, program []string) error {
	return From(ctx).Authorize(origin, program)
}

// Authorize is [Authorize] for a policy already in hand, for a caller that
// checks before it has a context to carry one, such as source classification.
// It is the same decision: both spellings end here.
func (p Policy) Authorize(origin string, program []string) error {
	if origin == "" || p.Mode != Refuse {
		return nil
	}
	executable := "a program"
	if len(program) > 0 && program[0] != "" {
		executable = filepath.Base(program[0])
	}
	if p.OptIn == "" {
		return fmt.Errorf("%s runs %s: %w", origin, executable, ErrRefused)
	}
	return fmt.Errorf("%s runs %s: %w; %s to run it", origin, executable, ErrRefused, p.OptIn)
}
