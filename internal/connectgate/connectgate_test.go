package connectgate_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/connectgate"
)

var errRefused = errors.New("refused")

func refuseYDB(dialect string) error {
	if dialect == "ydb" {
		return errRefused
	}
	return nil
}

func refuseAll(string) error { return errRefused }

// A context with no refusal, or a nil one, lets every dialect through, and a
// refusal lets through what it does not name.
func TestCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		dialect string
	}{
		{name: "no refusal", ctx: context.Background(), dialect: "ydb"},
		{name: "a nil context", ctx: nil, dialect: "ydb"},
		{name: "a dialect the refusal does not name", ctx: connectgate.With(context.Background(), refuseYDB),
			dialect: "postgres"},
		{name: "a context derived from a gated one, after the refusal is replaced",
			ctx: connectgate.With(connectgate.With(context.Background(), refuseAll), refuseYDB), dialect: "postgres"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(connectgate.Check(test.ctx, test.dialect), qt.IsNil)
		})
	}
}

// The refusal reaches every context derived from the one it was put on.
func TestCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "the gated context", ctx: connectgate.With(context.Background(), refuseYDB)},
		{name: "a context derived from it", ctx: func() context.Context {
			ctx, cancel := context.WithCancel(connectgate.With(context.Background(), refuseYDB))
			cancel()
			return ctx
		}()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(connectgate.Check(test.ctx, "ydb"), qt.ErrorIs, errRefused)
		})
	}
}
