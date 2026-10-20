package schemasource_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/schemasource"
)

type contextFormats struct {
	decode func(context.Context) (*schemamodel.Database, error)
}

func (r contextFormats) ReadYAML(ctx context.Context, _ []byte) (*schemamodel.Database, error) {
	return r.decode(ctx)
}
func (r contextFormats) ReadSQL(ctx context.Context, _ []byte, _ string) (*schemamodel.Database, error) {
	return r.decode(ctx)
}
func (r contextFormats) ReadHCL(ctx context.Context, _ []byte, _, _ string) (*schemamodel.Database, error) {
	return r.decode(ctx)
}

type readerContextKey struct{}

func TestRunCancelsOutputInterpretation(t *testing.T) {
	for _, format := range []string{"sql", "yaml", "hcl"} {
		t.Run(format, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), readerContextKey{}, "selected operation"))
			defer cancel()
			calls := 0
			readers := contextFormats{decode: func(received context.Context) (*schemamodel.Database, error) {
				calls++
				c.Assert(received.Value(readerContextKey{}), qt.Equals, "selected operation")
				cancel()
				c.Assert(received.Err(), qt.ErrorIs, context.Canceled)
				// A reader returning a result after cancellation cannot publish it.
				return &schemamodel.Database{}, nil
			}}
			db, err := schemasource.Run(ctx, readers, schemasource.Command{Args: helperArgs(), Env: helperEnv(format), Format: format})
			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(db, qt.IsNil)
			c.Assert(calls, qt.Equals, 1)
		})
	}
}

func TestRunDeadlineIncludesOutputInterpretation(t *testing.T) {
	c := qt.New(t)
	parent, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	parentDeadline, _ := parent.Deadline()
	calls := 0
	readers := contextFormats{decode: func(received context.Context) (*schemamodel.Database, error) {
		calls++
		deadline, ok := received.Deadline()
		c.Assert(ok, qt.IsTrue)
		c.Assert(deadline.Before(parentDeadline), qt.IsTrue)
		<-received.Done()
		return nil, received.Err()
	}}
	db, err := schemasource.Run(parent, readers, schemasource.Command{Args: helperArgs(), Env: helperEnv("yaml"), Format: "yaml", Timeout: 2 * time.Second})
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(db, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}
