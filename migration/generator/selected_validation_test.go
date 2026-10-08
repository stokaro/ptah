package generator_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

type recordedValidator struct {
	*engine.Runtime
	requests []schemavalidation.Request
	contexts []context.Context
	failAt   int
	failure  error
	cancel   context.CancelFunc
}

func (s *recordedValidator) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	s.requests = append(s.requests, request)
	s.contexts = append(s.contexts, ctx)
	result, err := s.Runtime.ValidateSchema(ctx, request)
	if err != nil {
		return result, err
	}
	if len(s.requests) == s.failAt {
		if s.cancel != nil {
			s.cancel()
		}
		return result, s.failure
	}
	return result, nil
}

func TestGenerateMigrationValidatesDesiredAndRollbackWithSelectedService(t *testing.T) {
	c := qt.New(t)
	selected := &recordedValidator{Runtime: must.Must(builtin.New())}
	files, err := generator.GenerateMigration(t.Context(), selectedRenderingOptions(c, selected))
	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(selected.requests, qt.HasLen, 2)
	for i, request := range selected.requests {
		c.Assert(selected.contexts[i], qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "sqlite")
		c.Assert(request.Capabilities, qt.Not(qt.HasLen), 0)
	}
	c.Assert(selected.requests[0].Schema.Tables, qt.HasLen, 1)
	c.Assert(selected.requests[0].Schema.Tables[0].Name, qt.Equals, "widgets")
	c.Assert(selected.requests[1].Schema.Tables, qt.HasLen, 0)
}

func TestGenerateMigrationPublishesNothingAfterSelectedValidationFails(t *testing.T) {
	failure := errors.New("selected validation failed")
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			c := qt.New(t)
			selected := &recordedValidator{Runtime: must.Must(builtin.New()), failAt: failAt, failure: failure}
			opts := selectedRenderingOptions(c, selected)
			files, err := generator.GenerateMigration(t.Context(), opts)
			c.Assert(err, qt.ErrorIs, failure)
			c.Assert(files, qt.IsNil)
			c.Assert(selected.requests, qt.HasLen, failAt)
			entries, err := os.ReadDir(opts.OutputDir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}

func TestGenerateMigrationPublishesNothingAfterRollbackValidationCancels(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	selected := &recordedValidator{Runtime: must.Must(builtin.New()), failAt: 2, cancel: cancel}
	opts := selectedRenderingOptions(c, selected)
	files, err := generator.GenerateMigration(ctx, opts)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(files, qt.IsNil)
	c.Assert(selected.requests, qt.HasLen, 2)
	entries, err := os.ReadDir(opts.OutputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}
