package annotation_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

type retentionFunc func(context.Context, annotation.RetentionRequest) ([]bool, error)

func (f retentionFunc) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	return f(ctx, request)
}

func TestRetained_BatchesAndIsolatesOwnerInput(t *testing.T) {
	c := qt.New(t)
	extension := widget("example.org/widget", "ptah:schema:widget")
	calls := 0
	var captured annotation.RetentionRequest
	extension.Retainer = retentionFunc(func(_ context.Context, request annotation.RetentionRequest) ([]bool, error) {
		calls++
		captured = request
		request.Declarations[0].Attributes["level"] = "changed"
		request.Declarations[0].Targets[0] = "changed"
		request.Tables[0].Name = "changed"
		request.ExportedTables[0].Table.Name = "changed"
		return []bool{true, false}, nil
	})
	set := must.Must(annotation.NewSet(extension))
	request := annotation.RetentionRequest{
		Declarations: []annotation.Declaration{
			{Directive: "ptah:schema:widget", Attributes: map[string]string{"level": "high"}, Targets: []string{"postgresql"}},
			{Directive: "ptah:schema:widget", Attributes: map[string]string{"level": "low"}},
		},
		Tables:         annotation.Tables{{Name: "items"}},
		ExportedTables: []annotation.RetainedTable{{Table: annotation.Table{Name: "items"}, Facets: must.Must(schemaext.NewFacets(&level{Value: "high"}))}},
	}
	result, err := set.Retained(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{true, false})
	c.Assert(calls, qt.Equals, 1)
	c.Assert(captured.Declarations, qt.HasLen, 2)
	c.Assert(captured.ExportedTables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{levelKind})
	c.Assert(request.Declarations[0].Attributes["level"], qt.Equals, "high")
	c.Assert(request.Declarations[0].Targets, qt.DeepEquals, []string{"postgresql"})
	c.Assert(request.Tables[0].Name, qt.Equals, "items")
	c.Assert(request.ExportedTables[0].Table.Name, qt.Equals, "items")
}

func TestRetained_MissingServiceRefusesCleanup(t *testing.T) {
	c := qt.New(t)
	set := must.Must(annotation.NewSet(widget("example.org/widget", "ptah:schema:widget")))
	result, err := set.Retained(context.Background(), annotation.RetentionRequest{Declarations: []annotation.Declaration{{Directive: "ptah:schema:widget"}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{false})
}

func TestRetained_FailurePath(t *testing.T) {
	failure := errors.New("retention failure")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	extension := widget("example.org/widget", "ptah:schema:widget")
	short := extension
	short.Retainer = retentionFunc(func(context.Context, annotation.RetentionRequest) ([]bool, error) { return nil, nil })
	broken := extension
	broken.Retainer = retentionFunc(func(context.Context, annotation.RetentionRequest) ([]bool, error) { return nil, failure })
	rows := []struct {
		name      string
		set       annotation.Set
		ctx       context.Context
		directive string
		want      error
	}{
		{name: "unselected", ctx: context.Background(), want: annotation.ErrUnselected},
		{name: "nil context", set: annotation.None(), want: schemaext.ErrInvalidValue},
		{name: "canceled", set: annotation.None(), ctx: canceled, want: context.Canceled},
		{name: "unowned", set: annotation.None(), ctx: context.Background(), directive: "ptah:schema:widget", want: schemaext.ErrInvalidValue},
		{name: "short reply", set: must.Must(annotation.NewSet(short)), ctx: context.Background(), directive: "ptah:schema:widget", want: schemaext.ErrInvalidValue},
		{name: "owner error", set: must.Must(annotation.NewSet(broken)), ctx: context.Background(), directive: "ptah:schema:widget", want: failure},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := row.set.Retained(row.ctx, annotation.RetentionRequest{Declarations: []annotation.Declaration{{Directive: row.directive}}})
			c.Assert(err, qt.ErrorIs, row.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestRetained_CancellationDiscardsOwnerReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	extension := widget("example.org/widget", "ptah:schema:widget")
	extension.Retainer = retentionFunc(func(context.Context, annotation.RetentionRequest) ([]bool, error) { cancel(); return []bool{true}, nil })
	set := must.Must(annotation.NewSet(extension))
	result, err := set.Retained(ctx, annotation.RetentionRequest{Declarations: []annotation.Declaration{{Directive: "ptah:schema:widget"}}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}

func TestRetained_RoutesOwnersAndFiltersModels(t *testing.T) {
	c := qt.New(t)
	first := widget("example.org/widget", "ptah:schema:widget")
	var calls []string
	first.Retainer = retentionFunc(func(context.Context, annotation.RetentionRequest) ([]bool, error) {
		calls = append(calls, "first")
		return []bool{true, false}, nil
	})
	var captured annotation.RetentionRequest
	second := annotation.Extension{Owner: "example.org/second", Directives: []annotation.Directive{{Name: "ptah:schema:second"}}, Decode: noContributions, Coverage: annotation.Unlimited(levelCoverage), Retainer: retentionFunc(func(_ context.Context, request annotation.RetentionRequest) ([]bool, error) {
		calls = append(calls, "second")
		captured = request
		return []bool{true}, nil
	})}
	set := must.Must(annotation.NewSet(first, second))
	objects := must.Must(schemaext.NewObjects(schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(levelKind), Name: objectidentity.Part{Source: "item", Normalized: "item"}}, Value: &level{Value: "high"}}))
	result, err := set.Retained(context.Background(), annotation.RetentionRequest{
		Declarations: []annotation.Declaration{{Directive: "ptah:schema:widget"}, {Directive: "ptah:schema:second"}, {Directive: "ptah:schema:widget"}},
		Objects:      objects, ExportedTables: []annotation.RetainedTable{{Table: annotation.Table{Name: "items"}, Facets: must.Must(schemaext.NewFacets(&level{Value: "high"}))}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{true, true, false})
	c.Assert(calls, qt.DeepEquals, []string{"first", "second"})
	c.Assert(captured.Declarations, qt.HasLen, 1)
	c.Assert(captured.Objects.Len(), qt.Equals, 0)
	c.Assert(captured.ExportedTables[0].Facets.IsZero(), qt.IsTrue)
}
