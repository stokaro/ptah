package sqlext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

type commentEditorFunc func(context.Context, sqlext.CommentRequest) ([]schemaext.Object, error)

func (f commentEditorFunc) ApplySQLComments(ctx context.Context, request sqlext.CommentRequest) ([]schemaext.Object, error) {
	return f(ctx, request)
}

func commentSet(editor sqlext.CommentEditor) sqlext.Set {
	return must.Must(sqlext.NewSet(sqlext.Extension{Owner: "example.org/a", Targets: []string{"x"}, Kinds: []schemaext.Kind{gadgetKind}, Comments: editor}))
}

func TestSet_CommentsAreOneIndependentBatch(t *testing.T) {
	c := qt.New(t)
	first, second := gadgetObject(gadgetKind, "first"), gadgetObject(gadgetKind, "second")
	calls := 0
	var captured []sqlext.CommentEdit
	set := commentSet(commentEditorFunc(func(_ context.Context, request sqlext.CommentRequest) ([]schemaext.Object, error) {
		calls++
		c.Assert(request.Target, qt.Equals, "x")
		captured = request.Edits
		return []schemaext.Object{request.Edits[0].Object, request.Edits[1].Object}, nil
	}))
	result, err := set.ApplyComments(t.Context(), "x", []sqlext.CommentEdit{{Object: first, Text: "first comment"}, {Object: second, Text: ""}})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(captured, qt.HasLen, 2)
	c.Assert(captured[0].Text, qt.Equals, "first comment")
	c.Assert(captured[1].Text, qt.Equals, "")
	c.Assert(result, qt.DeepEquals, []schemaext.Object{first, second})
	captured[0].Object.Value.(*gadget).kind = "example.org/changed"
	c.Assert(result[0].Value.Kind(), qt.Equals, gadgetKind)
	c.Assert(first.Value.Kind(), qt.Equals, gadgetKind)
}

func TestSet_CommentFailureReturnsNoObjects(t *testing.T) {
	failure := errors.New("provider failed")
	tests := []struct {
		name   string
		editor commentEditorFunc
		want   error
	}{
		{name: "provider failure", editor: func(context.Context, sqlext.CommentRequest) ([]schemaext.Object, error) { return nil, failure }, want: failure},
		{name: "missing result", editor: func(context.Context, sqlext.CommentRequest) ([]schemaext.Object, error) { return nil, nil }, want: schemaext.ErrInvalidValue},
		{name: "wrong identity", editor: func(_ context.Context, r sqlext.CommentRequest) ([]schemaext.Object, error) {
			o := r.Edits[0].Object
			o.Ref.Name.Source = "other"
			return []schemaext.Object{o}, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "changed scope", editor: func(_ context.Context, r sqlext.CommentRequest) ([]schemaext.Object, error) {
			o := r.Edits[0].Object
			o.Targets = []string{"x"}
			return []schemaext.Object{o}, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "missing payload", editor: func(_ context.Context, r sqlext.CommentRequest) ([]schemaext.Object, error) {
			o := r.Edits[0].Object
			o.Value = nil
			return []schemaext.Object{o}, nil
		}, want: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := commentSet(test.editor).ApplyComments(t.Context(), "x", []sqlext.CommentEdit{{Object: childGadget(), Text: "comment"}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestSet_CanceledCommentsDoNotCallOwner(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	set := commentSet(commentEditorFunc(func(context.Context, sqlext.CommentRequest) ([]schemaext.Object, error) { calls++; return nil, nil }))
	result, err := set.ApplyComments(ctx, "x", []sqlext.CommentEdit{{Object: childGadget()}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(calls, qt.Equals, 0)
}

func TestSet_CommentsRequireAnEditorForTheSelectedTarget(t *testing.T) {
	tests := []struct {
		name, target string
		set          sqlext.Set
	}{
		{name: "missing owner", target: "x", set: sqlext.None()},
		{name: "another target", target: "other", set: commentSet(commentEditorFunc(func(context.Context, sqlext.CommentRequest) ([]schemaext.Object, error) { return nil, nil }))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := test.set.ApplyComments(t.Context(), test.target, []sqlext.CommentEdit{{Object: childGadget()}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}
