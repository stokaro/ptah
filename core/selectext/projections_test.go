package selectext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

const projectionKind schemaext.Kind = "example.org/projection/change"
const projectionModel schemaext.Kind = "example.org/projection/model"

type selectedChange struct {
	remove bool
	label  string
}

func (*selectedChange) Kind() schemaext.Kind                 { return projectionKind }
func (v *selectedChange) CloneChange() schemaext.ChangeValue { return new(*v) }
func (v *selectedChange) RemovesState() bool                 { return v.remove }

type projectionAnswer struct {
	additive selectext.AdditiveResult
	baseline selectext.BaselineResult
	changes  *[]selectext.AdditiveRequest
	objects  *[]selectext.BaselineRequest
	err      error
	cancel   context.CancelFunc
}

func (a projectionAnswer) SelectAdditions(_ context.Context, request selectext.AdditiveRequest) (selectext.AdditiveResult, error) {
	if a.changes != nil {
		*a.changes = append(*a.changes, request)
	}
	if a.cancel != nil {
		a.cancel()
	}
	return a.additive, a.err
}
func (a projectionAnswer) SubtractBaseline(_ context.Context, request selectext.BaselineRequest) (selectext.BaselineResult, error) {
	if a.objects != nil {
		*a.objects = append(*a.objects, request)
	}
	if a.cancel != nil {
		a.cancel()
	}
	return a.baseline, a.err
}
func projectionExtension(answer projectionAnswer) selectext.Extension {
	return selectext.Extension{Owner: "example.org/projection", ChangeKinds: []schemaext.Kind{projectionKind}, Additive: answer, Baseline: answer,
		Selections: []selectext.Selection{{Kind: projectionModel, Type: "projection", Naming: selectext.InSchema}}}
}

func TestProjectionRoutesIndependentBatches(t *testing.T) {
	c := qt.New(t)
	subject := ref(projectionModel, "held")
	var changes []selectext.AdditiveRequest
	var objects []selectext.BaselineRequest
	value := &selectedChange{label: "added"}
	kept := &gizmo{kind: projectionModel, Label: "new right"}
	set := must.Must(selectext.NewSet(projectionExtension(projectionAnswer{
		changes: &changes, objects: &objects,
		additive: selectext.AdditiveResult{Complete: true, Decisions: []selectext.AdditiveDecision{{Subject: subject, Kind: projectionKind, Value: value}}},
		baseline: selectext.BaselineResult{Complete: true, Decisions: []selectext.BaselineDecision{{Subject: subject, Value: kept}}},
	})))
	change := &selectedChange{remove: true, label: "mixed"}
	additions, err := set.SelectAdditions(c.Context(), selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: subject, Value: change}}})
	c.Assert(err, qt.IsNil)
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(additions, qt.HasLen, 1)
	c.Assert(additions[0].Subject, qt.Equals, subject)
	c.Assert(additions[0].Value.(*selectedChange).label, qt.Equals, "added")
	value.label = "changed answer"
	changes[0].Changes[0].Value.(*selectedChange).label = "changed request"
	c.Assert(change.label, qt.Equals, "mixed")
	c.Assert(additions[0].Value.(*selectedChange).label, qt.Equals, "added")
	held := must.Must(schemaext.NewObjects(object(projectionModel, "held", "both rights")))
	foreign := object("example.org/other/model", "unrelated", "")
	current := must.Must(schemaext.NewObjects(object(projectionModel, "held", "both rights"), foreign))
	selected, err := set.SubtractBaseline(c.Context(), selectext.BaselineRequest{Current: current, StartingPoint: held, Declared: held, DefaultSchema: " app "})
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Current.Refs(), qt.DeepEquals, held.Refs())
	c.Assert(objects[0].DefaultSchema, qt.Equals, " app ")
	kept.Label = "changed answer"
	c.Assert(selected.Refs(), qt.DeepEquals, current.Refs())
	selectedObject, found, err := selected.Get(subject)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(selectedObject.Value.Equal(&gizmo{kind: projectionModel, Label: "new right"}), qt.IsTrue)
	original, found, err := current.Get(subject)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(original.Value.Equal(&gizmo{kind: projectionModel, Label: "both rights"}), qt.IsTrue)
}

func TestProjectionRejectsIncompleteAndChangedReplies(t *testing.T) {
	subject := ref(projectionModel, "held")
	changed := subject
	changed.Name.Source = "HELD"
	additive := func(decisions ...selectext.AdditiveDecision) selectext.AdditiveResult {
		return selectext.AdditiveResult{Complete: true, Decisions: decisions}
	}
	baseline := func(decisions ...selectext.BaselineDecision) selectext.BaselineResult {
		return selectext.BaselineResult{Complete: true, Decisions: decisions}
	}
	tests := []struct {
		name   string
		answer projectionAnswer
	}{
		{name: "zero"},
		{name: "omitted", answer: projectionAnswer{additive: additive(), baseline: baseline()}},
		{name: "renamed", answer: projectionAnswer{additive: additive(selectext.AdditiveDecision{Subject: changed, Kind: projectionKind}), baseline: baseline(selectext.BaselineDecision{Subject: changed})}},
		{name: "duplicate", answer: projectionAnswer{additive: additive(selectext.AdditiveDecision{Subject: subject, Kind: projectionKind}, selectext.AdditiveDecision{Subject: subject, Kind: projectionKind}), baseline: baseline(selectext.BaselineDecision{Subject: subject}, selectext.BaselineDecision{Subject: subject})}},
		{name: "wrong kind", answer: projectionAnswer{additive: additive(selectext.AdditiveDecision{Subject: subject, Kind: projectionModel}), baseline: baseline(selectext.BaselineDecision{Subject: subject, Value: &gizmo{kind: "example.org/other/model"}})}},
		{name: "typed nil", answer: projectionAnswer{additive: additive(selectext.AdditiveDecision{Subject: subject, Kind: projectionKind, Value: (*selectedChange)(nil)}), baseline: baseline(selectext.BaselineDecision{Subject: subject, Value: (*gizmo)(nil)})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := must.Must(selectext.NewSet(projectionExtension(test.answer)))
			changes, err := set.SelectAdditions(c.Context(), selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &selectedChange{}}}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(changes, qt.IsNil)
			objects, err := set.SubtractBaseline(c.Context(), selectext.BaselineRequest{Current: must.Must(schemaext.NewObjects(object(projectionModel, "held", "")))})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(objects.Len(), qt.Equals, 0)
		})
	}
}

func TestAdditiveProjectionRejectsReturnedRemoval(t *testing.T) {
	c := qt.New(t)
	subject := ref(projectionModel, "held")
	set := must.Must(selectext.NewSet(projectionExtension(projectionAnswer{additive: selectext.AdditiveResult{Complete: true, Decisions: []selectext.AdditiveDecision{{Subject: subject, Kind: projectionKind, Value: &selectedChange{remove: true}}}}})))
	got, err := set.SelectAdditions(c.Context(), selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &selectedChange{remove: true}}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(got, qt.IsNil)
}

func TestProjectionDiscardsProviderFailureAndCancellation(t *testing.T) {
	refused := errors.New("provider failed")
	tests := []struct {
		name          string
		providerError error
		want          error
	}{
		{name: "error", providerError: refused, want: refused},
		{name: "before", want: context.Canceled},
		{name: "during", want: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, ctx, cancel := projectionFailureFixture(c.Context(), test.name, test.providerError)
			defer cancel()
			changes, err := set.SelectAdditions(ctx, selectext.AdditiveRequest{Changes: []schemaext.ChangeRecord{{Subject: ref(projectionModel, "held"), Value: &selectedChange{}}}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(changes, qt.IsNil)
			// Use a fresh context to reach the baseline service's own cancellation path.
			set, ctx, cancelBaseline := projectionFailureFixture(c.Context(), test.name, test.providerError)
			defer cancelBaseline()
			objects, err := set.SubtractBaseline(ctx, selectext.BaselineRequest{Current: must.Must(schemaext.NewObjects(object(projectionModel, "held", "")))})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(objects.Len(), qt.Equals, 0)
		})
	}
}

func TestProjectionRequiresSelectedOwnersAndContext(t *testing.T) {
	tests := []struct {
		name string
		set  selectext.Set
		ctx  context.Context
		want error
	}{
		{name: "unselected", ctx: t.Context(), want: selectext.ErrUnselected},
		{name: "nil context", set: selectext.None(), want: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := test.ctx
			changes, err := test.set.SelectAdditions(ctx, selectext.AdditiveRequest{})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(changes, qt.IsNil)
			objects, err := test.set.SubtractBaseline(ctx, selectext.BaselineRequest{})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(objects.Len(), qt.Equals, 0)
		})
	}
}

// projectionFailureFixture sets dispatch conditions without conditional assertions.
func projectionFailureFixture(parent context.Context, mode string, failure error) (selectext.Set, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	answer := projectionAnswer{err: failure}
	switch mode {
	case "before":
		cancel()
	case "during":
		answer.cancel = cancel
	}
	return must.Must(selectext.NewSet(projectionExtension(answer))), ctx, cancel
}

func TestProjectionRegistrationRejectsAmbiguousServices(t *testing.T) {
	tests := []struct {
		name       string
		extensions []selectext.Extension
	}{
		{name: "service without changes", extensions: []selectext.Extension{{Owner: "example.org/projection", Additive: projectionAnswer{}}}},
		{name: "changes without service", extensions: []selectext.Extension{{Owner: "example.org/projection", ChangeKinds: []schemaext.Kind{projectionKind}}}},
		{name: "invalid kind", extensions: []selectext.Extension{{Owner: "example.org/projection", ChangeKinds: []schemaext.Kind{""}, Additive: projectionAnswer{}}}},
		{name: "repeated kind", extensions: []selectext.Extension{projectionExtension(projectionAnswer{}), {Owner: "example.org/other", ChangeKinds: []schemaext.Kind{projectionKind}, Additive: projectionAnswer{}}}},
		{name: "baseline without models", extensions: []selectext.Extension{{Owner: "example.org/projection", Baseline: projectionAnswer{}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, err := selectext.NewSet(test.extensions...)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}
