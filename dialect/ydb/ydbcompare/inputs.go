package ydbcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

func comparisonInputs(ctx context.Context, request schemaext.ObjectComparisonRequest) (map[objectidentity.Key]stream, map[objectidentity.Key]schemaext.ParentState, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if request.Target != "ydb" {
		return nil, nil, fmt.Errorf("%w: YDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{ydbschema.ChangefeedKind}) {
		return nil, nil, fmt.Errorf("%w: unsupported YDB comparison kinds", schemaext.ErrInvalidValue)
	}
	parents := make(map[objectidentity.Key]schemaext.ParentState)
	for _, parent := range request.Parents {
		if parent.Subject.Kind != objectidentity.KindTable || parent.Subject.Name.Empty() || (!parent.Desired && !parent.Current) {
			return nil, nil, fmt.Errorf("%w: invalid changefeed parent", schemaext.ErrInvalidValue)
		}
		if _, found := parents[parent.Subject.Key()]; found {
			return nil, nil, fmt.Errorf("%w: duplicate changefeed parent", schemaext.ErrDuplicate)
		}
		parents[parent.Subject.Key()] = parent
	}
	streams := make(map[objectidentity.Key]stream)
	for _, direction := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		if err := collectStreams(ctx, request, direction, parents, streams); err != nil {
			return nil, nil, err
		}
	}
	return streams, parents, nil
}

func collectStreams(ctx context.Context, request schemaext.ObjectComparisonRequest, direction schemaext.Representation,
	parents map[objectidentity.Key]schemaext.ParentState, streams map[objectidentity.Key]stream) error {
	state := request.Desired
	if direction == schemaext.Observed {
		state = request.Current
	}
	objects, err := state.Objects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, err := readStream(request, direction, object, parents, streams[object.Ref.Key()])
		if err != nil {
			return err
		}
		if knowledge := state.Coverage.Lookup(ydbschema.ChangefeedKind, object.Ref); knowledge.State == schemaext.Absent {
			return fmt.Errorf("%w: present changefeed %s is marked absent", schemaext.ErrInvalidValue, object.Ref)
		}
		streams[object.Ref.Key()] = value
	}
	for _, record := range state.Coverage.SubjectRecords() {
		if record.Kind != ydbschema.ChangefeedKind {
			return fmt.Errorf("%w: unrelated changefeed coverage", schemaext.ErrInvalidValue)
		}
		if record.Knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: a changefeed requires a definition; there is no default stream", schemaext.ErrInvalidValue)
		}
		if record.Subject.Kind == objectidentity.KindTable {
			parent, found := parents[record.Subject.Key()]
			if !found || (direction == schemaext.Desired && !parent.Desired) || (direction == schemaext.Observed && !parent.Current) {
				return fmt.Errorf("%w: changefeed namespace has no parent on its source side", schemaext.ErrInvalidValue)
			}
			if record.Knowledge.State == schemaext.Absent {
				return fmt.Errorf("%w: an empty changefeed namespace requires complete coverage", schemaext.ErrInvalidValue)
			}
			continue
		}
		if err := streamParent(record.Subject, direction, parents); err != nil {
			return err
		}
		value := streams[record.Subject.Key()]
		value.ref = record.Subject
		streams[record.Subject.Key()] = value
	}
	return nil
}

func readStream(request schemaext.ObjectComparisonRequest, direction schemaext.Representation, object schemaext.Object, parents map[objectidentity.Key]schemaext.ParentState, value stream) (stream, error) {
	if err := streamParent(object.Ref, direction, parents); err != nil {
		return stream{}, err
	}
	var spec ydbschema.ChangefeedSpec
	if direction == schemaext.Desired {
		typed, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok || typed == nil {
			return stream{}, fmt.Errorf("%w: expected desired changefeed", schemaext.ErrInvalidValue)
		}
		value.desired, spec = typed, typed.Spec
	} else {
		typed, ok := object.Value.(*ydbschema.ObservedChangefeed)
		if !ok || typed == nil {
			return stream{}, fmt.Errorf("%w: expected observed changefeed", schemaext.ErrInvalidValue)
		}
		value.current, spec = typed, typed.Spec
	}
	expected := ydbschema.ChangefeedRef(object.Ref.Schema.Source, object.Ref.Parent.Source, spec.Name)
	if expected.Key() != object.Ref.Key() {
		return stream{}, fmt.Errorf("%w: changefeed definition disagrees with its subject", schemaext.ErrInvalidValue)
	}
	if err := ydbschema.ValidateChangefeed(spec); err != nil {
		return stream{}, err
	}
	// Disabled is observable state. Whether a change can recreate it is a planning
	// decision; comparison must still preserve and recognize an unchanged stream.
	spec.Disabled = false
	if refusal := ydbchangefeed.Check(parentRef(object.Ref).String(), spec, request.Capabilities); refusal != nil {
		if refusal.Key != "" {
			return stream{}, fmt.Errorf("%w: %s requires %s", ptaherr.ErrUnsupportedFeature, refusal.Subject, refusal.Key)
		}
		return stream{}, fmt.Errorf("%w: %s: %s", schemaext.ErrInvalidValue, refusal.Subject, refusal.Reason)
	}
	value.ref = object.Ref
	return value, nil
}

func streamParent(ref objectidentity.ID, direction schemaext.Representation, parents map[objectidentity.Key]schemaext.ParentState) error {
	parent, found := parents[parentRef(ref).Key()]
	if ref.Kind != objectidentity.Kind(ydbschema.ChangefeedKind) || ref.Parent.Empty() || !found || (direction == schemaext.Desired && !parent.Desired) || (direction == schemaext.Observed && !parent.Current) {
		return fmt.Errorf("%w: changefeed %s has no parent on its source side", schemaext.ErrInvalidValue, ref)
	}
	return nil
}
