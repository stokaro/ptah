package selectext

import (
	"context"
	"fmt"
	"reflect"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// BaselineRequest supplies captured state for a dev-database replay. Current
// and StartingPoint are observations; Declared contains observations converted
// from the comparison side. DefaultSchema resolves unqualified table children.
// A service receives only its models and no live database handles.
type BaselineRequest struct {
	Current       schemaext.Objects
	StartingPoint schemaext.Objects
	Declared      schemaext.Objects
	DefaultSchema string
}

// BaselineDecision accounts for one current object. A nil Value removes it.
// A retained value contains the state absent from the starting point, together
// with any starting-point state Declared explicitly retains.
type BaselineDecision struct {
	Subject objectidentity.ID
	Value   schemaext.Value
}

// BaselineResult accounts for every current object in its collection order.
// Complete confirms the entire batch, including an intentionally empty one.
type BaselineResult struct {
	Complete  bool
	Decisions []BaselineDecision
}

// BaselineService subtracts starting-point state without external I/O or input
// mutation. Errors and cancellation discard the batch. Calls can run concurrently.
type BaselineService interface {
	SubtractBaseline(context.Context, BaselineRequest) (BaselineResult, error)
}

// SubtractBaseline removes starting-point state unless Declared retains it.
// Registered services can subtract individual members of an object. Other
// objects are matched by identity; their definitions do not affect membership.
// Only a defaulted schema or an unqualified table child inherits DefaultSchema.
// Global owner objects retain their absent schema. Results are independent.
func (s Set) SubtractBaseline(ctx context.Context, request BaselineRequest) (schemaext.Objects, error) {
	if err := s.projectionContext(ctx); err != nil {
		return schemaext.Objects{}, err
	}
	current, err := request.Current.All()
	if err != nil {
		return schemaext.Objects{}, err
	}
	kept := make(map[objectidentity.Key]schemaext.Object, len(current))
	starting, declared := baselineKeys(request.StartingPoint, request.DefaultSchema), baselineKeys(request.Declared, request.DefaultSchema)
	for _, object := range current {
		key := baselineKey(object.Ref, request.DefaultSchema)
		if !starting[key] || declared[key] {
			kept[object.Ref.Key()] = object
		}
	}
	for _, extension := range s.extensions {
		if extension.Baseline == nil {
			continue
		}
		owned, err := request.forExtension(extension)
		if err != nil {
			return schemaext.Objects{}, err
		}
		objects, err := owned.Current.All()
		if err != nil {
			return schemaext.Objects{}, err
		}
		if len(objects) == 0 {
			continue
		}
		result, err := extension.Baseline.SubtractBaseline(ctx, owned)
		if err != nil {
			return schemaext.Objects{}, err
		}
		if err := ctx.Err(); err != nil {
			return schemaext.Objects{}, err
		}
		if err := applyBaseline(kept, objects, result); err != nil {
			return schemaext.Objects{}, err
		}
	}
	var output []schemaext.Object
	for _, object := range current {
		if value, found := kept[object.Ref.Key()]; found {
			output = append(output, value)
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.Objects{}, err
	}
	return schemaext.NewObjects(output...)
}

func applyBaseline(kept map[objectidentity.Key]schemaext.Object, objects []schemaext.Object, result BaselineResult) error {
	if !result.Complete || len(result.Decisions) != len(objects) {
		return fmt.Errorf("%w: baseline selection must account for every current object", schemaext.ErrInvalidValue)
	}
	for i, decision := range result.Decisions {
		object := objects[i]
		if decision.Subject != object.Ref {
			return fmt.Errorf("%w: baseline selection changes a subject", schemaext.ErrInvalidValue)
		}
		delete(kept, object.Ref.Key())
		if decision.Value != nil {
			if err := schemaext.ValidatePayload(decision.Value); err != nil {
				return err
			}
			if decision.Value.Kind() != object.Value.Kind() || reflect.TypeOf(decision.Value) != reflect.TypeOf(object.Value) {
				return fmt.Errorf("%w: baseline selection changes a model or representation", schemaext.ErrInvalidValue)
			}
			value, err := schemaext.CloneValue(decision.Value)
			if err != nil {
				return err
			}
			object.Value = value
			kept[object.Ref.Key()] = object
		}
	}
	return nil
}

func (r BaselineRequest) forExtension(extension Extension) (BaselineRequest, error) {
	result := BaselineRequest{DefaultSchema: r.DefaultSchema}
	for _, pair := range []struct {
		from schemaext.Objects
		to   *schemaext.Objects
	}{
		{r.Current, &result.Current}, {r.StartingPoint, &result.StartingPoint}, {r.Declared, &result.Declared},
	} {
		objects, err := pair.from.Select(func(ref objectidentity.ID) bool { return extension.owns(ref.Kind) }).All()
		if err != nil {
			return BaselineRequest{}, err
		}
		*pair.to, err = schemaext.NewObjects(objects...)
		if err != nil {
			return BaselineRequest{}, err
		}
	}
	return result, nil
}

func baselineKeys(objects schemaext.Objects, defaultSchema string) map[objectidentity.Key]bool {
	keys := make(map[objectidentity.Key]bool, objects.Len())
	for _, ref := range objects.Refs() {
		keys[baselineKey(ref, defaultSchema)] = true
	}
	return keys
}

func baselineKey(ref objectidentity.ID, defaultSchema string) objectidentity.Key {
	if ref.Schema.Defaulted || !ref.Parent.Empty() && ref.Schema.Empty() {
		ref.Schema.Normalized = defaultSchema
	}
	return ref.Key()
}
