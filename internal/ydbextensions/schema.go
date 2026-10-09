package ydbextensions

import (
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbchangefeed"
)

// ValidateObjects checks the complete named state handed to the selected YDB
// adapter. An unrelated kind, an observed value in a declaration, or a missing
// capability is an error, never a reason to omit an object.
func ValidateObjects(target string, caps capability.Capabilities, objects schemaext.Objects) error {
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		switch value := object.Value.(type) {
		case *ydbschema.DesiredChangefeed:
			if err := validateChangefeedObject(target, caps, object, value); err != nil {
				return err
			}
		case *ydbcoordination.Desired:
			if err := validateCoordinationObject(target, caps, object, value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: YDB does not render feature object %s with payload %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
		}
	}
	return nil
}

func validateChangefeedObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbschema.DesiredChangefeed) error {
	if object.Ref.Parent.Empty() {
		return fmt.Errorf("%w: a changefeed requires a parent table", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := value.RetainedReplication.Validate(); err != nil {
		return err
	}
	if ydbschema.ChangefeedRef(object.Ref.Schema.Source, object.Ref.Parent.Source, value.Spec.Name).Key() != object.Ref.Key() {
		return fmt.Errorf("%w: changefeed reference disagrees with its payload", ptaherr.ErrInvalidSchemaDiff)
	}
	table := tableref.Canonical(object.Ref.Schema.Source, object.Ref.Parent.Source)
	if refusal := ydbchangefeed.Check(table, value.Spec, caps); refusal != nil {
		return &ptaherr.CapabilityError{Dialect: target, Feature: string(refusal.Key), Err: ptaherr.ErrUnsupportedFeature,
			Message: refusalMessage(target, refusal.Subject, refusal.Key, refusal.Reason)}
	}
	return nil
}

func validateCoordinationObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbcoordination.Desired) error {
	if err := ydbcoordination.ValidateRef(object.Ref); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	// Reuse the operation owner's validation so schema validation and rendering
	// require the same capabilities and accept the same configuration.
	operation := &ydbast.CoordinationNode{Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source,
		Change: ydbdiff.CoordinationNode{After: value}}
	return ydbrender.CoordinationHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps}, operation)
}

// ValidateCreationObjects checks a CREATE TABLE's children. A retained binding
// is valid planning state, but creating its stream cannot restore the controller
// or its consumer position. Keep this decision separate from state validation so
// an inspected snapshot can be compared again without becoming a create request.
func ValidateCreationObjects(target string, caps capability.Capabilities, objects schemaext.Objects) error {
	if err := ValidateObjects(target, caps, objects); err != nil {
		return err
	}
	all, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range all {
		value, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return fmt.Errorf("%w: standalone feature object %s cannot be created inside a table", ptaherr.ErrUnsupportedFeature, object.Ref)
		}
		if value.RetainedReplication != nil {
			return fmt.Errorf("%w: retained replication-managed changefeed %s is an observation, not a standalone creation instruction", ptaherr.ErrUnsupportedFeature, object.Ref)
		}
	}
	return nil
}
