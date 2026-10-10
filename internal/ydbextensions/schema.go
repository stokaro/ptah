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
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
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
		if err := validateObject(target, caps, object); err != nil {
			return err
		}
	}
	return nil
}

// validateObject holds one declared object to its owner's rules.
func validateObject(target string, caps capability.Capabilities, object schemaext.Object) error {
	switch value := object.Value.(type) {
	case *ydbschema.DesiredChangefeed:
		return validateChangefeedObject(target, caps, object, value)
	case *ydbcoordination.Desired:
		return validateCoordinationObject(target, caps, object, value)
	case *ydbstreaming.Desired:
		return validateStreamingObject(target, caps, object, value)
	case *ydbworkload.DesiredPool:
		return validatePoolObject(target, caps, object, value)
	case *ydbworkload.DesiredClassifier:
		return validateClassifierObject(target, caps, object, value)
	case *ydbsecret.Desired:
		return validateSecretObject(target, caps, object, value)
	case *ydbtopic.Desired:
		return validateTopicObject(target, caps, object, value)
	case *ydbexternal.DesiredSource:
		return validateExternalSourceObject(target, caps, object, value)
	case *ydbexternal.DesiredTable:
		return validateExternalTableObject(target, caps, object, value)
	default:
		return fmt.Errorf("%w: YDB does not render feature object %s with payload %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
	}
}

func validatePoolObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbworkload.DesiredPool) error {
	if err := ydbworkload.ValidatePoolRef(object.Ref, value.Spec); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	context := renderer.ExtensionContext{Target: target, Capabilities: caps}
	if object.Ref.Name.Source == ydbworkload.DefaultPool {
		return ydbrender.DefaultPoolSettingsHandler().Validate(context, &ydbast.DefaultPoolSettings{Spec: value.Spec})
	}
	return ydbrender.ResourcePoolHandler().Validate(context,
		&ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: object.Ref.Name.Source, Spec: &value.Spec})
}

func validateClassifierObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbworkload.DesiredClassifier) error {
	if err := ydbworkload.ValidateIdentity(object.Ref, ydbworkload.ClassifierKind); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	return ydbrender.ResourcePoolClassifierHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps},
		&ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: object.Ref.Name.Source, Spec: &value.Spec})
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

// validateTopicObject holds a declared topic to the rules its creation is
// rendered with, so schema validation and rendering refuse the same topics.
func validateTopicObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbtopic.Desired) error {
	if err := ydbtopic.ValidateIdentity(object.Ref); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	operation := &ydbast.Topic{Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source, Change: ydbdiff.Topic{After: value}}
	return ydbrender.TopicHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps}, operation)
}

func validateStreamingObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbstreaming.Desired) error {
	if err := ydbstreaming.ValidateIdentity(object.Ref); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	operation := &ydbast.StreamingQuery{Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source,
		Operation: ydbast.StreamingCreate, Spec: value.Spec, Creation: ydbast.StreamingCreation{OrReplace: value.AllowStateReset}}
	return ydbrender.StreamingHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps}, operation)
}

// validateSecretObject reuses the operation owner's validation, so schema
// validation and rendering require the same capability and accept the same
// variable.
func validateSecretObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbsecret.Desired) error {
	if err := ydbsecret.ValidateIdentity(object.Ref); err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	operation := &ydbast.Secret{Operation: ydbast.SecretCreate, Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source,
		ValueEnv: value.Variable(object.Ref)}
	return ydbrender.SecretHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps}, operation)
}

// validateExternalSourceObject and validateExternalTableObject hold a declared
// external object to the rules its creation is rendered with, so schema
// validation and rendering refuse the same declarations.
func validateExternalSourceObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbexternal.DesiredSource) error {
	if err := ydbexternal.ValidateIdentity(object.Ref); err != nil || schemaext.Kind(object.Ref.Kind) != ydbexternal.SourceKind {
		return fmt.Errorf("%w: data source %s has an invalid identity: %w", ptaherr.ErrInvalidSchemaDiff, object.Ref, err)
	}
	operation := &ydbast.ExternalDataSource{Operation: ydbast.ExternalCreate, Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source,
		Spec: value.Spec}
	return ydbrender.ExternalDataSourceHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps}, operation)
}

func validateExternalTableObject(target string, caps capability.Capabilities, object schemaext.Object, value *ydbexternal.DesiredTable) error {
	if err := ydbexternal.ValidateIdentity(object.Ref); err != nil || schemaext.Kind(object.Ref.Kind) != ydbexternal.TableKind {
		return fmt.Errorf("%w: external table %s has an invalid identity: %w", ptaherr.ErrInvalidSchemaDiff, object.Ref, err)
	}
	operation := &ydbast.ExternalTable{Operation: ydbast.ExternalCreate, Schema: object.Ref.Schema.Source, Name: object.Ref.Name.Source,
		Spec: value.Spec}
	return ydbrender.ExternalTableHandler().Validate(renderer.ExtensionContext{Target: target, Capabilities: caps}, operation)
}
