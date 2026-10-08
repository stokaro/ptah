package ydbextensions

import (
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
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
		if object.Ref.Parent.Empty() {
			return fmt.Errorf("%w: a changefeed requires a parent table", ptaherr.ErrInvalidSchemaDiff)
		}
		value, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return fmt.Errorf("%w: YDB does not render feature object %s with payload %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
		}
		if ydbschema.ChangefeedRef(object.Ref.Schema.Source, object.Ref.Parent.Source, value.Spec.Name).Key() != object.Ref.Key() {
			return fmt.Errorf("%w: changefeed reference disagrees with its payload", ptaherr.ErrInvalidSchemaDiff)
		}
		table := tableref.Canonical(object.Ref.Schema.Source, object.Ref.Parent.Source)
		if refusal := ydbchangefeed.Check(table, value.Spec, caps); refusal != nil {
			return &ptaherr.CapabilityError{Dialect: target, Feature: string(refusal.Key), Err: ptaherr.ErrUnsupportedFeature,
				Message: refusalMessage(target, refusal.Subject, refusal.Key, refusal.Reason)}
		}
	}
	return nil
}
