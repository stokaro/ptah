package atlas

import (
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/atlasschema"
)

func atlasLiveSchemaObjectValidator(
	policy atlascompatpolicy.Policy,
) func(atlasschema.LiveSchemaObject) error {
	if !policy.IsStrictCE() {
		return nil
	}
	return func(object atlasschema.LiveSchemaObject) error {
		return policy.ValidateLiveSchemaObject(atlascompatpolicy.LiveSchemaObject{
			Kind:             object.Kind,
			Name:             object.Name,
			ImplicitSequence: object.ImplicitSequence,
		})
	}
}
