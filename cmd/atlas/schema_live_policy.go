package atlas

import (
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasschema"
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
