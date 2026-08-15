package atlas

// White-box testing required: this test pins the adapter's nil callback in
// full mode. A process test can prove that full mode succeeds, but cannot
// observe whether it made an unnecessary supplemental catalog query first.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasschema"
)

func TestAtlasLiveSchemaObjectValidator(t *testing.T) {
	t.Run("full mode performs no supplemental inventory", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(atlasLiveSchemaObjectValidator(atlascompatpolicy.Full()), qt.IsNil)
	})

	t.Run("strict mode translates catalog identity", func(t *testing.T) {
		c := qt.New(t)
		validate := atlasLiveSchemaObjectValidator(atlascompatpolicy.StrictCE())
		c.Assert(validate, qt.IsNotNil)
		err := validate(atlasschema.LiveSchemaObject{
			Kind: "procedure",
			Name: "refresh_users()",
		})
		c.Assert(err, qt.ErrorMatches,
			`Atlas Community Edition strict compatibility does not support inspecting live schema procedure "refresh_users\(\)"`)
	})
}
