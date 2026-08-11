package atlas

// White-box testing required: displayAtlasSchemaApplyError is the unexported
// compatibility-boundary adapter for one exact diagnostic prefix. Driving its
// error-identity branches through the exported command requires filesystem and
// SQLite boundaries, which belong to the tagged integration contour.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestDisplayAtlasSchemaApplyError(t *testing.T) {
	t.Run("exact HCL loader context is hidden", func(t *testing.T) {
		c := qt.New(t)
		sentinel := errors.New("sentinel")
		sourceErr := fmt.Errorf(
			"load --to schema: parse HCL schema: malformed schema: %w",
			sentinel,
		)

		got := displayAtlasSchemaApplyError(sourceErr)

		c.Assert(got.Error(), qt.Equals, "malformed schema: sentinel")
		c.Assert(errors.Is(got, sentinel), qt.IsTrue)
		c.Assert(errors.Unwrap(got), qt.Equals, sourceErr)
	})

	t.Run("other errors pass through", func(t *testing.T) {
		tests := []struct {
			name    string
			message string
		}{
			{
				name:    "unrelated loader context",
				message: "load --to schema: schema file does not exist",
			},
			{
				name:    "near match",
				message: "load --to schema: parse SQL schema: malformed schema",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				sentinel := errors.New("sentinel")
				sourceErr := fmt.Errorf("%s: %w", test.message, sentinel)

				got := displayAtlasSchemaApplyError(sourceErr)

				c.Assert(got.Error(), qt.Equals, test.message+": sentinel")
				c.Assert(errors.Is(got, sentinel), qt.IsTrue)
				c.Assert(got, qt.Equals, sourceErr)
			})
		}
	})
}
