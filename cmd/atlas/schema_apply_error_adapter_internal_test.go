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
	tests := []struct {
		name        string
		message     string
		wantDisplay string
		wantWrapped bool
	}{
		{
			name:        "exact HCL loader context is hidden",
			message:     "load --to schema: parse HCL schema: malformed schema",
			wantDisplay: "malformed schema",
			wantWrapped: true,
		},
		{
			name:        "unrelated loader context is preserved",
			message:     "load --to schema: schema file does not exist",
			wantDisplay: "load --to schema: schema file does not exist",
		},
		{
			name:        "near match is preserved",
			message:     "load --to schema: parse SQL schema: malformed schema",
			wantDisplay: "load --to schema: parse SQL schema: malformed schema",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sentinel := errors.New("sentinel")
			sourceErr := fmt.Errorf("%s: %w", test.message, sentinel)

			got := displayAtlasSchemaApplyError(sourceErr)

			c.Assert(got.Error(), qt.Equals, test.wantDisplay+": sentinel")
			c.Assert(errors.Is(got, sentinel), qt.IsTrue)
			if test.wantWrapped {
				c.Assert(errors.Unwrap(got), qt.Equals, sourceErr)
				return
			}
			c.Assert(got, qt.Equals, sourceErr)
		})
	}
}
