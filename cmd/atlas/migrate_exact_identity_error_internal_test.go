package atlas

// White-box testing required: these tests exercise the unexported error
// adapter's string-boundary rules and Unwrap contract. The compatibility
// process tests cover user-visible diagnostics, but cannot distinguish whether
// the wrapper preserved errors.Is and errors.As for its original cause.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRemapAtlasExactIdentityErrorPreservesCauseAndDecimalBoundaries(t *testing.T) {
	c := qt.New(t)
	sentinel := errors.New("sentinel")
	err := fmt.Errorf("set revision 123; preserve x1234, 1234, and object 123: %w", sentinel)

	mapped := remapAtlasExactIdentityError(err, map[int64]string{
		123: "exact-token",
	})

	c.Assert(mapped, qt.ErrorMatches,
		`set revision exact-token; preserve x1234, 1234, and object 123: sentinel`)
	c.Assert(mapped, qt.ErrorIs, sentinel)
	var exactErr *atlasExactIdentityError
	c.Assert(mapped, qt.ErrorAs, &exactErr)
}

func TestRemapAtlasExactIdentityErrorDoesNotRewriteSQLObjectName(t *testing.T) {
	c := qt.New(t)
	err := errors.New(`failed to apply migration 123: no such table: "123"`)

	mapped := remapAtlasExactIdentityError(err, map[int64]string{123: "1.5"})

	c.Assert(mapped, qt.ErrorMatches, `failed to apply migration 1\.5: no such table: "123"`)
}

func TestRemapAtlasExactIdentityErrorRendersPresentEmptyIdentity(t *testing.T) {
	c := qt.New(t)
	err := errors.New("failed to apply migration 123: broken statement")

	mapped := remapAtlasExactIdentityError(err, map[int64]string{123: ""})

	c.Assert(mapped, qt.ErrorMatches, `failed to apply migration "": broken statement`)
}
