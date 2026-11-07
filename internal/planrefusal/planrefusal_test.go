package planrefusal_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/planrefusal"
)

// TestRefusalsKeepTheErrorTheyWrap pins what the package promises a command: the
// type adds a reason a report can read, and the message and the chain stay as
// they are, wrapped further or not.
func TestRefusalsKeepTheErrorTheyWrap(t *testing.T) {
	cause := errors.New("cause")
	wrapped := fmt.Errorf("%w: what the plan would do", cause)
	tests := []struct {
		name string
		err  error
	}{
		{name: "a lost view security option", err: &planrefusal.LostViewSecurityError{Err: wrapped}},
		{name: "a dependent object", err: &planrefusal.DependentObjectError{Err: wrapped}},
		{name: "a refused declaration", err: &planrefusal.DeclarationError{Err: wrapped}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := fmt.Errorf("generate operations: %w", test.err)

			c.Assert(got, qt.ErrorIs, cause)
			c.Assert(test.err.Error(), qt.Equals, wrapped.Error())
			c.Assert(got.Error(), qt.Equals, "generate operations: "+wrapped.Error())
		})
	}
}
