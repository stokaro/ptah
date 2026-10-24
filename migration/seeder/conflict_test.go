package seeder_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/migration/seeder"
)

// A seed service reports a key conflict by wrapping dataext.ErrKeyConflict,
// which is what an idempotent run tolerates whatever the engine's own error
// says.
func TestIsConflictError_KeyConflictFromASeedService(t *testing.T) {
	c := qt.New(t)
	wrapped := fmt.Errorf("apply seed 002_more.dev.sql: %w", fmt.Errorf("%w: provider seed outcome is KEY_CONFLICT", dataext.ErrKeyConflict))
	c.Assert(seeder.IsConflictError(wrapped), qt.IsTrue)
	c.Assert(seeder.IsConflictError(errors.New("apply seed 002_more.dev.sql: the provider applied nothing of the file")), qt.IsFalse)
}
