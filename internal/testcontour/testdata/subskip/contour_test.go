//go:build testcontour_fixture

package subskip_test

import "testing"

func TestTaggedSubtestSkip(t *testing.T) {
	t.Run("skipped", func(t *testing.T) {
		t.Skip("fixture skip")
	})
}
