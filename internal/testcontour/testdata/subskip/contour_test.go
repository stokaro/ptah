//go:build ptah_live_subskip

package subskip_test

import "testing"

func TestTaggedSubtestSkip(t *testing.T) {
	t.Run("skipped", func(t *testing.T) {
		t.Skip("fixture skip")
	})
}
