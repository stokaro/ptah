//go:build ptah_live_topskip

package topskip_test

import "testing"

func TestTaggedTopLevelSkip(t *testing.T) {
	t.Skip("fixture skip")
}
