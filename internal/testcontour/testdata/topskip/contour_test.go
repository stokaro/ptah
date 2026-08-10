//go:build testcontour_fixture

package topskip_test

import "testing"

func TestTaggedTopLevelSkip(t *testing.T) {
	t.Skip("fixture skip")
}
