//go:build integration || !ptah_live_empty

package empty_test

import "testing"

func TestNegatedContourMustNotRun(t *testing.T) {
	t.Fatal("a negated contour tag declared membership")
}
