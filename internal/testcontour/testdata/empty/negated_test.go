//go:build !testcontour_fixture

package empty_test

import "testing"

func TestNegatedContourMustNotRun(t *testing.T) {
	t.Fatal("a negated contour tag declared membership")
}
