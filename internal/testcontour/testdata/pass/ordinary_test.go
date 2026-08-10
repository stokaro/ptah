package pass_test

import "testing"

func TestOrdinaryMustNotRun(t *testing.T) {
	t.Fatal("ordinary test ran inside a live contour")
}
