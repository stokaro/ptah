//go:build integration

package pass_test

import "testing"

func TestIntegrationOnlyMustNotRun(t *testing.T) {
	t.Fatal("integration-only test ran inside a narrower live contour")
}
