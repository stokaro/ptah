//go:build testcontour_fixture

package fail_test

import "testing"

func TestTaggedFailure(t *testing.T) {
	t.Fatal("fixture failure")
}
