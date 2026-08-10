//go:build ptah_live_fail

package fail_test

import "testing"

func TestTaggedFailure(t *testing.T) {
	t.Fatal("fixture failure")
}
