//go:build testcontour_fixture

package pass_test

import "testing"

func TestTaggedPass(_ *testing.T) {}

func TestTaggedSubtestPass(t *testing.T) {
	t.Run("pass", func(_ *testing.T) {})
}
