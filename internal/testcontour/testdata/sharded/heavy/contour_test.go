//go:build testcontour_fixture

package heavy_test

import "testing"

func TestHeavyOne(_ *testing.T) {}

func TestHeavyTwo(t *testing.T) {
	t.Run("nested", func(_ *testing.T) {})
}

func TestHeavyThree(_ *testing.T) {}
