//go:build testcontour_fixture

package unlisted_test

import (
	"fmt"
	"testing"
)

func TestDeclared(_ *testing.T) {}

func Example() {
	fmt.Println("ran")
	// Output: ran
}
