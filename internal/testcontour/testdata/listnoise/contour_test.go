//go:build testcontour_fixture

package listnoise_test

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	fmt.Println("TestPrintedByTestMain")
	os.Exit(m.Run())
}

func TestDeclared(_ *testing.T) {}
