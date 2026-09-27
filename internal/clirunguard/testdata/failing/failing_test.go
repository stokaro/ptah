// Package failing_test is a test package whose one test fails, run through
// clirun.Main. TestMainKeepsTheCodeOfAFailingPackage in the package above
// runs it and expects a non-zero exit.
package failing_test

import (
	"testing"

	"ptah.run/internal/clirun"
)

func TestMain(m *testing.M) {
	clirun.Main(m)
}

func TestFails(t *testing.T) {
	t.Fatal("this test fails on purpose")
}
