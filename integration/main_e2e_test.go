//go:build integration

package integration_test

import (
	"testing"

	"ptah.run/internal/clirun"
)

// TestMain runs the tests through clirun.Main, which removes the programs
// clirun.Build compiled for them when they end.
func TestMain(m *testing.M) {
	clirun.Main(m)
}
