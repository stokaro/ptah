package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/integration"
)

func TestTestRunnerAddDatabaseWithCleanup_HappyPath(t *testing.T) {
	c := qt.New(t)
	runner := integration.NewTestRunner(nil)

	err := runner.AddDatabaseWithCleanup(
		"mysql",
		"mysql://app:secret@tcp(localhost:3306)/ptah_test",
		"mysql://root:secret@tcp(127.0.0.1:3306)/ptah_test",
	)

	c.Assert(err, qt.IsNil)
}

func TestTestRunnerAddDatabaseWithCleanup_FailurePath(t *testing.T) {
	c := qt.New(t)
	runner := integration.NewTestRunner(nil)

	err := runner.AddDatabaseWithCleanup(
		"mysql",
		"mysql://app:secret@tcp(localhost:3306)/ptah_test",
		"mysql://root:secret@tcp(localhost:3306)/other",
	)

	c.Assert(err, qt.ErrorMatches, "mysql cleanup URL must address the scenario database")
}
