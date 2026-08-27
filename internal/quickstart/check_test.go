package quickstart_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/quickstart"
)

// transcript builds the two streams a run of the fixture's Bash program would
// produce, with the sentinels the generated script prints between steps.
//
// Taking the second step's standard output as a parameter is what makes the
// failure case real: the same builder produces the passing and the failing
// transcript, so the difference between them is the one line under test.
func transcript(readBack string) (stdout, stderr string) {
	stdout = strings.Join([]string{
		quickstart.Sentinel(1),
		readBack,
		quickstart.Sentinel(2),
		quickstart.Sentinel(3),
		"",
	}, "\n")
	stderr = strings.Join([]string{
		quickstart.Sentinel(1),
		quickstart.Sentinel(2),
		"schema.sql",
		quickstart.Sentinel(3),
		"",
	}, "\n")
	return stdout, stderr
}

// TestCheck_HappyPath is the control for the failure case below: with the
// transcript the page publishes, every expectation holds and both are checked.
//
// Without it, a Check that asserted nothing at all would satisfy the failure
// test by never reporting a pass.
func TestCheck_HappyPath(t *testing.T) {
	c := qt.New(t)

	page := loadPage(c, optedInPage)
	found := program(c, page, quickstart.Bash)
	stdout, stderr := transcript("CREATE TABLE users (id INTEGER PRIMARY KEY);")

	failures, asserted := quickstart.Check(page.Path, found, stdout, stderr)

	c.Assert(failures, qt.HasLen, 0)
	c.Assert(asserted, qt.Equals, 2)
}

// TestCheck_FailurePath is the proof that this runner can fail.
//
// A gate nobody has watched go red is not a gate, and the whole point of
// reading the commands out of the page is that the page and the binary can
// disagree. Here they do: the page publishes one line and the command printed
// another.
func TestCheck_FailurePath(t *testing.T) {
	c := qt.New(t)

	page := loadPage(c, optedInPage)
	found := program(c, page, quickstart.Bash)
	stdout, stderr := transcript("CREATE TABLE users (id BIGINT);")

	failures, asserted := quickstart.Check(page.Path, found, stdout, stderr)

	c.Assert(failures, qt.HasLen, 1)
	c.Assert(asserted, qt.Equals, 2)
	c.Assert(failures[0].Step, qt.Equals, 2)
	c.Assert(failures[0].Page, qt.Equals, optedInPage)
	c.Assert(failures[0].Command, qt.Equals, "cat schema.sql")
	c.Assert(failures[0].Stream, qt.Equals, quickstart.Stdout)
	c.Assert(failures[0].Missing, qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	c.Assert(failures[0].Got, qt.Equals, "CREATE TABLE users (id BIGINT);")
}

// TestCheck_AStreamThatWasNeverDelimitedFails keeps a broken run from reading
// as an unasserted one. A missing sentinel means the shell stopped, and a
// skipped assertion there would report the same success as a passing one.
func TestCheck_AStreamThatWasNeverDelimitedFails(t *testing.T) {
	c := qt.New(t)

	page := loadPage(c, optedInPage)
	found := program(c, page, quickstart.Bash)

	failures, _ := quickstart.Check(page.Path, found,
		quickstart.Sentinel(1)+"\nsomething went wrong\n", "")

	c.Assert(failures, qt.HasLen, 1)
	c.Assert(failures[0].Step, qt.Equals, 2)
	c.Assert(failures[0].Problem, qt.Equals, "the step did not finish; the run stopped here")
}

// TestFormatFailure_NamesThePageAndTheCommand pins what a red build prints.
// A failure a reader cannot act on without opening this package sends them to
// the wrong file.
func TestFormatFailure_NamesThePageAndTheCommand(t *testing.T) {
	c := qt.New(t)

	page := loadPage(c, optedInPage)
	found := program(c, page, quickstart.Bash)
	stdout, stderr := transcript("CREATE TABLE users (id BIGINT);")
	failures, _ := quickstart.Check(page.Path, found, stdout, stderr)

	rendered := quickstart.FormatFailure(quickstart.Bash, failures[0])

	c.Assert(rendered, qt.Contains, optedInPage)
	c.Assert(rendered, qt.Contains, "bash step 2")
	c.Assert(rendered, qt.Contains, "cat schema.sql")
	c.Assert(rendered, qt.Contains, "the missing line is:")
	c.Assert(rendered, qt.Contains, "CREATE TABLE users (id BIGINT);")
}
