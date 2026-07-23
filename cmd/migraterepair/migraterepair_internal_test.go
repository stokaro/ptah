package migraterepair

// White-box testing required: parseResumeFrom is a small validation boundary
// for resume semantics whose zero/default behavior is clearer to test directly
// than through a database-backed repair command execution.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestParseResumeFrom_HappyPath(t *testing.T) {
	c := qt.New(t)

	resumeFrom, err := parseResumeFrom("")
	c.Assert(err, qt.IsNil)
	c.Assert(resumeFrom, qt.Equals, 0)

	resumeFrom, err = parseResumeFrom("3")
	c.Assert(err, qt.IsNil)
	c.Assert(resumeFrom, qt.Equals, 3)
}

func TestParseResumeFrom_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := parseResumeFrom("0")
	c.Assert(err, qt.ErrorMatches, `invalid resume-from value "0"`)

	_, err = parseResumeFrom("bad")
	c.Assert(err, qt.ErrorMatches, `invalid resume-from value "bad"`)
}
