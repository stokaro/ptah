package testcontour_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/testcontour"
)

func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/pass",
		Tag:     "ptah_live_fixture",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
		Stdout:  &stdout,
		Stderr:  &stderr,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, "--- PASS: TestTaggedPass")
	c.Assert(stdout.String(), qt.Contains, "--- PASS: TestTaggedSubtestPass")
	c.Assert(stdout.String(), qt.Not(qt.Contains), "TestOrdinaryMustNotRun")
	c.Assert(stdout.String(), qt.Not(qt.Contains), "TestIntegrationOnlyMustNotRun")
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestRun_FailurePathRejectsTopLevelSkip(t *testing.T) {
	c := qt.New(t)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/topskip",
		Tag:     "ptah_live_topskip",
		Timeout: time.Minute,
	})

	c.Assert(err, qt.ErrorMatches, `test contour "ptah_live_topskip" skipped TestTaggedTopLevelSkip`)
}

func TestRun_FailurePathRejectsSubtestSkip(t *testing.T) {
	c := qt.New(t)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/subskip",
		Tag:     "ptah_live_subskip",
		Timeout: time.Minute,
	})

	c.Assert(err, qt.ErrorMatches, `test contour "ptah_live_subskip" skipped TestTaggedSubtestSkip/skipped`)
}

func TestRun_FailurePathPreservesTestFailure(t *testing.T) {
	c := qt.New(t)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/fail",
		Tag:     "ptah_live_fail",
		Timeout: time.Minute,
	})

	c.Assert(err, qt.ErrorMatches, `test contour "ptah_live_fail" failed: exit status 1`)
}

func TestRun_FailurePathRejectsMissingResult(t *testing.T) {
	c := qt.New(t)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/missing",
		Tag:     "ptah_live_missing",
		Timeout: time.Minute,
	})

	c.Assert(err, qt.ErrorMatches, `test contour "ptah_live_missing" produced no complete result for TestTaggedMissingResult`)
}

func TestRun_FailurePathRejectsEmptyContour(t *testing.T) {
	c := qt.New(t)

	err := testcontour.Run(context.Background(), testcontour.Config{
		Package: "./testdata/empty",
		Tag:     "ptah_live_empty",
		Tags:    []string{"integration"},
		Timeout: time.Minute,
	})

	c.Assert(err, qt.ErrorMatches, `test contour "ptah_live_empty" selects no tests in ./testdata/empty`)
}

func TestRun_FailurePathValidatesConfiguration(t *testing.T) {
	c := qt.New(t)

	c.Run("package", func(c *qt.C) {
		err := testcontour.Run(context.Background(), testcontour.Config{
			Tag:     "ptah_live_fixture",
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, "test contour package is required")
	})

	c.Run("tag", func(c *qt.C) {
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Tag:     "integration",
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, `test contour tag "integration" must start with ptah_live_`)
	})

	c.Run("timeout", func(c *qt.C) {
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Tag:     "ptah_live_fixture",
		})
		c.Assert(err, qt.ErrorMatches, "test contour timeout must be positive")
	})

	c.Run("invalid tag", func(c *qt.C) {
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Tag:     "ptah_live_fixture,other",
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, `test contour tag "ptah_live_fixture,other" is not a valid build tag`)
	})

	c.Run("invalid additional tag", func(c *qt.C) {
		err := testcontour.Run(context.Background(), testcontour.Config{
			Package: "./testdata/pass",
			Tag:     "ptah_live_fixture",
			Tags:    []string{"integration,unix"},
			Timeout: time.Minute,
		})
		c.Assert(err, qt.ErrorMatches, `additional build tag "integration,unix" is not valid`)
	})
}
