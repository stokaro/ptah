package cmdutil_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/cmdutil"
)

func TestWorkReportAnswersWhatTheVerbReported(t *testing.T) {
	t.Run("nothing reported", func(t *testing.T) {
		c := qt.New(t)

		_, finished := cmdutil.WithWorkReport(context.Background())

		c.Assert(finished(), qt.IsFalse)
	})

	t.Run("reported once", func(t *testing.T) {
		c := qt.New(t)

		ctx, finished := cmdutil.WithWorkReport(context.Background())
		cmdutil.ReportWorkFinished(ctx)

		c.Assert(finished(), qt.IsTrue)
	})

	t.Run("reported twice", func(t *testing.T) {
		c := qt.New(t)

		ctx, finished := cmdutil.WithWorkReport(context.Background())
		cmdutil.ReportWorkFinished(ctx)
		cmdutil.ReportWorkFinished(ctx)

		// The reader is consulted once per process and must not depend on how
		// many times the verb reported, any more than on how many times it is
		// asked.
		c.Assert(finished(), qt.IsTrue)
		c.Assert(finished(), qt.IsTrue)
	})
}

// TestReportingIntoAContextWithoutAReportIsANoOp is the case every unit test of
// a verb takes: the command runs under a plain context, reports, and nothing
// panics or leaks into another run's answer.
func TestReportingIntoAContextWithoutAReportIsANoOp(t *testing.T) {
	c := qt.New(t)

	cmdutil.ReportWorkFinished(context.Background())

	_, finished := cmdutil.WithWorkReport(context.Background())
	c.Assert(finished(), qt.IsFalse)
}

// TestOneReportDoesNotReachAnotherContext pins the report to the context it was
// installed in. Two runs in one process -- a test binary is the case that
// matters -- must not read each other's answer.
func TestOneReportDoesNotReachAnotherContext(t *testing.T) {
	c := qt.New(t)

	first, firstFinished := cmdutil.WithWorkReport(context.Background())
	_, secondFinished := cmdutil.WithWorkReport(context.Background())
	cmdutil.ReportWorkFinished(first)

	c.Assert(firstFinished(), qt.IsTrue)
	c.Assert(secondFinished(), qt.IsFalse)
}

// TestTheNearestReportIsTheOneThatAnswers covers a context derived from another
// that already carries a report: the inner one is what a verb running under it
// reports to.
func TestTheNearestReportIsTheOneThatAnswers(t *testing.T) {
	c := qt.New(t)

	outer, outerFinished := cmdutil.WithWorkReport(context.Background())
	inner, innerFinished := cmdutil.WithWorkReport(outer)
	cmdutil.ReportWorkFinished(inner)

	c.Assert(innerFinished(), qt.IsTrue)
	c.Assert(outerFinished(), qt.IsFalse)
}
