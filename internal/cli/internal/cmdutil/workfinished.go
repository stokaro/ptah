package cmdutil

import (
	"context"
	"sync/atomic"
)

// workReport carries one bit from a running verb back to the surface that owns
// the process exit status: the verb did everything the invocation asked for.
//
// The surface cannot derive that bit. A command that returns nil while a signal
// is in flight is either a server the signal stopped -- `ptah schema serve`
// returns nil from its shutdown, and docs/exit_codes.md promises 130 for it --
// or a verb that finished before the signal arrived and has nothing left to
// stop. Both look the same from outside: nil, and a signal.
//
// Reporting is the opt-in side. A verb that says nothing keeps the signal
// status, so a surface added later reports an interrupt as an interrupt until
// somebody decides otherwise.
type workReport struct {
	finished atomic.Bool
}

type workReportKey struct{}

// WithWorkReport returns a context that carries a work report, and the reader
// for it. The reader answers false until something running under the context
// calls [ReportWorkFinished].
func WithWorkReport(parent context.Context) (context.Context, func() bool) {
	report := &workReport{}
	return context.WithValue(parent, workReportKey{}, report), report.finished.Load
}

// ReportWorkFinished records that the command running under ctx did everything
// the invocation asked for. A verb calls it once its work is complete and
// before it returns success.
//
// Without a report in the context it does nothing, which is what a verb reached
// from a test or a library call needs: there is no process status to protect.
func ReportWorkFinished(ctx context.Context) {
	report, ok := ctx.Value(workReportKey{}).(*workReport)
	if !ok {
		return
	}
	report.finished.Store(true)
}
