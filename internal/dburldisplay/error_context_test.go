package dburldisplay_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dburldisplay"
)

func TestRecordedErrorRedactionUsesEveryAddressInOneRun(t *testing.T) {
	c := qt.New(t)
	ctx := dburldisplay.WithErrorRedaction(t.Context())
	dburldisplay.RecordErrorAddress(ctx, "postgres://app:Pa55wordX9@localhost/first")
	dburldisplay.RecordErrorAddress(ctx, "postgres://app:Zq7Kp123@localhost/second")
	cause := errors.New("driver failure")
	err := errors.Join(cause, errors.New("statement: Pa55wordX9 and Zq7Kp123"))

	redacted := dburldisplay.RedactRecordedError(ctx, err)

	c.Assert(redacted.Error(), qt.Not(qt.Contains), "Pa55wordX9")
	c.Assert(redacted.Error(), qt.Not(qt.Contains), "Zq7Kp123")
	c.Assert(redacted, qt.ErrorIs, cause)
	c.Assert(dburldisplay.RedactRecordedError(context.Background(), err), qt.Equals, err)
}
