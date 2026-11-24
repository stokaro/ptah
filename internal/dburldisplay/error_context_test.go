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

// A server's own text, such as the DETAIL of a refused statement, loses the
// same secrets an error message does, and keeps the rest.
func TestRecordedTextRedaction(t *testing.T) {
	ctx := dburldisplay.WithErrorRedaction(t.Context())
	dburldisplay.RecordErrorAddress(ctx, "postgres://app:Pa55wordX9@localhost/first")
	short := dburldisplay.WithErrorRedaction(t.Context())
	dburldisplay.RecordErrorAddress(short, "postgres://app:x@localhost/execute")
	tests := []struct {
		name string
		ctx  context.Context
		text string
		want string
	}{
		{name: "a recorded password", ctx: ctx, text: "Key (secret)=(Pa55wordX9) already exists.", want: "Key (secret)=(***) already exists."},
		{name: "a text that quotes none", ctx: ctx, text: "Key (id)=(1) already exists.", want: "Key (id)=(1) already exists."},
		{name: "a context that records nothing", ctx: t.Context(), text: "Pa55wordX9", want: "Pa55wordX9"},
		{name: "a short password's letters", ctx: short, text: "Key (name)=(execute) already exists.", want: "Key (name)=(execute) already exists."},
		{
			name: "a short password in a quoted address",
			ctx:  short,
			text: "dblink to postgres://app:x@localhost/execute refused",
			want: "dblink to postgres://app:***@localhost/execute refused",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dburldisplay.RedactRecordedText(test.ctx, test.text), qt.Equals, test.want)
		})
	}
}
