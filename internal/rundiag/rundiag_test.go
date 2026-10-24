package rundiag_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/rundiag"
)

// widgets is a category for these tests alone.
var widgets = &rundiag.Category{
	Name:    "widget",
	Summary: func(records []rundiag.Record) string { return strings.Repeat("w", len(records)) + " widgets" },
}

func record(key string) rundiag.Record {
	return rundiag.Record{Key: key, Line: "widget " + key, Attrs: []slog.Attr{slog.String("key", key)}}
}

// Without a run, a warning is written at once, as the loaders wrote it before
// runs aggregated them.
func TestWarn_WithoutARunWritesTheLine(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := rundiag.Warn(context.Background(), &out, widgets, record("a"))

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "warning: widget a\n")
}

func TestWarn_WithoutARunReturnsTheWriteError(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("closed")

	err := rundiag.Warn(context.Background(), failingWriter{refused}, widgets, record("a"))

	c.Assert(err, qt.ErrorIs, refused)
}

// A nil writer is a caller that wants no warning of the kind, such as the
// strict compatibility profile; a run does not override it.
func TestWarn_NilWriterRaisesNothing(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New()

	c.Assert(rundiag.Warn(context.Background(), nil, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(rundiag.With(context.Background(), report), nil, widgets, record("a")), qt.IsNil)

	c.Assert(report.Count(widgets), qt.Equals, 0)
}

// A run writes nothing until it ends, and a category with one object writes
// that object's own line, so a lone typo keeps its location.
func TestReport_OneObjectWritesItsLine(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New()
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	report.Flush()

	c.Assert(out.String(), qt.Equals, "warning: widget a\n")
}

// Several objects of a category are one line with their summary, and the same
// object raised twice, as a source read twice in one run raises it, counts
// once.
func TestReport_SeveralObjectsAreOneSummary(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New()
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	for _, key := range []string{"a", "b", "a", "c", "b"} {
		c.Assert(rundiag.Warn(ctx, &out, widgets, record(key)), qt.IsNil)
	}
	report.Flush()

	c.Assert(report.Count(widgets), qt.Equals, 0)
	c.Assert(out.String(), qt.Equals, "warning: www widgets; PTAH_LOG_LEVEL=debug lists them\n")
}

func TestReport_FlushKeepsTheOrderCategoriesWereRaisedIn(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New()
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer
	gadgets := &rundiag.Category{Name: "gadget", Summary: func(records []rundiag.Record) string { return "gadgets" }}

	c.Assert(rundiag.Warn(ctx, &out, gadgets, rundiag.Record{Key: "g", Line: "gadget g"}), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	report.Flush()

	c.Assert(out.String(), qt.Equals, "warning: gadget g\nwarning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n")
}

// Each object is logged once at debug level, with its category and fields,
// which is what PTAH_LOG_LEVEL=debug shows.
func TestReport_LogsEachObjectOnceAtDebug(t *testing.T) {
	c := qt.New(t)
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	ctx := rundiag.With(context.Background(), rundiag.New())

	c.Assert(rundiag.Warn(ctx, &bytes.Buffer{}, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &bytes.Buffer{}, widgets, record("a")), qt.IsNil)

	c.Assert(strings.Count(logged.String(), "\n"), qt.Equals, 1)
	c.Assert(logged.String(), qt.Contains, `level=DEBUG msg="widget a" category=widget key=a`)
}

func TestTruncatedNames_SummaryNamesTheLimitTheRecordsShare(t *testing.T) {
	tests := []struct {
		name   string
		limits []int
		want   string
	}{
		{
			name:   "one limit",
			limits: []int{63, 63},
			want:   "2 names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read that way",
		},
		{
			name:   "two limits",
			limits: []int{63, 128},
			want:   "2 names are longer than the server's identifier limit; they are read the way the server stores them",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			records := make([]rundiag.Record, 0, len(test.limits))
			for _, limit := range test.limits {
				records = append(records, rundiag.Record{Attrs: []slog.Attr{slog.Int("limit", limit)}})
			}

			c.Assert(rundiag.TruncatedNames.Summary(records), qt.Equals, test.want)
		})
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }
