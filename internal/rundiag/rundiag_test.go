package rundiag_test

import (
	"bytes"
	"context"
	"encoding/json"
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

// A nil writer is a caller that wants no warning of the kind; a run does not
// override it.
func TestWarn_NilWriterRaisesNothing(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Stream: &bytes.Buffer{}, Listing: rundiag.ListedAtDebugLevel})

	c.Assert(rundiag.Warn(context.Background(), nil, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(rundiag.With(context.Background(), report), nil, widgets, record("a")), qt.IsNil)

	c.Assert(report.Count(widgets), qt.Equals, 0)
}

// A run writes nothing until it ends, and a category with one object writes
// that object's own line, so a lone typo keeps its location.
func TestReport_OneObjectWritesItsLine(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
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
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
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
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer
	gadgets := &rundiag.Category{Name: "gadget", Summary: func(records []rundiag.Record) string { return "gadgets" }}

	c.Assert(rundiag.Warn(ctx, &out, gadgets, rundiag.Record{Key: "g", Line: "gadget g"}), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	report.Flush()

	c.Assert(out.String(), qt.Equals, "warning: gadget g\nwarning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n")
}

// Each object is logged once at debug level when the run ends, with its
// category and fields, which is what PTAH_LOG_LEVEL=debug shows.
func TestReport_LogsEachObjectOnceAtDebug(t *testing.T) {
	c := qt.New(t)
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c.Cleanup(func() { slog.SetDefault(previous) })
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)

	c.Assert(rundiag.Warn(ctx, &bytes.Buffer{}, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &bytes.Buffer{}, widgets, record("a")), qt.IsNil)
	c.Assert(logged.String(), qt.Equals, "")
	report.Flush()

	c.Assert(strings.Count(logged.String(), "\n"), qt.Equals, 1)
	c.Assert(logged.String(), qt.Contains, `level=DEBUG msg="widget a" category=widget key=a`)
}

// A summary says how to list its objects in the words of the surface the run
// is on, and a run that names none lists them at debug level.
func TestReport_SummaryNamesTheListing(t *testing.T) {
	tests := []struct {
		name    string
		listing rundiag.Listing
		want    string
	}{
		{name: "none", want: "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n"},
		{name: "at debug level", listing: rundiag.ListedAtDebugLevel, want: "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n"},
		{
			name:    "outside the strict compatibility profile",
			listing: rundiag.ListedOutsideStrictCompat,
			want:    "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them when PTAH_ATLAS_STRICT_COMPAT is off\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := rundiag.New(rundiag.Options{Listing: test.listing})
			ctx := rundiag.With(context.Background(), report)
			var out bytes.Buffer

			c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
			c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
			report.Flush()

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// A run hands its diagnostics stream to a loader with no writer of its own,
// so what the loader raises is reported by the run.
func TestStream_IsTheRunsDiagnosticsStream(t *testing.T) {
	c := qt.New(t)
	var stream bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)

	c.Assert(rundiag.Warn(ctx, rundiag.Stream(ctx), widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, rundiag.Stream(ctx), widgets, record("b")), qt.IsNil)
	report.Flush()

	c.Assert(stream.String(), qt.Equals, "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n")
}

// Outside a run, or in a run given no stream, there is none, so a loader that
// has no writer of its own stays quiet.
func TestStream_IsNilWithoutARun(t *testing.T) {
	c := qt.New(t)

	c.Assert(rundiag.Stream(context.Background()), qt.IsNil)
	c.Assert(rundiag.Stream(rundiag.With(context.Background(), rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel}))), qt.IsNil)
}

// A run whose log is JSON writes its warnings as JSON records at warning
// level, with the category and the number of objects, on the stream a line
// would go to, and writes no line. The default logger's level does not drop
// them: a default logger that keeps errors alone sees nothing, and the
// records are still written.
func TestReport_RecordsAreLogRecords(t *testing.T) {
	c := qt.New(t)
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError})))
	c.Cleanup(func() { slog.SetDefault(previous) })
	report := rundiag.New(rundiag.Options{Records: true})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer
	gadgets := &rundiag.Category{Name: "gadget", Summary: func(records []rundiag.Record) string { return "gadgets" }}

	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, gadgets, rundiag.Record{Key: "g", Line: "gadget g", Attrs: []slog.Attr{slog.String("key", "g")}}), qt.IsNil)
	report.Flush()

	c.Assert(logged.String(), qt.Equals, "")
	c.Assert(jsonRecords(c, out.String()), qt.DeepEquals, []map[string]any{
		{"level": "WARN", "msg": "ww widgets; PTAH_LOG_LEVEL=debug lists them", "category": "widget", "count": float64(2)},
		{"level": "WARN", "msg": "gadget g", "category": "gadget", "count": float64(1), "key": "g"},
	})
}

// A command that logs through a JSON logger of its own hands it to the run:
// the objects' records and the warning go through it, with the fields it adds
// to every record, those raised before it was handed over too, and nothing
// goes to the stream. The logger's level keeps the objects or drops them, and
// the warning is written at any level.
func TestUseLogger_WritesTheRunThroughAJSONLogger(t *testing.T) {
	summary := map[string]any{
		"level": "WARN", "msg": "ww widgets; PTAH_LOG_LEVEL=debug or --log-level debug lists them",
		"correlation_id": "c1", "category": "widget", "count": float64(2),
	}
	tests := []struct {
		name  string
		level slog.Level
		want  []map[string]any
	}{
		{name: "at debug level", level: slog.LevelDebug, want: []map[string]any{
			{"level": "DEBUG", "msg": "widget a", "correlation_id": "c1", "category": "widget", "key": "a"},
			{"level": "DEBUG", "msg": "widget b", "correlation_id": "c1", "category": "widget", "key": "b"},
			summary,
		}},
		{name: "at error level", level: slog.LevelError, want: []map[string]any{summary}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var stream, logged bytes.Buffer
			report := rundiag.New(rundiag.Options{Stream: &stream})
			ctx := rundiag.With(context.Background(), report)
			logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: test.level})).With("correlation_id", "c1")

			c.Assert(rundiag.Warn(ctx, &stream, widgets, record("a")), qt.IsNil)
			rundiag.UseLogger(ctx, logger, true)
			c.Assert(rundiag.Warn(ctx, &stream, widgets, record("b")), qt.IsNil)
			report.Flush()

			c.Assert(stream.String(), qt.Equals, "")
			c.Assert(jsonRecords(c, logged.String()), qt.DeepEquals, test.want)
		})
	}
}

// With a text logger the objects go through it, and the warning stays a line
// on the stream, naming the flag beside the variable.
func TestUseLogger_KeepsTheLineWithATextLogger(t *testing.T) {
	c := qt.New(t)
	var stream, logged bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream})
	ctx := rundiag.With(context.Background(), report)
	rundiag.UseLogger(ctx, slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})), false)

	c.Assert(rundiag.Warn(ctx, &stream, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &stream, widgets, record("b")), qt.IsNil)
	report.Flush()

	c.Assert(stream.String(), qt.Equals, "warning: ww widgets; PTAH_LOG_LEVEL=debug or --log-level debug lists them\n")
	c.Assert(strings.Count(logged.String(), "level=DEBUG"), qt.Equals, 2)
}

// The strict compatibility profile's listing stays its own: no command there
// has a --log-level flag to name.
func TestUseLogger_KeepsTheStrictListing(t *testing.T) {
	c := qt.New(t)
	var stream bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedOutsideStrictCompat})
	ctx := rundiag.With(context.Background(), report)
	rundiag.UseLogger(ctx, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), false)

	c.Assert(rundiag.Warn(ctx, &stream, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &stream, widgets, record("b")), qt.IsNil)
	report.Flush()

	c.Assert(stream.String(), qt.Equals, "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them when PTAH_ATLAS_STRICT_COMPAT is off\n")
}

// Settle writes a category's warning at once, as the end of the run would,
// and the end of the run does not write it again: a warning a confirmation
// depends on precedes the confirmation.
func TestSettle_WritesTheCategoryAtOnce(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	rundiag.Settle(ctx, widgets)

	c.Assert(out.String(), qt.Equals, "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n")
	report.Flush()
	c.Assert(out.String(), qt.Equals, "warning: ww widgets; PTAH_LOG_LEVEL=debug lists them\n")
}

// An object Settle wrote is not raised again in the run, and one raised after
// it is written when the run ends.
func TestSettle_WritesEachObjectOnce(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	rundiag.Settle(ctx, widgets)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	report.Flush()

	c.Assert(out.String(), qt.Equals, "warning: widget a\nwarning: widget b\n")
}

// Settle writes the category it names and leaves the others to the end of the
// run.
func TestSettle_LeavesOtherCategoriesToTheEnd(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer
	gadgets := &rundiag.Category{Name: "gadget", Summary: func(records []rundiag.Record) string { return "gadgets" }}

	c.Assert(rundiag.Warn(ctx, &out, gadgets, rundiag.Record{Key: "g", Line: "gadget g"}), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	rundiag.Settle(ctx, widgets)

	c.Assert(out.String(), qt.Equals, "warning: widget a\n")
	report.Flush()
	c.Assert(out.String(), qt.Equals, "warning: widget a\nwarning: gadget g\n")
}

// Without a run Warn has written each object, and Settle adds nothing.
func TestSettle_WithoutARunWritesNothingMore(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	c.Assert(rundiag.Warn(context.Background(), &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(context.Background(), &out, widgets, record("b")), qt.IsNil)
	rundiag.Settle(context.Background(), widgets)

	c.Assert(out.String(), qt.Equals, "warning: widget a\nwarning: widget b\n")
}

// A settled warning of a run whose log is JSON is a record too, with the
// category and the number of objects.
func TestSettle_WritesARecordWhenTheLogIsJSON(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Records: true})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	rundiag.Settle(ctx, widgets)

	c.Assert(jsonRecords(c, out.String()), qt.DeepEquals, []map[string]any{
		{"level": "WARN", "msg": "ww widgets; PTAH_LOG_LEVEL=debug lists them", "category": "widget", "count": float64(2)},
	})
}

// Drain hands each category's warning to the caller instead of writing it:
// the message a line would carry, how many objects, and each object's line,
// the categories in the order they were raised. It empties the report, so the
// end of the run writes nothing, and it logs the objects at debug level as the
// end of the run does.
func TestDrain_ReturnsTheWarningsInsteadOfWritingThem(t *testing.T) {
	c := qt.New(t)
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c.Cleanup(func() { slog.SetDefault(previous) })
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedInToolResult})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer
	gadgets := &rundiag.Category{Name: "gadget", Summary: func(records []rundiag.Record) string { return "gadgets" }}

	c.Assert(rundiag.Warn(ctx, &out, gadgets, rundiag.Record{Key: "g", Line: "gadget g"}), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("a")), qt.IsNil)
	c.Assert(rundiag.Warn(ctx, &out, widgets, record("b")), qt.IsNil)
	warnings := report.Drain()
	report.Flush()

	c.Assert(warnings, qt.DeepEquals, []rundiag.Warning{
		{Category: "gadget", Count: 1, Message: "gadget g", Objects: []string{"gadget g"}},
		{
			Category: "widget", Count: 2, Message: "ww widgets; the result's _meta lists them under ptah.run/warnings",
			Objects: []string{"widget a", "widget b"},
		},
	})
	c.Assert(out.String(), qt.Equals, "")
	c.Assert(strings.Count(logged.String(), "level=DEBUG"), qt.Equals, 3)
}

// A report nothing was raised in drains to no warning.
func TestDrain_ReturnsNothingForAnEmptyReport(t *testing.T) {
	c := qt.New(t)

	c.Assert(rundiag.New(rundiag.Options{}).Drain(), qt.HasLen, 0)
}

// jsonRecords is the JSON lines of written, without their time.
func jsonRecords(c *qt.C, written string) []map[string]any {
	c.Helper()
	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(written), "\n") {
		var record map[string]any
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("%s", line))
		delete(record, "time")
		records = append(records, record)
	}
	return records
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

// The undecided objects are named, those that share a cause together and
// followed by it, the causes in the order they were first raised.
func TestUndecidedObjects_SummaryNamesEachObjectByItsCause(t *testing.T) {
	c := qt.New(t)
	undecided := func(object, cause string) rundiag.Record {
		return rundiag.Record{Attrs: []slog.Attr{slog.String("object", object), slog.String("cause", cause)}}
	}

	summary := rundiag.UndecidedObjects.Summary([]rundiag.Record{
		undecided(`role "a"`, "roles were not read"),
		undecided(`sequence "s"`, "sequences were not read"),
		undecided(`role "b"`, "roles were not read"),
	})

	c.Assert(summary, qt.Equals, `3 objects get no planned change because the comparison could not decide them: `+
		`role "a", role "b" (roles were not read); sequence "s" (sequences were not read)`)
}

// The omitted blocks name the variable that keeps them, which the records
// carry, so the summary keeps the remedy each object's line gives.
func TestAtlasOmittedBlocks_SummaryNamesTheVariableTheRecordsShare(t *testing.T) {
	tests := []struct {
		name    string
		records []rundiag.Record
		want    string
	}{
		{
			name: "every record names it",
			records: []rundiag.Record{
				{Attrs: []slog.Attr{slog.String("variable", "KEEP_ALL")}},
				{Attrs: []slog.Attr{slog.String("variable", "KEEP_ALL")}},
			},
			want: "2 blocks the Atlas community CLI refuses are omitted from the document; KEEP_ALL=1 keeps every block Ptah models",
		},
		{
			name:    "no record names it",
			records: []rundiag.Record{{}, {}},
			want:    "2 blocks the Atlas community CLI refuses are omitted from the document",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(rundiag.AtlasOmittedBlocks.Summary(test.records), qt.Equals, test.want)
		})
	}
}
