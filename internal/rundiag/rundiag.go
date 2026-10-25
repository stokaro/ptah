// Package rundiag aggregates the warnings a command run can raise about many
// objects at once.
//
// A desired schema can hold dozens of names PostgreSQL cuts, and a project
// file can carry several settings Atlas accepts and ignores. Written one line
// per object, those warnings bury the result of a run: OpenMeter's
// `migrate diff` printed 47 lines before "no changes to be made". So a run
// reports each [Category] once, with the number of objects it concerns, and
// keeps the objects themselves for the debug log.
//
// The objects are logged as they are recorded, through log/slog at debug
// level with the category and the record's fields, so the existing
// PTAH_LOG_LEVEL=debug lists them and PTAH_LOG_FORMAT=json lists them as
// records. The summary is written when the run ends, by [Report.Flush], and
// says how to list the objects in the words of the surface the run is on; see
// [Listing].
//
// Aggregation never hides a refusal: an error is returned, not recorded here.
package rundiag

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// A Category is one kind of warning a run can raise about many objects.
type Category struct {
	// Name identifies the category in the debug log.
	Name string
	// Summary is the warning for several objects; it is given them.
	Summary func(records []Record) string
}

// A Record is one object of a category.
type Record struct {
	// Key identifies the object, so a run that reads the same source twice
	// counts it once.
	Key string
	// Line is the whole warning for this object, without the "warning: "
	// prefix. A category with one object prints it as it is.
	Line string
	// Attrs are the record's fields in the debug log.
	Attrs []slog.Attr
}

// A Listing is how a summary tells its reader to list the objects it counts.
// A surface picks the one its own switches make true; the words are defined
// here, once, so every command of a surface says the same.
type Listing string

const (
	// ListedAtDebugLevel names PTAH_LOG_LEVEL, which native ptah and the
	// default ptah-compat profile read on every command, before the command
	// starts: it sets the default logger, and the --log-level flag of the
	// commands that have one is bound to it. The flag alone is not named,
	// because those commands read a project file before they start their
	// logger, and no command that reads a SQL schema file has the flag.
	ListedAtDebugLevel Listing = "PTAH_LOG_LEVEL=debug lists them"
	// ListedOutsideStrictCompat is for the strict compatibility profile,
	// which refuses PTAH_LOG_LEVEL because the community binary has no such
	// setting. A run outside the profile raises the same objects and lists
	// them.
	ListedOutsideStrictCompat Listing = "PTAH_LOG_LEVEL=debug lists them when PTAH_ATLAS_STRICT_COMPAT is off"
)

// Report collects the records of one run.
type Report struct {
	mu      sync.Mutex
	stream  io.Writer
	listing Listing
	records bool
	buckets []*bucket
}

// Options describe the report of one run.
type Options struct {
	// Stream is the run's diagnostics stream, the writer [Stream] hands a
	// loader that has none of its own. A nil Stream gives such a loader none.
	Stream io.Writer
	// Listing is how a summary tells its reader to list the objects it
	// counts. The zero Listing is [ListedAtDebugLevel].
	Listing Listing
	// Records writes each warning as a log record at warning level, through
	// the default logger, instead of as a line, for a run whose log is JSON:
	// the summaries then join the objects' records as JSON lines rather than
	// break that log with text. A record carries the category, the number of
	// objects, and for one object that object's fields.
	Records bool
}

// bucket is the records of one category. They are written to the writer the
// first of them was raised for: every command raises a category on its error
// stream, and a writer is not always comparable -- a function type is not --
// so it cannot be what tells two buckets apart.
type bucket struct {
	category *Category
	out      io.Writer
	keys     map[string]bool
	records  []Record
}

// New returns an empty report for a run as opts describe it.
func New(opts Options) *Report {
	listing := opts.Listing
	if listing == "" {
		listing = ListedAtDebugLevel
	}
	return &Report{stream: opts.Stream, listing: listing, records: opts.Records}
}

type reportKey struct{}

// With returns ctx carrying report.
func With(ctx context.Context, report *Report) context.Context {
	return context.WithValue(ctx, reportKey{}, report)
}

// From returns the report ctx carries, or nil, as it does for a nil ctx.
func From(ctx context.Context) *Report {
	if ctx == nil {
		return nil
	}
	report, _ := ctx.Value(reportKey{}).(*Report)
	return report
}

// Stream returns the diagnostics stream of the run ctx carries, or nil without
// a run. A loader with no writer of its own raises its warnings there, so a run
// reports what a reader found whichever command read it, and a load outside a
// run stays as quiet as its caller made it.
func Stream(ctx context.Context) io.Writer {
	report := From(ctx)
	if report == nil {
		return nil
	}
	return report.stream
}

// Warn raises record for the run ctx carries.
//
// A nil out means the caller wants no warning of this kind, and Warn does
// nothing. Without a run -- a library caller, or a test that drives a loader
// directly -- the record's line is written to out at once, and the error is
// the write's.
func Warn(ctx context.Context, out io.Writer, category *Category, record Record) error {
	if out == nil {
		return nil
	}
	report := From(ctx)
	if report == nil {
		_, err := fmt.Fprintf(out, "warning: %s\n", record.Line)
		return err
	}
	report.add(ctx, out, category, record)
	return nil
}

func (r *Report) add(ctx context.Context, out io.Writer, category *Category, record Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	target := r.bucket(category, out)
	if target.keys[record.Key] {
		return
	}
	target.keys[record.Key] = true
	target.records = append(target.records, record)
	attrs := append([]slog.Attr{slog.String("category", category.Name)}, record.Attrs...)
	slog.LogAttrs(ctx, slog.LevelDebug, record.Line, attrs...)
}

func (r *Report) bucket(category *Category, out io.Writer) *bucket {
	for _, existing := range r.buckets {
		if existing.category == category {
			return existing
		}
	}
	created := &bucket{category: category, out: out, keys: make(map[string]bool)}
	r.buckets = append(r.buckets, created)
	return created
}

// Flush writes one warning per category to the writer its first record was
// raised for, in the order the categories were first raised, and empties the
// report. A category with one object writes that object's line; one with
// several writes its summary and the report's [Listing]. With
// [Options.Records] each warning is a log record instead.
func (r *Report) Flush() {
	r.mu.Lock()
	buckets := r.buckets
	r.buckets = nil
	r.mu.Unlock()
	for _, target := range buckets {
		line, attrs := r.summary(target)
		switch {
		case line == "":
		case r.records:
			slog.LogAttrs(context.Background(), slog.LevelWarn, line, attrs...)
		default:
			fmt.Fprintf(target.out, "warning: %s\n", line)
		}
	}
}

// summary is the warning of one category and the fields of its record: the
// object's own line and fields for one object, and for several their summary
// and how to list them. It is empty for a category with no object.
func (r *Report) summary(target *bucket) (string, []slog.Attr) {
	attrs := []slog.Attr{slog.String("category", target.category.Name), slog.Int("count", len(target.records))}
	switch len(target.records) {
	case 0:
		return "", nil
	case 1:
		return target.records[0].Line, append(attrs, target.records[0].Attrs...)
	default:
		return target.category.Summary(target.records) + "; " + string(r.listing), attrs
	}
}

// Count returns how many objects of category the report holds.
func (r *Report) Count(category *Category) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, target := range r.buckets {
		if target.category == category {
			return len(target.records)
		}
	}
	return 0
}

// Shared returns the value every record holds for the attribute key, and
// whether they all hold one and the same.
func Shared(records []Record, key string) (slog.Value, bool) {
	var shared slog.Value
	for index, record := range records {
		value, ok := attr(record, key)
		if !ok || (index > 0 && !value.Equal(shared)) {
			return slog.Value{}, false
		}
		shared = value
	}
	return shared, len(records) > 0
}

func attr(record Record, key string) (slog.Value, bool) {
	for _, candidate := range record.Attrs {
		if candidate.Key == key {
			return candidate.Value, true
		}
	}
	return slog.Value{}, false
}
