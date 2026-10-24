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
// records. The summary is written when the run ends, by [Report.Flush].
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

// Report collects the records of one run.
type Report struct {
	mu      sync.Mutex
	buckets []*bucket
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

// New returns an empty report.
func New() *Report {
	return &Report{}
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

// Warn raises record for the run ctx carries.
//
// A nil out means the caller wants no warning of this kind, as the strict
// compatibility profile does, and Warn does nothing. Without a run -- a library
// caller, or a test that drives a loader directly -- the record's line is
// written to out at once, as the loaders wrote it before runs aggregated them,
// and the error is the write's.
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
// several writes its summary and how to list them.
func (r *Report) Flush() {
	r.mu.Lock()
	buckets := r.buckets
	r.buckets = nil
	r.mu.Unlock()
	for _, target := range buckets {
		switch len(target.records) {
		case 0:
		case 1:
			fmt.Fprintf(target.out, "warning: %s\n", target.records[0].Line)
		default:
			fmt.Fprintf(target.out, "warning: %s; PTAH_LOG_LEVEL=debug lists them\n", target.category.Summary(target.records))
		}
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
