// Package datadiff computes row-level differences between the desired managed
// (reference/seed) data declared in Go sources and the rows currently stored in
// a database table.
//
// It is a pure computation layer with no database or filesystem dependencies. A
// caller in a later phase composes the full pipeline: load desired rows with
// schemamodel.LoadManagedRows, read the live rows with dbschema.ReadTableRows, and
// hand both to [Compute]. Rendering the resulting diff into DML and wiring it
// into the migrate/apply commands are separate, later phases; this package only
// reports what changed.
//
// # Value comparison
//
// Compute compares values by a normalized string form (see [Compute]): a
// one-byte type tag ("N" nil, "S" a string or a []byte, "V" everything else via
// fmt's default verb) followed by the value. The tag keeps distinct kinds from
// colliding — notably a SQL NULL stays distinct from an empty string — while
// still making the "V" form driver-agnostic (a desired int 1 and a live int64 1
// compare equal). A string and a []byte holding the same bytes compare equal:
// a declaration writes a binary column's value as text, and the driver hands it
// back as bytes. One pair is compared as values rather than as strings: a
// time.Time the driver returned against the text a declaration carries, which
// otherwise can never pair and leaves a date column planning the same UPDATE
// forever. Two texts stay two texts, so a text column keeps both spellings of
// one instant. It remains only approximate across dialects for the "V" form:
// numeric scale, boolean encoding, and decimal precision differences between
// PostgreSQL, MySQL, and others are not modeled. Type-exact, dialect-aware value
// comparison is a known follow-up, matching the issue's "cross-dialect value
// rendering is substantial" caveat.
package datadiff

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Row is a single table row keyed by column name. It matches the shape returned
// by schemamodel.LoadManagedRows and dbschema.ReadTableRows.
type Row = map[string]any

// RowUpdate describes a row present in both the desired and live data whose
// managed columns differ. Both Desired and Live are carried so a later phase can
// render a reversible UPDATE (Desired drives the forward statement, Live the
// rollback). Key holds only the key-column values that identify the row.
type RowUpdate struct {
	Key     map[string]any
	Desired Row
	Live    Row
}

// DataDiff is the set of row-level changes needed to bring a table's live rows
// in line with the desired managed data.
//
// Inserts are rows present in the desired data but absent live. Deletes are
// rows present live but absent from the desired data. Updates are rows present
// in both whose managed columns differ. Inserts, Updates, and Deletes are each
// sorted by their composite key so the output is deterministic and testable.
type DataDiff struct {
	// Schema is the database schema the table belongs to, or empty for the
	// connection's default schema. It does not affect the diff computation (rows
	// are matched by key regardless of schema); it identifies the target table so
	// [Render] can emit a schema-qualified name.
	Schema  string
	Table   string
	Keys    []string
	Inserts []Row
	Updates []RowUpdate
	Deletes []Row
}

// Compute computes the row-level diff for schema.table between the desired rows
// and the live rows, keyed by the key columns in keys. schema may be empty for
// the connection's default schema; it is recorded on the returned diff so
// [Render] can emit a schema-qualified name but does not affect matching.
//
// keys must be non-empty and every desired and live row must contain a value
// for each key column; a missing key column is a validation error. Rows are
// matched by the tuple of their key-column values, compared by their normalized
// string form (see the package documentation). A row is:
//
//   - an Insert if its key appears in desired but not live;
//   - a Delete if its key appears in live but not desired;
//   - an Update if its key appears in both and any managed column differs. Only
//     the columns present in the desired row (the managed columns) are compared,
//     so columns that exist only in the live row never trigger an update.
//
// If two rows on the same side share a composite key, the later one in input
// order wins; keys should be chosen so this does not occur.
func Compute(schema, table string, keys []string, desired, live []Row) (*DataDiff, error) {
	if len(keys) == 0 {
		return nil, errors.New("datadiff: keys must be non-empty")
	}

	desiredByKey, err := indexByKey(keys, desired, "desired")
	if err != nil {
		return nil, err
	}
	liveByKey, err := indexByKey(keys, live, "live")
	if err != nil {
		return nil, err
	}

	diff := &DataDiff{
		Schema: schema,
		Table:  table,
		Keys:   slices.Clone(keys),
	}

	for _, k := range sortedKeys(desiredByKey) {
		desiredRow := desiredByKey[k]
		liveRow, ok := liveByKey[k]
		if !ok {
			diff.Inserts = append(diff.Inserts, desiredRow)
			continue
		}
		if managedColumnsDiffer(desiredRow, liveRow) {
			diff.Updates = append(diff.Updates, RowUpdate{
				Key:     keyValues(keys, desiredRow),
				Desired: desiredRow,
				Live:    liveRow,
			})
		}
	}

	for _, k := range sortedKeys(liveByKey) {
		if _, ok := desiredByKey[k]; !ok {
			diff.Deletes = append(diff.Deletes, liveByKey[k])
		}
	}

	return diff, nil
}

// indexByKey builds a map from composite-key string to row. side ("desired" or
// "live") is used only to make missing-key errors point at the offending input.
func indexByKey(keys []string, rows []Row, side string) (map[string]Row, error) {
	out := make(map[string]Row, len(rows))
	for i, row := range rows {
		k, err := keyString(keys, row)
		if err != nil {
			return nil, fmt.Errorf("datadiff: %s row %d: %w", side, i, err)
		}
		out[k] = row
	}
	return out, nil
}

// keyString builds a collision-free composite key from the key columns of row.
// Each normalized key value is length-prefixed and NUL-terminated so that no
// combination of values can produce the same string as a different tuple.
func keyString(keys []string, row Row) (string, error) {
	var b strings.Builder
	for _, k := range keys {
		v, ok := row[k]
		if !ok {
			return "", fmt.Errorf("missing key column %q", k)
		}
		n := normalizeValue(v)
		b.WriteString(strconv.Itoa(len(n)))
		b.WriteByte(':')
		b.WriteString(n)
		b.WriteByte(0)
	}
	return b.String(), nil
}

// keyValues returns a copy of just the key-column values of row.
func keyValues(keys []string, row Row) map[string]any {
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		out[k] = row[k]
	}
	return out
}

// managedColumnsDiffer reports whether any column present in desired has a
// different normalized value than the same column in live. Columns present only
// in live are ignored, so a live row that is a superset of the managed columns
// does not spuriously register as an update.
func managedColumnsDiffer(desired, live Row) bool {
	for col, desiredValue := range desired {
		if !valuesEqual(desiredValue, live[col]) {
			return true
		}
	}
	return false
}

// valuesEqual answers whether a declared value and the value the database
// returned are the same value.
//
// The two arrive in different shapes. A declaration carries text -- the YAML
// says `2026-01-02T03:04:05Z` -- while a driver decides for itself what a
// timestamp column scans into, and pgx makes it a time.Time. Compared as
// strings those can never pair, so every reconciliation of a date or timestamp
// column plans the same UPDATE again and the convergence the reference-data
// page promises never arrives (stokaro/ptah#3261).
//
// The instant comparison is entered only where one side is a time and the other
// is text. Two texts stay two texts: in a text column `2026-01-02T03:04:05Z`
// and `2026-01-02T04:04:05+01:00` are different values, and folding them would
// report a converged row the author never wrote.
func valuesEqual(desired, live any) bool {
	if desiredTime, liveText, ok := timeAndText(desired, live); ok {
		return sameInstant(desiredTime, liveText)
	}
	if liveTime, desiredText, ok := timeAndText(live, desired); ok {
		return sameInstant(liveTime, desiredText)
	}
	return normalizeValue(desired) == normalizeValue(live)
}

// timeAndText reports the pair where one value is a time and the other is text.
func timeAndText(candidate, other any) (time.Time, string, bool) {
	moment, ok := candidate.(time.Time)
	if !ok {
		return time.Time{}, "", false
	}
	switch text := other.(type) {
	case string:
		return moment, text, true
	case []byte:
		return moment, string(text), true
	}
	return time.Time{}, "", false
}

// timeLayouts are the spellings a declaration writes a moment in. A value that
// parses in none of them is not a moment, and the comparison falls back to the
// text it is.
var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func sameInstant(moment time.Time, text string) bool {
	trimmed := strings.TrimSpace(text)
	for _, layout := range timeLayouts {
		// A layout without a zone reads as UTC, which is the zone the readers
		// hand back for a column that carries none.
		parsed, err := time.Parse(layout, trimmed)
		if err != nil {
			continue
		}
		return parsed.UTC().Equal(moment.UTC())
	}
	return false
}

// sortedKeys returns the map keys in ascending order.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// normalizeValue reduces a value to a comparable string form, prefixed with a
// one-byte type tag so distinct kinds can never collide: "N" for nil (a SQL
// NULL), "S" for a string or a []byte, and "V" for any other value via fmt's
// default verb. A []byte shares the string tag because a declaration writes a
// binary column's value as text while dbschema.ReadTableRows returns it as
// bytes; with a tag of its own, every such row would plan the same UPDATE on
// every run. The tag keeps a NULL distinct from an empty string, so a live
// NULL versus a desired "" is correctly reported as a change rather than
// silently matching. A time and the text naming it are paired before this is
// reached; see valuesEqual. The package documentation carries the remaining
// cross-dialect limitations (numeric scale, boolean encoding) of the "V" form.
func normalizeValue(v any) string {
	switch value := v.(type) {
	case nil:
		return "N"
	case []byte:
		return "S" + string(value)
	case string:
		return "S" + value
	default:
		return "V" + fmt.Sprintf("%v", value)
	}
}
