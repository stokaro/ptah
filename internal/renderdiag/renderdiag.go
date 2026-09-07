// Package renderdiag carries the record a renderer leaves when a target cannot
// hold something the schema declared, and the sink it writes that record to.
//
// Rendering answers a declaration in one of three ways. It emits the
// declaration, it refuses the whole render with an error, or it continues
// without the declaration. The third answer is the one nothing could see: a
// PostgreSQL-family target writes a `skipped` comment beside the statement,
// while SQLite, SQL Server and Oracle drop the same table options without a
// word, and both spellings exit 0 (stokaro/ptah#2976).
//
// A record here is what makes that answer readable by a program. It is not a
// finding and it is not published: `ptah schema validate --no-skipped` maps a
// record into the problem model that verb already reports, so this package adds
// a vocabulary rather than a fifth analysis result shape.
//
// # This is not coverage
//
// [ptah.run/core/coverage] answers a neighboring question and must not be
// reached for here. Coverage says a description never claimed to describe an
// object, so the comparator plans nothing for it. An omission says the author
// declared an object and this target will not carry it. Feeding one into the
// other would suppress the difference instead of reporting the loss.
package renderdiag

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

// Reason names why a declaration did not reach the output.
//
// The set is closed and each value is stable: a caller branches on it, and the
// sentence a renderer writes beside the record is free to change wording
// without changing what the caller decided.
type Reason string

// The reasons a renderer records.
const (
	// ReasonUnsupported marks a declaration the target has no representation
	// for. The declaration is well formed and another target renders it.
	ReasonUnsupported Reason = "unsupported"
)

// Omission is one declared thing a target did not render.
//
// Kind and Name identify the object that owned the declaration, so a reader can
// find it in the source. Property names what was lost from that object, and is
// empty when the whole object was. Detail carries the declared value where
// repeating it helps the reader recognize what went missing.
//
// The target is deliberately not a field. A sink belongs to one render of one
// target, and the entry point that owns the sink is the one place that already
// holds the normalized dialect name, so stamping it there keeps eight renderers
// from each spelling it again.
type Omission struct {
	// Reason is why the declaration did not reach the output.
	Reason Reason
	// Kind is the owning object's kind, such as "table".
	Kind string
	// Name is the owning object's name as the declaration spells it.
	Name string
	// Property names the lost property, empty when the whole object was lost.
	Property string
	// Detail is the declared value, empty when there is nothing to repeat.
	Detail string
	// Remedy states how to keep the declaration on this target. It is empty
	// unless a remedy exists here: a suggestion that does not work on the
	// target it is printed for costs the reader more than silence.
	Remedy string
}

// Sink collects omissions during one render.
//
// A renderer is built fresh for each statement on the ordered-render path, so
// the sink rather than the renderer is what spans a schema. A nil *Sink is
// usable and drops what it is given, which is what a render nobody asked to
// report runs with.
type Sink struct {
	omissions []Omission
}

// Record adds one omission. A nil sink ignores it.
func (s *Sink) Record(omission Omission) {
	if s == nil {
		return
	}
	s.omissions = append(s.omissions, omission)
}

// Omissions returns what was recorded, in a deterministic order.
//
// Records arrive in whatever order the walk reached the objects, and that order
// is an implementation detail of the lowering rather than something a reader
// should have to predict. Sorting by the identity a reader sees keeps a report
// stable across runs and across changes to the walk.
func (s *Sink) Omissions() []Omission {
	if s == nil || len(s.omissions) == 0 {
		return nil
	}
	out := slices.Clone(s.omissions)
	slices.SortStableFunc(out, func(a, b Omission) int {
		return cmp.Or(
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.Property, b.Property),
			cmp.Compare(a.Detail, b.Detail),
		)
	})
	return out
}

// The vocabulary for a table option a target does not carry.
//
// Five renderers drop these and each one would otherwise spell the record its
// own way, which is the shape a reader has to normalize before they can compare
// two targets. One constructor is what keeps `table option ENGINE` from also
// being `ENGINE option` and `engine`.
const (
	// TableKind is the owning object's kind for a table-level loss.
	TableKind = "table"
	// TableOptionProperty prefixes the property name of a table option.
	TableOptionProperty = "table option"
)

// TableOptionOmission is the record for one table option a target drops.
//
// remedy is empty unless keeping the declaration is possible on that target;
// the option key alone does not decide that, because the answer differs per
// family.
func TableOptionOmission(table, key, value, remedy string) Omission {
	return Omission{
		Reason:   ReasonUnsupported,
		Kind:     TableKind,
		Name:     table,
		Property: TableOptionProperty + " " + key,
		Detail:   value,
		Remedy:   remedy,
	}
}

// RecordDroppedTableOptions records every option the renderer did not consume.
//
// kept names the keys the caller rendered, compared without case because the
// same option reaches a renderer spelled both `WITHOUT_ROWID` and
// `WITHOUT ROWID`. Recording the complement rather than a list of losses is
// what keeps a renderer honest when a new option key appears: an option nobody
// taught it to render is reported rather than silently new.
func (s *Sink) RecordDroppedTableOptions(table string, options map[string]string, kept ...string) {
	if s == nil {
		return
	}
	for _, key := range slices.Sorted(maps.Keys(options)) {
		if slices.ContainsFunc(kept, func(k string) bool { return strings.EqualFold(k, key) }) {
			continue
		}
		s.Record(TableOptionOmission(table, key, options[key], ""))
	}
}
