package chschema

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// RefreshKind identifies the refresh schedule of a refreshable materialized
// view, a setting attached to the common materialized view.
const RefreshKind schemaext.Kind = "ptah.run/clickhouse/refresh"

// Refresh modes, as ClickHouse spells them.
const (
	// RefreshEvery refreshes on a wall-clock schedule.
	RefreshEvery = "EVERY"
	// RefreshAfter refreshes that long after the previous refresh finished.
	RefreshAfter = "AFTER"
)

// Schedule is one ClickHouse refresh schedule, the clauses of
// `REFRESH EVERY|AFTER <interval> [OFFSET ...] [RANDOMIZE FOR ...]
// [DEPENDS ON ...] [APPEND]`. Intervals keep the spelling they are given; the
// owner's comparison reads both sides in the spelling the server stores, so
// `60 MINUTE` and `1 HOUR` are one schedule there.
type Schedule struct {
	// Mode is RefreshEvery or RefreshAfter.
	Mode string `json:"mode"`
	// Interval is the period, such as `1 HOUR` or `1 MINUTE 30 SECOND`.
	Interval string `json:"interval"`
	// Offset shifts an EVERY schedule within its period. AFTER takes none.
	Offset string `json:"offset,omitempty"`
	// Randomize spreads each refresh over a window, RANDOMIZE FOR.
	Randomize string `json:"randomize,omitempty"`
	// DependsOn names the views this one refreshes after, in order.
	DependsOn []string `json:"depends_on,omitempty"`
	// Append adds each refresh's rows instead of replacing the previous ones.
	Append bool `json:"append,omitempty"`
}

// DesiredRefresh declares the refresh schedule of one materialized view. A
// view without this facet declares no schedule only where its source's
// coverage says the source states schedules; otherwise the schedule is
// unmanaged and the server's is kept.
type DesiredRefresh struct {
	Schedule
}

// ObservedRefresh is the refresh schedule a server reports for one
// materialized view. A refreshable view whose schedule could not be read
// carries no observation and a coverage limit instead.
type ObservedRefresh struct {
	Schedule
}

// Kind returns the owned refresh model identity.
func (*DesiredRefresh) Kind() schemaext.Kind { return RefreshKind }

// Kind returns the owned refresh model identity.
func (*ObservedRefresh) Kind() schemaext.Kind { return RefreshKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredRefresh) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredRefresh)(nil)
	}
	return &DesiredRefresh{Schedule: v.Schedule.Clone()}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedRefresh) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedRefresh)(nil)
	}
	return &ObservedRefresh{Schedule: v.Schedule.Clone()}
}

// Equal compares declarations as written, without reading intervals.
func (v *DesiredRefresh) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredRefresh)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Schedule.Equal(w.Schedule)
}

// Equal compares observations as reported.
func (v *ObservedRefresh) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedRefresh)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Schedule.Equal(w.Schedule)
}

// Desired captures an observed schedule as a declaration. A nil receiver
// remains nil.
func (v *ObservedRefresh) Desired() *DesiredRefresh {
	if v == nil {
		return nil
	}
	return &DesiredRefresh{Schedule: v.Schedule.Clone()}
}

// Observed projects a declaration as the schedule a server would report once
// it holds it. A nil receiver is invalid.
func (v *DesiredRefresh) Observed() (*ObservedRefresh, error) {
	if err := ValidateDesiredRefresh(v); err != nil {
		return nil, err
	}
	return &ObservedRefresh{Schedule: v.Schedule.Clone()}, nil
}

// Equal reports whether two schedules hold the same clauses as written.
func (s Schedule) Equal(other Schedule) bool {
	return s.Mode == other.Mode && s.Interval == other.Interval && s.Offset == other.Offset &&
		s.Randomize == other.Randomize && s.Append == other.Append && slices.Equal(s.DependsOn, other.DependsOn)
}

// Clause writes the schedule as the clause that follows REFRESH, such as
// `EVERY 1 HOUR OFFSET 5 MINUTE DEPENDS ON db.a, db.b APPEND`.
func (s Schedule) Clause() string {
	parts := []string{s.Mode, s.Interval}
	if s.Offset != "" {
		parts = append(parts, "OFFSET", s.Offset)
	}
	if s.Randomize != "" {
		parts = append(parts, "RANDOMIZE FOR", s.Randomize)
	}
	if len(s.DependsOn) > 0 {
		parts = append(parts, "DEPENDS ON", strings.Join(s.DependsOn, ", "))
	}
	if s.Append {
		parts = append(parts, "APPEND")
	}
	return strings.Join(parts, " ")
}

// Clone returns a schedule that shares no dependency list with s.
func (s Schedule) Clone() Schedule {
	s.DependsOn = slices.Clone(s.DependsOn)
	return s
}

// ValidateDesiredRefresh checks representation invariants: a known mode, a
// nonempty interval, no OFFSET on an AFTER schedule, and nonempty dependency
// names, all valid text without NUL bytes. It does not read intervals; the
// owner's source and comparison do. Invalid data returns a
// schemaext.InvalidModelError.
func ValidateDesiredRefresh(v *DesiredRefresh) error {
	if v == nil {
		return refreshModelError(schemaext.Desired, fmt.Errorf("%w: nil ClickHouse refresh declaration", schemaext.ErrInvalidValue))
	}
	return refreshModelError(schemaext.Desired, validateSchedule(v.Schedule))
}

// ValidateObservedRefresh checks the same invariants for a reported schedule.
func ValidateObservedRefresh(v *ObservedRefresh) error {
	if v == nil {
		return refreshModelError(schemaext.Observed, fmt.Errorf("%w: nil ClickHouse refresh observation", schemaext.ErrInvalidValue))
	}
	return refreshModelError(schemaext.Observed, validateSchedule(v.Schedule))
}

func validateSchedule(s Schedule) error {
	if s.Mode != RefreshEvery && s.Mode != RefreshAfter {
		return fmt.Errorf("%w: ClickHouse refresh mode %q is neither %s nor %s", schemaext.ErrInvalidValue, s.Mode, RefreshEvery, RefreshAfter)
	}
	if err := refreshText(s.Interval, "interval"); err != nil {
		return err
	}
	if s.Offset != "" {
		if s.Mode != RefreshEvery {
			return fmt.Errorf("%w: a ClickHouse refresh OFFSET belongs to %s, and this schedule is %s", schemaext.ErrInvalidValue, RefreshEvery, s.Mode)
		}
		if err := refreshText(s.Offset, "offset"); err != nil {
			return err
		}
	}
	if s.Randomize != "" {
		if err := refreshText(s.Randomize, "randomization window"); err != nil {
			return err
		}
	}
	for _, dependency := range s.DependsOn {
		if err := refreshText(dependency, "dependency"); err != nil {
			return err
		}
	}
	return nil
}

func refreshText(value, property string) error {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: ClickHouse refresh %s must be nonempty valid text without NUL bytes", schemaext.ErrInvalidValue, property)
	}
	return nil
}

func refreshModelError(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: RefreshKind, Representation: representation, Message: err.Error()}
}
