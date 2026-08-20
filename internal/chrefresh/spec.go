package chrefresh

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
)

// Mode names the two ways ClickHouse schedules a refresh.
const (
	// ModeEvery refreshes on a wall-clock schedule, so a run that takes longer
	// than the interval does not push the next one back.
	ModeEvery = "EVERY"
	// ModeAfter refreshes that long after the previous run finished.
	ModeAfter = "AFTER"
)

// Canonical returns spec in the form the server stores, or an error naming what
// the server would have refused.
//
// The intervals are canonicalized ([CanonicalInterval]) and the dependencies
// are qualified with schema, because those are the two things the server
// rewrites: a comparison against what it stored has to start from the same
// place or a synchronized view reads as drifted forever.
func Canonical(spec *ast.MatViewRefreshSpec, schema string) (*ast.MatViewRefreshSpec, error) {
	if spec == nil {
		return nil, nil
	}
	mode := strings.ToUpper(strings.TrimSpace(spec.Mode))
	if mode != ModeEvery && mode != ModeAfter {
		return nil, fmt.Errorf("refresh mode %q: expected %s or %s", spec.Mode, ModeEvery, ModeAfter)
	}

	interval, err := CanonicalInterval(spec.Interval)
	if err != nil {
		return nil, err
	}
	canonical := &ast.MatViewRefreshSpec{
		Mode:     mode,
		Interval: interval,
		Append:   spec.Append,
	}

	if strings.TrimSpace(spec.Offset) != "" {
		// Measured: `AFTER 1 HOUR OFFSET 5 MINUTE` is a syntax error, so the
		// combination is refused here rather than sent.
		if mode != ModeEvery {
			return nil, fmt.Errorf("refresh OFFSET belongs to %s and this schedule is %s", ModeEvery, mode)
		}
		canonical.Offset, err = CanonicalInterval(spec.Offset)
		if err != nil {
			return nil, fmt.Errorf("refresh OFFSET: %w", err)
		}
	}
	if strings.TrimSpace(spec.Randomize) != "" {
		canonical.Randomize, err = CanonicalInterval(spec.Randomize)
		if err != nil {
			return nil, fmt.Errorf("refresh RANDOMIZE FOR: %w", err)
		}
	}
	canonical.DependsOn = qualifyDependencies(spec.DependsOn, schema)
	return canonical, nil
}

// qualifyDependencies spells every dependency `schema.view`, which is how the
// server stores one: measured, `DEPENDS ON mv_every` reads back as
// `DEPENDS ON ptah_test.mv_every`.
//
// A dependency that already names a schema keeps it, so a view may depend on
// one in another database.
func qualifyDependencies(dependencies []string, schema string) []string {
	if len(dependencies) == 0 {
		return nil
	}
	qualified := make([]string, 0, len(dependencies))
	for _, dependency := range dependencies {
		trimmed := strings.TrimSpace(dependency)
		if trimmed == "" {
			continue
		}
		if schema != "" && !strings.Contains(trimmed, ".") {
			trimmed = schema + "." + trimmed
		}
		qualified = append(qualified, trimmed)
	}
	if len(qualified) == 0 {
		return nil
	}
	return qualified
}

// Clause renders spec as the text that follows REFRESH in a CREATE statement,
// in the order the server prints it.
func Clause(spec *ast.MatViewRefreshSpec) string {
	if spec == nil {
		return ""
	}
	parts := []string{spec.Mode, spec.Interval}
	if spec.Offset != "" {
		parts = append(parts, "OFFSET", spec.Offset)
	}
	if spec.Randomize != "" {
		parts = append(parts, "RANDOMIZE FOR", spec.Randomize)
	}
	if len(spec.DependsOn) > 0 {
		parts = append(parts, "DEPENDS ON", strings.Join(spec.DependsOn, ", "))
	}
	if spec.Append {
		parts = append(parts, "APPEND")
	}
	return strings.Join(parts, " ")
}

// Equal reports whether two schedules describe the same thing.
//
// Both sides are expected to be canonical already -- the declared one through
// [Canonical] and the read one through the parser that produced it -- so this
// compares values rather than folding again. Comparing a raw declaration here
// would hide the case the canonicalizer exists for.
func Equal(a, b *ast.MatViewRefreshSpec) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Mode == b.Mode &&
		a.Interval == b.Interval &&
		a.Offset == b.Offset &&
		a.Randomize == b.Randomize &&
		a.Append == b.Append &&
		slices.Equal(a.DependsOn, b.DependsOn)
}
