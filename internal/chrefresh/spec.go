package chrefresh

import (
	"fmt"
	"strings"

	"ptah.run/dialect/clickhouse/chschema"
)

// Canonical returns spec in the form the server stores, or an error naming what
// the server would have refused.
//
// The intervals are canonicalized ([CanonicalInterval]) and the dependencies
// are qualified with schema, because those are the two values the server
// rewrites: a comparison against what it stored has to start from the same
// place or a synchronized view reads as drifted forever.
func Canonical(spec *chschema.Schedule, schema string) (*chschema.Schedule, error) {
	if spec == nil {
		return nil, nil
	}
	mode := strings.ToUpper(strings.TrimSpace(spec.Mode))
	if mode != chschema.RefreshEvery && mode != chschema.RefreshAfter {
		return nil, fmt.Errorf("refresh mode %q: expected %s or %s", spec.Mode, chschema.RefreshEvery, chschema.RefreshAfter)
	}

	interval, err := CanonicalInterval(spec.Interval)
	if err != nil {
		return nil, err
	}
	canonical := &chschema.Schedule{
		Mode:     mode,
		Interval: interval,
		Append:   spec.Append,
	}

	if strings.TrimSpace(spec.Offset) != "" {
		// Measured: `AFTER 1 HOUR OFFSET 5 MINUTE` is a syntax error, so the
		// combination is refused here rather than sent.
		if mode != chschema.RefreshEvery {
			return nil, fmt.Errorf("refresh OFFSET belongs to %s and this schedule is %s", chschema.RefreshEvery, mode)
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
