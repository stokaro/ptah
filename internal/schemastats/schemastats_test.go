package schemastats_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemastats"
)

// metricValue reads one metric's sample line out of a rendered scrape.
func metricValue(c *qt.C, body, name string) string {
	c.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			fields := strings.Fields(line)
			return fields[len(fields)-1]
		}
	}
	return ""
}

// render is Collect plus WriteOpenMetrics, which is how both verbs use them.
func render(c *qt.C, db *goschema.Database, labels map[string]string) string {
	c.Helper()
	var out strings.Builder
	c.Assert(schemastats.WriteOpenMetrics(&out, schemastats.Collect(db), labels), qt.IsNil)
	return out.String()
}

// TestCollect_CountsEachObjectKind pins the counting.
//
// Each row puts a different number of objects in a different collection, so a
// metric reading another collection's length is visible rather than hidden
// behind equal counts (stokaro/ptah#1711).
func TestCollect_CountsEachObjectKind(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables:            []goschema.Table{{Name: "a"}, {Name: "b"}},
		Fields:            []goschema.Field{{Name: "c1"}, {Name: "c2"}, {Name: "c3"}},
		Indexes:           []goschema.Index{{Name: "i"}},
		Views:             []goschema.View{{Name: "v1"}, {Name: "v2"}, {Name: "v3"}, {Name: "v4"}},
		MaterializedViews: []goschema.MaterializedView{{Name: "m"}},
		Enums:             []goschema.Enum{{Name: "e1"}, {Name: "e2"}, {Name: "e3"}, {Name: "e4"}, {Name: "e5"}},
	}

	body := render(c, db, nil)

	c.Assert(metricValue(c, body, "ptah_schema_tables"), qt.Equals, "2")
	c.Assert(metricValue(c, body, "ptah_schema_columns"), qt.Equals, "3")
	c.Assert(metricValue(c, body, "ptah_schema_indexes"), qt.Equals, "1")
	c.Assert(metricValue(c, body, "ptah_schema_views"), qt.Equals, "4")
	c.Assert(metricValue(c, body, "ptah_schema_materialized_views"), qt.Equals, "1")
	c.Assert(metricValue(c, body, "ptah_schema_enums"), qt.Equals, "5")
	// A kind with nothing in it reports zero rather than being absent: a
	// pipeline charting a series needs the series to exist before it can fall
	// to zero.
	c.Assert(metricValue(c, body, "ptah_schema_triggers"), qt.Equals, "0")
}

// TestWriteOpenMetrics_CarriesTheFormatsRequiredParts covers the three rules a
// collector enforces and a hand-rolled renderer usually misses.
func TestWriteOpenMetrics_CarriesTheFormatsRequiredParts(t *testing.T) {
	c := qt.New(t)

	body := render(c, &goschema.Database{Tables: []goschema.Table{{Name: "a"}}}, nil)

	// Every metric declares its help and its type before its sample.
	c.Assert(body, qt.Contains, "# HELP ptah_schema_tables Tables.\n")
	c.Assert(body, qt.Contains, "# TYPE ptah_schema_tables gauge\n")
	// A gauge, not a counter: a schema loses objects as well as gaining them.
	c.Assert(body, qt.Not(qt.Contains), "counter")
	// The body ends with the EOF marker. Without it a collector reads the
	// scrape as truncated rather than complete.
	c.Assert(strings.HasSuffix(body, "# EOF\n"), qt.IsTrue)
}

// TestWriteOpenMetrics_LabelsAreStableAndEscaped covers the two ways a label
// set breaks a scrape.
func TestWriteOpenMetrics_LabelsAreStableAndEscaped(t *testing.T) {
	c := qt.New(t)
	labels := map[string]string{"dialect": "postgres", "schemas": `we"ird`, "blank": "  "}

	first := render(c, &goschema.Database{}, labels)
	second := render(c, &goschema.Database{}, labels)

	// Sorted, not map order: a scrape that reorders its labels between runs is
	// a different series to some collectors, and Go's map iteration is
	// deliberately unordered.
	c.Assert(first, qt.Equals, second)
	c.Assert(first, qt.Contains, `{dialect="postgres",schemas="we\"ird"}`)
	// A blank value is dropped rather than emitted empty.
	c.Assert(first, qt.Not(qt.Contains), "blank=")
}

// TestWriteOpenMetrics_NoLabelsEmitsNoBraces keeps the sample line valid when
// nothing labels it.
func TestWriteOpenMetrics_NoLabelsEmitsNoBraces(t *testing.T) {
	c := qt.New(t)

	body := render(c, &goschema.Database{Tables: []goschema.Table{{Name: "a"}}}, nil)

	c.Assert(body, qt.Contains, "ptah_schema_tables 1\n")
	c.Assert(body, qt.Not(qt.Contains), "ptah_schema_tables{}")
}

// TestCollect_NilDatabaseScrapesAsZeroes states the choice for an absent
// schema.
//
// "No schema" and "an empty schema" are the same shape to a metrics pipeline,
// and a scrape that errors is worse than one reporting zeroes it can chart.
func TestCollect_NilDatabaseScrapesAsZeroes(t *testing.T) {
	c := qt.New(t)

	body := render(c, nil, nil)

	c.Assert(metricValue(c, body, "ptah_schema_tables"), qt.Equals, "0")
	c.Assert(strings.HasSuffix(body, "# EOF\n"), qt.IsTrue)
}
