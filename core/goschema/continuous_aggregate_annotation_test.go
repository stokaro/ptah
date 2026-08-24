package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// TestParseSource_ReadsTheContinuousAggregateAnnotation pins what the
// annotation carries, and that the body is kept as it was WRITTEN.
//
// The catalog stores a rewritten SELECT, and the comparison puts the
// declaration through the same rewrite rather than folding either text -- so a
// body normalized here would be normalized twice and match nothing
// (stokaro/ptah#1026).
func TestParseSource_ReadsTheContinuousAggregateAnnotation(t *testing.T) {
	c := qt.New(t)
	materializedOnly := true
	const body = "SELECT time_bucket('1 hour', time) AS bucket, avg(value) AS v " +
		"FROM readings GROUP BY bucket"
	source := "package models\n\n" +
		"//ptah:schema:continuousaggregate name=\"hourly\" schema=\"metrics\" body=\"" + body + "\" " +
		"materialized_only=\"true\" comment=\"one row per hour\"\n" +
		"type Hourly struct{}\n"

	db := mustParseSource(c, "aggregate.go", source)

	c.Assert(db.ContinuousAggregates, qt.DeepEquals, []goschema.ContinuousAggregate{{
		StructName: "Hourly", Name: "hourly", Schema: "metrics", Body: body,
		MaterializedOnly: &materializedOnly, Comment: "one row per hour",
	}})
}

// TestParseSource_TheAggregateOptionDefaultsOff is the control on the boolean:
// an omitted attribute is the server's own default, not true.
func TestParseSource_TheAggregateOptionDefaultsOff(t *testing.T) {
	c := qt.New(t)
	source := "package models\n\n" +
		"//ptah:schema:continuousaggregate name=\"hourly\" body=\"SELECT 1\"\n" +
		"type Hourly struct{}\n"

	db := mustParseSource(c, "aggregate.go", source)

	c.Assert(db.ContinuousAggregates, qt.DeepEquals, []goschema.ContinuousAggregate{{
		StructName: "Hourly", Name: "hourly", Body: "SELECT 1",
	}})
}

// TestParseSource_TheAggregateRequiresANameAndABody pins the two attributes an
// aggregate cannot be declared without.
func TestParseSource_TheAggregateRequiresANameAndABody(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
	}{
		{name: "no name", annotation: "//ptah:schema:continuousaggregate body=\"SELECT 1\""},
		{name: "no body", annotation: "//ptah:schema:continuousaggregate name=\"hourly\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package models\n\n" + test.annotation + "\ntype Hourly struct{}\n"

			_, err := goschema.ParseSource("aggregate.go", source)

			c.Assert(err, qt.IsNotNil)
		})
	}
}
