package omissionnote_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/omissionnote"
	"ptah.run/internal/rundiag"
)

// engineOption is a table option a target has no clause for.
var engineOption = renderer.Omission{
	Dialect: "sqlite", Reason: "unsupported", Kind: "table", Name: "users", Property: "table option ENGINE", Detail: "InnoDB",
}

// autoIncrement is a lost property a target offers a remedy for.
var autoIncrement = renderer.Omission{
	Dialect: "postgres", Reason: "unsupported", Kind: "column", Name: "users.id", Property: "auto-increment",
	Remedy: "declare the column type as SERIAL",
}

// TestLine names the target, the object, the property and its declared value
// where there is one, the reason token, and the remedy where there is one.
func TestLine(t *testing.T) {
	tests := []struct {
		name     string
		omission renderer.Omission
		want     string
	}{
		{name: "a property with a value", omission: engineOption,
			want: "sqlite: table users: table option ENGINE=InnoDB was skipped (unsupported)"},
		{name: "a remedy", omission: autoIncrement,
			want: "postgres: column users.id: auto-increment was skipped (unsupported); declare the column type as SERIAL"},
		{name: "a whole object", omission: renderer.Omission{Dialect: "clickhouse", Reason: "unsupported", Kind: "index", Name: "u_name"},
			want: "clickhouse: index u_name was skipped (unsupported)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(omissionnote.Line(test.omission), qt.Equals, test.want)
		})
	}
}

// TestReport_WritesEachOmissionNowAndOncePerRun reports the omissions of two
// renders in one run. Each is written by the time Report returns, ahead of
// whatever the command prints next, and an omission the second render records
// again is not written again.
func TestReport_WritesEachOmissionNowAndOncePerRun(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	omissionnote.Report(ctx, &out, []renderer.Omission{engineOption, autoIncrement})
	first := out.String()
	omissionnote.Report(ctx, &out, []renderer.Omission{autoIncrement})
	report.Flush()

	c.Assert(first, qt.Equals, "warning: "+omissionnote.Line(engineOption)+"\n"+"warning: "+omissionnote.Line(autoIncrement)+"\n")
	c.Assert(out.String(), qt.Equals, first)
}

// TestReport_WithoutARunWritesEachLine is a library caller with no run: each
// line is written at once.
func TestReport_WithoutARunWritesEachLine(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	omissionnote.Report(context.Background(), &out, []renderer.Omission{engineOption})

	c.Assert(out.String(), qt.Equals, "warning: "+omissionnote.Line(engineOption)+"\n")
}

// TestReport_WritesNothingForARenderThatLeftNothingOut is the control: a plan
// with no statement, or with statements that keep every declaration, records
// no omission and raises no warning.
func TestReport_WritesNothingForARenderThatLeftNothingOut(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	var out bytes.Buffer

	omissionnote.Report(ctx, &out, nil)
	report.Flush()

	c.Assert(out.String(), qt.Equals, "")
	c.Assert(omissionnote.Comments(nil), qt.HasLen, 0)
}

// TestComments is each omission's text once, as a line comment, in the order
// the render recorded them.
func TestComments(t *testing.T) {
	c := qt.New(t)

	comments := omissionnote.Comments([]renderer.Omission{autoIncrement, engineOption, autoIncrement})

	c.Assert(comments, qt.DeepEquals, []string{
		"-- " + omissionnote.Line(autoIncrement),
		"-- " + omissionnote.Line(engineOption),
	})
}
