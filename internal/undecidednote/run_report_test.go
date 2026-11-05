package undecidednote_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/schemadiff"
)

// withheldRole is a declared role a refused catalog read left undecided.
func withheldRole(name string) coverage.Object {
	refused := coverage.Refused(coverage.Role)
	return coverage.Object{Kind: coverage.Role, Name: name, Reason: refused.Reason, Provenance: refused.Provenance}
}

// withheldSequence is a declared sequence a document records it does not
// describe.
func withheldSequence(name string) coverage.Object {
	return coverage.Object{Kind: coverage.Sequence, Name: name}
}

// Within a run the undecided objects are one warning, written before Report
// returns rather than when the run ends, because the plan, the confirmation
// or the error that follows is about them. Each object is a change the plan
// leaves out, so the warning names every one, grouped by why it was not
// decided, and the end of the run does not repeat it.
func TestReport_NamesEveryObjectInOneWarningForTheRun(t *testing.T) {
	c := qt.New(t)
	report := rundiag.New(rundiag.Options{})
	ctx := rundiag.With(c.Context(), report)
	var diagnostics bytes.Buffer

	undecidednote.Report(ctx, &diagnostics, schemadiff.Diagnostics{
		Common: []coverage.Object{withheldRole("reporter"), withheldSequence("order_seq"), withheldRole("auditor")},
		Features: []schemaext.UndecidedChange{{
			Kind: "test/extension", Label: "PostgreSQL extension", Reason: "it was not read",
			Subject: objectidentity.ID{Kind: "test/extension", Name: objectidentity.Part{Source: "citext", Normalized: "citext"}},
		}},
	}, "the database", "the desired schema")

	want := `warning: 4 objects get no planned change because the comparison could not decide them: ` +
		`role "reporter", role "auditor" (the database does not describe role objects because the read was refused` +
		` the catalog that would have listed them); sequence "order_seq" (the database records` +
		" `ptah:not-described sequence`); PostgreSQL extension citext (it was not read);" +
		" PTAH_LOG_LEVEL=debug lists them\n"
	c.Assert(diagnostics.String(), qt.Equals, want)
	report.Flush()
	c.Assert(diagnostics.String(), qt.Equals, want)
}

// One undecided object is its own line, which says why in full.
func TestReport_OneObjectIsItsOwnLineForTheRun(t *testing.T) {
	c := qt.New(t)
	ctx := rundiag.With(c.Context(), rundiag.New(rundiag.Options{}))
	var diagnostics bytes.Buffer

	undecidednote.Report(ctx, &diagnostics, schemadiff.Diagnostics{Common: []coverage.Object{withheldRole("reporter")}},
		"the database", "the desired schema")

	c.Assert(diagnostics.String(), qt.Equals, `warning: role "reporter" is declared by the desired schema`+
		` but no change was planned for it: the database does not describe role objects because`+
		` the read was refused the catalog that would have listed them, so this comparison`+
		` cannot tell it apart from one that already exists, and the creation Ptah renders`+
		" for it cannot safely converge from an unknown current state\n")
}

// A run whose log is JSON writes the warning as one record, with its
// category and how many objects it names, and no text line.
func TestReport_IsARecordWhenTheLogIsJSON(t *testing.T) {
	c := qt.New(t)
	ctx := rundiag.With(c.Context(), rundiag.New(rundiag.Options{Records: true}))
	var diagnostics bytes.Buffer

	undecidednote.Report(ctx, &diagnostics, schemadiff.Diagnostics{
		Common: []coverage.Object{withheldSequence("a_seq"), withheldSequence("b_seq")},
	}, "--from", "--to")

	lines := strings.Split(strings.TrimSuffix(diagnostics.String(), "\n"), "\n")
	c.Assert(lines, qt.HasLen, 1)
	var record map[string]any
	c.Assert(json.Unmarshal([]byte(lines[0]), &record), qt.IsNil)
	c.Assert(record["level"], qt.Equals, "WARN")
	c.Assert(record["category"], qt.Equals, "undecided-object")
	c.Assert(record["count"], qt.Equals, float64(2))
	c.Assert(record["msg"], qt.Equals, `2 objects get no planned change because the comparison could not decide them: `+
		`sequence "a_seq", sequence "b_seq" (--from records `+"`ptah:not-described sequence`"+`); PTAH_LOG_LEVEL=debug lists them`)
}

// A second comparison in the run that withholds the same object for the same
// reason does not report it again.
func TestReport_ReportsAnObjectOncePerRun(t *testing.T) {
	c := qt.New(t)
	ctx := rundiag.With(c.Context(), rundiag.New(rundiag.Options{}))
	var diagnostics bytes.Buffer
	undecided := schemadiff.Diagnostics{Common: []coverage.Object{withheldSequence("a_seq")}}

	undecidednote.Report(ctx, &diagnostics, undecided, "--from", "--to")
	undecidednote.Report(ctx, &diagnostics, undecided, "--from", "--to")

	c.Assert(strings.Count(diagnostics.String(), "\n"), qt.Equals, 1)
}
