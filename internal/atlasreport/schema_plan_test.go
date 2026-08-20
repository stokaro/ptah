package atlasreport_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
)

func schemaPlanFixture() atlasreport.SchemaPlan {
	return atlasreport.NewSchemaPlan(atlasreport.SchemaPlanOptions{
		Name:        "plan_5bc9860ba0ab",
		Dialect:     "postgres",
		From:        "sha256:aa",
		To:          "sha256:bb",
		Exclude:     []string{"audit_*"},
		Destructive: true,
		Statements: []atlasreport.SchemaPlanChange{
			{Cmd: `CREATE TABLE "users" (id INT)`, Severity: "safe", Reason: "does not remove data"},
			{Cmd: `DROP TABLE "old"`, Severity: "destructive", Reason: "removes data"},
		},
		MigrationBody: "-- atlas:txmode none\n\nCREATE TABLE \"users\" (id INT);\nDROP TABLE \"old\";\n",
	})
}

// TestWriteSchemaPlanRendersTheDocumentedPayload pins the field names.
//
// They are the contract this issue exists to settle: the payload had no shape
// because the flag rendered nothing, and a template written against it now
// cannot be broken by a later rename without this test going red
// (stokaro/ptah#1700).
func TestWriteSchemaPlanRendersTheDocumentedPayload(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := atlasreport.WriteSchemaPlan(&out, "{{ json . }}", schemaPlanFixture())

	c.Assert(err, qt.IsNil)
	var payload map[string]any
	c.Assert(json.Unmarshal([]byte(out.String()), &payload), qt.IsNil)
	c.Assert(payload["Name"], qt.Equals, "plan_5bc9860ba0ab")
	c.Assert(payload["Dialect"], qt.Equals, "postgres")
	c.Assert(payload["From"], qt.Equals, "sha256:aa")
	c.Assert(payload["To"], qt.Equals, "sha256:bb")
	c.Assert(payload["Destructive"], qt.Equals, true)
	c.Assert(payload["Exclude"], qt.DeepEquals, []any{"audit_*"})
	c.Assert(payload["MigrationBody"], qt.Contains, "-- atlas:txmode none")

	changes, ok := payload["Changes"].([]any)
	c.Assert(ok, qt.IsTrue)
	c.Assert(changes, qt.HasLen, 2)
	first, ok := changes[0].(map[string]any)
	c.Assert(ok, qt.IsTrue)
	// .Changes[].Cmd is the sibling verb's spelling, so a template written for
	// `schema diff` reads a plan unchanged.
	c.Assert(first["Cmd"], qt.Equals, `CREATE TABLE "users" (id INT)`)
	c.Assert(first["Severity"], qt.Equals, "safe")
	c.Assert(first["Reason"], qt.Equals, "does not remove data")
}

// TestWriteSchemaPlanOmitsEmptyOptionalFields keeps a consumer from having to
// distinguish "no exclusions" from "the empty list".
func TestWriteSchemaPlanOmitsEmptyOptionalFields(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := atlasreport.WriteSchemaPlan(&out, "{{ json . }}", atlasreport.NewSchemaPlan(
		atlasreport.SchemaPlanOptions{Name: "p"}))

	c.Assert(err, qt.IsNil)
	var payload map[string]any
	c.Assert(json.Unmarshal([]byte(out.String()), &payload), qt.IsNil)
	_, hasExclude := payload["Exclude"]
	c.Assert(hasExclude, qt.IsFalse)
	// Changes is always present, and always a list: a consumer ranging over it
	// on a synced schema must get an empty array rather than null.
	c.Assert(payload["Changes"], qt.DeepEquals, make([]any, 0))
}

func TestWriteSchemaPlanSQLHelper(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "plain",
			format: "{{ sql . }}",
			want:   "CREATE TABLE \"users\" (id INT);\nDROP TABLE \"old\";\n",
		},
		{
			name:   "indented",
			format: `{{ sql . "  " }}`,
			want:   "  CREATE TABLE \"users\" (id INT);\n  DROP TABLE \"old\";\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out strings.Builder

			err := atlasreport.WriteSchemaPlan(&out, test.format, schemaPlanFixture())

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestWriteSchemaPlanRegistersTheSharedHelperSet is the difference from
// `schema diff`, stated as a test.
//
// There, the shared helpers are behind an environment variable, because the
// pinned community binary offers `sql` alone and rendering more would succeed
// on a template that binary refuses. Here that binary runs the verb not at
// all, so there is no narrower surface to stay inside of.
func TestWriteSchemaPlanRegistersTheSharedHelperSet(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "upper", format: "{{ upper .Dialect }}", want: "POSTGRES"},
		{name: "add", format: "{{ add 1 2 }}", want: "3"},
		{name: "color helpers are identity", format: "{{ red .Name }}", want: "plan_5bc9860ba0ab"},
		{name: "json_merge", format: `{{ json_merge (json .) (json .) }}`, want: `"Dialect":"postgres"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out strings.Builder

			err := atlasreport.WriteSchemaPlan(&out, test.format, schemaPlanFixture())

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}

func TestValidateSchemaPlanTemplate(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "unclosed action", format: "{{ json .", want: `parse --format template: .*`},
		{name: "unknown function", format: "{{ frobnicate . }}", want: `parse --format template: .*frobnicate.*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasreport.ValidateSchemaPlanTemplate(test.format), qt.ErrorMatches, test.want)
		})
	}
}

func TestValidateSchemaPlanTemplateAcceptsTheDocumentedOne(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasreport.ValidateSchemaPlanTemplate("{{ json . }}"), qt.IsNil)
}

// TestWriteSchemaPlanReportsAnExecutionFailure separates parsing from
// rendering: a template that parses can still fail on the data, and the error
// has to name which stage refused it.
func TestWriteSchemaPlanReportsAnExecutionFailure(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := atlasreport.WriteSchemaPlan(&out, `{{ sql . "a" "b" }}`, schemaPlanFixture())

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unexpected number of arguments: 2.*`)
}
