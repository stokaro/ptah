package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/atlasreport"
)

// usersTableDiff is a diff planning one table, under the comment the planner
// gives the change.
func usersTableDiff() atlasreport.SchemaDiff {
	return atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{
		Dialect: "postgres",
		Changes: []atlasreport.SchemaChange{{Cmd: `CREATE TABLE "users" ("id" integer)`, Comment: `create "users" table`}},
		Limits: []atlasreport.SchemaDiffLimit{{
			Side: "--from", Kind: "roles and grants", Reason: "the source did not describe this kind",
		}},
	})
}

// TestMigrateDiffTemplate_HappyPath renders the members of the plan the pinned
// community binary v1.3.0 hands a `migrate diff --format` template that Ptah
// fills, each measured there on PostgreSQL 18.
func TestMigrateDiffTemplate_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "each change carries its statement and comment",
			format: `{{ range .Changes }}{{ .Comment }}: {{ .Cmd }}|{{ end }}`,
			want:   `create "users" table: CREATE TABLE "users" ("id" integer)|`,
		},
		{
			name:   "the changes can be counted",
			format: `{{ len .Changes }}`,
			want:   "1",
		},
		{
			name:   "sql writes the plan",
			format: `{{ sql . "" }}`,
			want:   "-- Create \"users\" table\nCREATE TABLE \"users\" (\"id\" integer);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteMigrateDiff(&out, test.format, usersTableDiff())

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestMigrateDiffTemplate_FailurePath refuses the members Ptah's diff report
// carries and the community binary's plan does not. Measured there on
// PostgreSQL 18, `{{ .MarshalSQL }}` exits 1 with `template: format:1:3:
// executing "format" at <.MarshalSQL>: can't evaluate field MarshalSQL in type
// *migrate.Plan`, and so does each of the others. Nothing is written in front
// of a refusal.
func TestMigrateDiffTemplate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		wantErr string
	}{
		{
			name:    "the plan has no MarshalSQL",
			format:  `before {{ .MarshalSQL }}`,
			wantErr: `template: format:1:10: executing "format" at <.MarshalSQL>: can't evaluate field MarshalSQL in type atlasreport.migrateDiffView`,
		},
		{
			name:    "the plan has no From",
			format:  `before {{ .From }}`,
			wantErr: `template: format:1:10: executing "format" at <.From>: can't evaluate field From in type atlasreport.migrateDiffView`,
		},
		{
			name:    "the plan has no To",
			format:  `before {{ .To }}`,
			wantErr: `template: format:1:10: executing "format" at <.To>: can't evaluate field To in type atlasreport.migrateDiffView`,
		},
		{
			name:    "the plan has no Limits",
			format:  `before {{ .Limits }}`,
			wantErr: `template: format:1:10: executing "format" at <.Limits>: can't evaluate field Limits in type atlasreport.migrateDiffView`,
		},
		{
			name:    "the plan has no NoChangesText",
			format:  `before {{ .NoChangesText }}`,
			wantErr: `template: format:1:10: executing "format" at <.NoChangesText>: can't evaluate field NoChangesText in type atlasreport.migrateDiffView`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteMigrateDiff(&out, test.format, usersTableDiff())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// TestSchemaDiffTemplate_HappyPath renders the members of the report the
// pinned community binary v1.3.0 hands a `schema diff --format` template, each
// measured there on PostgreSQL 18.
func TestSchemaDiffTemplate_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "the changes can be counted and tested",
			format: `{{ len .Changes }}|{{ if .Changes }}changed{{ end }}`,
			want:   "1|changed",
		},
		{
			name:   "MarshalSQL writes the plan",
			format: `{{ .MarshalSQL "" }}`,
			want:   "-- Create \"users\" table\nCREATE TABLE \"users\" (\"id\" integer);\n",
		},
		{
			name:   "sql writes the plan",
			format: `{{ sql . "" }}`,
			want:   "-- Create \"users\" table\nCREATE TABLE \"users\" (\"id\" integer);\n",
		},
		{
			name:   "both sides resolve",
			format: `{{ if .From }}from{{ end }}|{{ if .To }}to{{ end }}`,
			want:   "|",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteSchemaDiff(t.Context(), &out, test.format, usersTableDiff(), schemaext.Registry{})

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestSchemaDiffTemplate_FailurePath refuses the members Ptah's diff report
// carries and the community binary's does not. A change there is a schema
// change, not a statement: measured on PostgreSQL 18, `{{ range .Changes }}{{
// .Cmd }}{{ end }}` exits 1 with `template: format:1:23: executing "format" at
// <.Cmd>: can't evaluate field Cmd in type schema.Change`.
func TestSchemaDiffTemplate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		wantErr string
	}{
		{
			name:    "a change has no Cmd",
			format:  `{{ range .Changes }}{{ .Cmd }}{{ end }}`,
			wantErr: `template: format:1:23: executing "format" at <.Cmd>: can't evaluate field Cmd in type atlasreport.schemaDiffViewChange`,
		},
		{
			name:    "a change has no Comment",
			format:  `{{ range .Changes }}{{ .Comment }}{{ end }}`,
			wantErr: `template: format:1:23: executing "format" at <.Comment>: can't evaluate field Comment in type atlasreport.schemaDiffViewChange`,
		},
		{
			name:    "the report has no Limits",
			format:  `before {{ .Limits }}`,
			wantErr: `template: format:1:10: executing "format" at <.Limits>: can't evaluate field Limits in type atlasreport.schemaDiffView`,
		},
		{
			name:    "the report has no NoChangesText",
			format:  `before {{ .NoChangesText }}`,
			wantErr: `template: format:1:10: executing "format" at <.NoChangesText>: can't evaluate field NoChangesText in type atlasreport.schemaDiffView`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteSchemaDiff(t.Context(), &out, test.format, usersTableDiff(), schemaext.Registry{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// TestDiffTemplateExtensions_HappyPath hands a template the whole report once
// atlasreport.TemplateExtensionsEnvVar opens Ptah's own surface, on both
// verbs.
func TestDiffTemplateExtensions_HappyPath(t *testing.T) {
	const format = `{{ range .Changes }}{{ .Cmd }}{{ end }}|{{ len .Limits }}|{{ .MarshalSQL "" }}`
	const want = "CREATE TABLE \"users\" (\"id\" integer)|1|-- Create \"users\" table\nCREATE TABLE \"users\" (\"id\" integer);\n"
	t.Run("schema diff", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreport.TemplateExtensionsEnvVar, "1")
		var out bytes.Buffer

		err := atlasreport.WriteSchemaDiff(t.Context(), &out, format, usersTableDiff(), schemaext.Registry{})

		c.Assert(err, qt.IsNil)
		c.Assert(out.String(), qt.Equals, want)
	})
	t.Run("migrate diff", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(atlasreport.TemplateExtensionsEnvVar, "1")
		var out bytes.Buffer

		err := atlasreport.WriteMigrateDiff(&out, format, usersTableDiff())

		c.Assert(err, qt.IsNil)
		c.Assert(out.String(), qt.Equals, want)
	})
}

// TestSchemaDiffOwnFormats_ReadTheWholeReport is the control for the views: the
// templates Ptah writes for `schema diff`, the compat default and native `ptah
// schema diff`'s, read NoChangesText, which no view carries, and still render
// with the variable absent.
func TestSchemaDiffOwnFormats_ReadTheWholeReport(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{name: "compat default", format: atlasreport.NormalizeSchemaDiffFormat("")},
		{name: "native", format: atlasreport.NativeSchemaDiffFormat},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{})

			err := atlasreport.WriteSchemaDiff(t.Context(), &out, test.format, report, schemaext.Registry{})

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}
