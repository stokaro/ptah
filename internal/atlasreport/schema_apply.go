package atlasreport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"text/template"
)

// SchemaApply is the `schema apply --format` template data, in the shape the
// pinned community binary v1.3.0 hands its template. Measured on PostgreSQL 18,
// these render there:
//
//	{{ .Driver }} {{ .URL.Host }} {{ .Env.Driver }} {{ .Dir }} {{ .Error }}
//	{{ range .Changes.Pending }}{{ .Cmd }} {{ .Comment }}{{ end }}
//	{{ .Changes.Applied }} {{ .Changes.Error }}
//
// and `{{ range .Changes }}`, `{{ len .Changes }}`, `{{ .MarshalSQL }}` and
// `{{ .MarshalJSON }}` fail, because Changes is a struct there and the report
// has neither method. The members here are the ones that binary carries and
// Ptah fills; the rest of its change members -- Args, Reverse, Source -- fail
// here, where a template reading them would print a value Ptah does not have.
type SchemaApply struct {
	// Env names the target the plan was built against. Its members are
	// promoted, as the community binary's are: `{{ .Driver }}` and
	// `{{ .Env.Driver }}` read the same value.
	Env
	// Changes holds the plan: Pending before it runs, on `--dry-run`, and
	// Applied once it has.
	Changes SchemaApplyChanges
	// Error is the community binary's member for a failed run. A run that
	// fails here reports on stderr, so it is empty whenever a template renders.
	Error string `json:"Error,omitempty"`

	// dialect is the dialect the statements were planned for, which the `sql`
	// helper reads them as when it indents them. It is unexported because
	// the community binary has no such template member.
	dialect string
}

// Env is the target of a `schema apply` report. It is the type of
// [SchemaApply.Env] under the name the community binary gives that member, so
// that `{{ .Env.Driver }}` resolves.
type Env = atlasEnv

// SchemaApplyChanges is the plan of a `schema apply` report.
type SchemaApplyChanges struct {
	Applied []SchemaApplyChange
	Pending []SchemaApplyChange
	// Error is the community binary's member for the statement a run failed
	// on. It is nil whenever a template renders, and prints as `<nil>` there.
	Error error
}

// MarshalJSON renders the document `{{ json .Changes }}` produces: each
// statement as a string, under Pending before the plan runs and under Applied
// after, `{"Pending":["CREATE TABLE ..."]}`. The community binary writes the
// same document, and an empty list is left out there too.
func (c SchemaApplyChanges) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Applied []string `json:"Applied,omitempty"`
		Pending []string `json:"Pending,omitempty"`
	}{
		Applied: changeStatements(c.Applied),
		Pending: changeStatements(c.Pending),
	})
}

func changeStatements(changes []SchemaChange) []string {
	if len(changes) == 0 {
		return nil
	}
	statements := make([]string, 0, len(changes))
	for _, change := range changes {
		statements = append(statements, change.Cmd)
	}
	return statements
}

type SchemaApplyChange = SchemaChange

// SchemaApplyOptions describes the apply the report is rendered for.
type SchemaApplyOptions struct {
	// Driver is the target dialect, as `conn.Info().Dialect` reports it. The
	// `sql` helper reads the statements as this dialect when it indents them.
	Driver string
	// URL is the raw --url value. Sensitive query parameters and the password
	// are redacted before the report carries it.
	URL string
	// DryRun reports the statements as pending rather than applied.
	DryRun bool
	// Statements is the ordered plan.
	Statements []string
	// Comments names the change of each statement of Statements, at the same
	// index. Empty names no change.
	Comments []string
}

func NewSchemaApply(opts SchemaApplyOptions) SchemaApply {
	changes := schemaChanges(opts.Statements)
	for i := range changes {
		if i < len(opts.Comments) {
			changes[i].Comment = opts.Comments[i]
		}
	}
	report := SchemaApply{dialect: opts.Driver}
	// A synced schema carries no environment on the community binary either:
	// `{{ json . }}` there renders exactly `{"Changes":{}}` when there is
	// nothing to apply, because the command returns before it fills the env
	// in. Populating Driver and URL only when there is a plan keeps the two
	// documents identical on that path too.
	if len(changes) == 0 {
		return report
	}
	if opts.DryRun {
		report.Changes.Pending = changes
	} else {
		report.Changes.Applied = changes
	}
	report.Driver = opts.Driver
	report.URL = atlasRedactedURL(opts.URL)
	return report
}

func WriteSchemaApply(w io.Writer, format string, result SchemaApply) error {
	return renderSchemaApplyTemplate(w, format, result)
}

func ValidateSchemaApplyTemplate(format string) error {
	_, err := newSchemaApplyTemplate(format)
	return err
}

func renderSchemaApplyTemplate(w io.Writer, format string, data SchemaApply) error {
	tmpl, err := newSchemaApplyTemplate(format)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return atlasSchemaVerbTemplateWording.executeError(err)
	}
	_, err = w.Write(out.Bytes())
	return err
}

// newSchemaApplyTemplate parses format with the helpers the community binary
// defines here, the whole shared set, and with Ptah's own `sql` when
// [TemplateExtensionsEnvVar] is on. That binary has no `sql` on this verb:
// `{{ sql . }}` exits 1 there with `parse log format: template: format:1:
// function "sql" not defined`.
func newSchemaApplyTemplate(format string) (*template.Template, error) {
	funcs := atlasTemplateFuncs()
	extended, err := ResolveTemplateExtensions()
	if err != nil {
		return nil, err
	}
	if extended {
		funcs["sql"] = schemaApplySQL
	}
	wording := atlasSchemaVerbTemplateWording
	tmpl, err := template.New(wording.name).Funcs(funcs).Parse(format)
	if err != nil {
		return nil, wording.parseError(err)
	}
	return tmpl, nil
}

// schemaApplySQL is the `sql` helper of [TemplateExtensionsEnvVar]: the plan as
// a script, each change's comment above its statement, indented with indent.
func schemaApplySQL(result SchemaApply, indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	changes := append(slices.Clone(result.Changes.Applied), result.Changes.Pending...)
	sql := schemaChangesSQLText(changes, capitalized, statementEnd(result.dialect))
	if len(indent) == 0 || indent[0] == "" || sql == "" {
		return sql, nil
	}
	return schemaIndentSQL(result.dialect, sql, indent[0]), nil
}
