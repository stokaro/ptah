package atlasreport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"text/template"
)

// SchemaApply is the `schema apply --format` template data.
//
// Driver and URL name the target the plan was built against. They are part of
// the Atlas template surface — `{{ .Driver }}` and `{{ .URL.Schema }}` both
// render on the pinned community binary v1.3.0 — and they are what the JSON
// document below carries alongside the statements.
//
// Changes stays a slice of rendered statements, which is Ptah's own surface:
// `{{ len .Changes }}` and `{{ range .Changes }}` work here. The JSON document
// is a different shape on purpose; see [SchemaApply.MarshalJSON].
type SchemaApply struct {
	Driver  string
	URL     atlasTemplateURL
	Changes []SchemaApplyChange

	// pending records that this report describes a plan that has not been
	// executed (`--dry-run`). It selects the JSON member the statements land
	// under, matching Atlas: a dry run reports Changes.Pending and a real
	// apply reports Changes.Applied. It is unexported because Atlas has no
	// such template field, and a field only Ptah offers would let a template
	// succeed here that the community binary refuses.
	pending bool
}

type SchemaApplyChange = SchemaChange

// SchemaApplyOptions describes the apply the report is rendered for.
type SchemaApplyOptions struct {
	// Driver is the target dialect, as `conn.Info().Dialect` reports it.
	Driver string
	// URL is the raw --url value. Sensitive query parameters and the password
	// are redacted before the report carries it.
	URL string
	// DryRun reports the statements as pending rather than applied.
	DryRun bool
	// Statements is the ordered plan.
	Statements []string
}

func NewSchemaApply(opts SchemaApplyOptions) SchemaApply {
	report := SchemaApply{
		Changes: schemaChanges(opts.Statements),
		pending: opts.DryRun,
	}
	// A synced schema carries no environment on the community binary either:
	// `{{ json . }}` there renders exactly `{"Changes":{}}` when there is
	// nothing to apply, because the command returns before it fills the env
	// in. Populating Driver and URL only when there is a plan keeps the two
	// documents identical on that path too.
	if len(opts.Statements) == 0 {
		return report
	}
	report.Driver = opts.Driver
	report.URL = atlasRedactedURL(opts.URL)
	return report
}

func WriteSchemaApply(w io.Writer, format string, result SchemaApply) error {
	return renderSchemaApplyTemplate(w, "atlas-schema-apply-format", format, result)
}

func ValidateSchemaApplyTemplate(format string) error {
	_, err := newSchemaApplyTemplate("atlas-schema-apply-format", format)
	return err
}

func renderSchemaApplyTemplate(w io.Writer, name, format string, data SchemaApply) error {
	tmpl, err := newSchemaApplyTemplate(name, format)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return fmt.Errorf("execute --format template: %w", err)
	}
	_, err = w.Write(out.Bytes())
	return err
}

// newSchemaApplyTemplate registers the helper set every Atlas Go-template
// surface exposes, plus this verb's own `sql`. `schema apply` used to register
// `sql` alone, so `{{ json . }}` — which the community binary renders here —
// failed at parse time. Every helper in the shared set was measured on the
// pinned community binary v1.3.0's own `schema apply --format`, so registering
// them cannot make this binary accept a template that one refuses.
func newSchemaApplyTemplate(name, format string) (*template.Template, error) {
	funcs := atlasTemplateFuncs()
	funcs["sql"] = schemaApplySQL
	tmpl, err := template.New(name).Funcs(funcs).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
}

// schemaApplyJSONChanges is the Changes member of the JSON document: the
// statements land under Pending before they run and under Applied after.
type schemaApplyJSONChanges struct {
	Applied []string `json:"Applied,omitempty"`
	Pending []string `json:"Pending,omitempty"`
}

// MarshalJSON renders the document `{{ json . }}` produces.
//
// The shape follows the pinned community binary v1.3.0 rather than this Go
// struct: `{"Driver":…,"URL":{…},"Changes":{"Applied":[…]}}` for a real apply
// and `"Changes":{"Pending":[…]}` for `--dry-run`. A pipeline that reads
// `.Changes.Applied` out of an Atlas run keeps reading it here.
//
// It is deliberately not the struct's own field layout: `Changes` is a
// statement slice for templates and an object with one member in JSON, exactly
// as it is on the community binary.
func (r SchemaApply) MarshalJSON() ([]byte, error) {
	statements := make([]string, 0, len(r.Changes))
	for _, change := range r.Changes {
		statements = append(statements, change.Cmd)
	}
	changes := schemaApplyJSONChanges{}
	switch {
	case len(statements) == 0:
	case r.pending:
		changes.Pending = statements
	default:
		changes.Applied = statements
	}
	return json.Marshal(struct {
		Driver  string                 `json:"Driver,omitempty"`
		URL     atlasTemplateURL       `json:"URL,omitzero"`
		Changes schemaApplyJSONChanges `json:"Changes"`
	}{
		Driver:  r.Driver,
		URL:     r.URL,
		Changes: changes,
	})
}

func (r SchemaApply) MarshalSQL(indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	sql := schemaChangesSQLText(r.Changes)
	if len(indent) == 0 || indent[0] == "" || sql == "" {
		return sql, nil
	}
	return schemaIndentSQL(sql, indent[0]), nil
}

func schemaApplySQL(result SchemaApply, indent ...string) (string, error) {
	return result.MarshalSQL(indent...)
}
