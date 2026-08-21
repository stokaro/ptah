package atlasreport

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/template"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/envbool"
)

// SchemaDiffTemplateHelpersEnvVar opens the shared `--format` helper set on
// compat `schema diff`.
//
// The narrow registration is deliberate and stays the default: the pinned
// community binary offers `sql` alone here, and registering more would let
// ptah-compat render a template that binary refuses -- the first half of the
// compatibility policy. The second half says compatibility must not withhold a
// capability Ptah has, and Ptah has the document: `schema apply` and
// `schema inspect` already render it, and native `ptah schema diff --format
// json` emits a machine-readable diff with no variable at all.
//
// So the fuller behavior lives behind this variable rather than behind a new
// flag, leaving the command and flag inventory identical (stokaro/ptah#1705).
// It is Gated because a true value adds a reading the pinned binary does not
// have.
const SchemaDiffTemplateHelpersEnvVar = "PTAH_SCHEMA_DIFF_TEMPLATE_HELPERS"

var schemaDiffTemplateHelpers = envbool.New(SchemaDiffTemplateHelpersEnvVar, false, envbool.Gated)

const schemaDiffDefaultFormat = `{{- with .Changes -}}
{{ sql $ }}
{{- else -}}
Schemas are synced, no changes to be made.
{{ end -}}
`

const migrateDiffDefaultFormat = `{{ sql . "  " }}`

type SchemaDiff struct {
	From    *goschema.Database
	To      *goschema.Database
	Changes []SchemaDiffChange
}

type SchemaChange struct {
	Cmd string
}

type SchemaDiffChange = SchemaChange

func NewSchemaDiff(from, to *goschema.Database, statements []string) SchemaDiff {
	return SchemaDiff{
		From:    from,
		To:      to,
		Changes: schemaChanges(statements),
	}
}

func WriteSchemaDiff(w io.Writer, format string, result SchemaDiff) error {
	return renderSchemaDiffTemplate(w, "atlas-schema-diff-format", format, result)
}

func ValidateSchemaDiffTemplate(format string) error {
	_, err := newSchemaDiffTemplate("atlas-schema-diff-format", format)
	return err
}

func renderSchemaDiffTemplate(w io.Writer, name, format string, data SchemaDiff) error {
	tmpl, err := newSchemaDiffTemplate(name, format)
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

func newSchemaDiffTemplate(name, format string) (*template.Template, error) {
	funcs, err := schemaDiffTemplateFuncs()
	if err != nil {
		return nil, err
	}
	tmpl, err := template.New(name).Funcs(funcs).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
}

// schemaDiffTemplateFuncs is `sql` alone by default, and the shared set plus
// `sql` when [SchemaDiffTemplateHelpersEnvVar] is on.
//
// The variable is resolved here rather than at start-up so that a malformed
// value is refused by the command that would have used it, naming the
// template it refused to parse.
func schemaDiffTemplateFuncs() (template.FuncMap, error) {
	enabled, err := schemaDiffTemplateHelpers.Resolve()
	if err != nil {
		return nil, err
	}
	if !enabled {
		return template.FuncMap{"sql": schemaDiffSQL}, nil
	}
	funcs := atlasTemplateFuncs()
	funcs["sql"] = schemaDiffSQL
	return funcs, nil
}

func NormalizeSchemaDiffFormat(format string) string {
	if strings.TrimSpace(format) == "" {
		return schemaDiffDefaultFormat
	}
	return format
}

func NormalizeMigrateDiffFormat(format string) string {
	if strings.TrimSpace(format) == "" {
		return migrateDiffDefaultFormat
	}
	return format
}

func schemaChanges(statements []string) []SchemaChange {
	changes := make([]SchemaChange, 0, len(statements))
	for _, statement := range statements {
		changes = append(changes, SchemaChange{Cmd: schemaStatement(statement)})
	}
	return changes
}

func (r SchemaDiff) MarshalSQL(indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	sql := schemaChangesSQLText(r.Changes)
	if len(indent) == 0 || indent[0] == "" || sql == "" {
		return sql, nil
	}
	return schemaIndentSQL(sql, indent[0]), nil
}

func schemaDiffSQL(result SchemaDiff, indent ...string) (string, error) {
	return result.MarshalSQL(indent...)
}

func schemaChangesSQLText(changes []SchemaChange) string {
	var sql strings.Builder
	for _, change := range changes {
		fmt.Fprintf(&sql, "%s;\n", strings.TrimSuffix(change.Cmd, ";"))
	}
	return sql.String()
}

func schemaStatement(statement string) string {
	return strings.TrimSuffix(statement, ";")
}

func schemaIndentSQL(sql, indent string) string {
	trimmed := strings.TrimSuffix(sql, "\n")
	return indent + strings.ReplaceAll(trimmed, "\n", "\n"+indent) + "\n"
}
