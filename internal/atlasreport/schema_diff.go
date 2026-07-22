package atlasreport

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/template"

	"github.com/stokaro/ptah/core/goschema"
)

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

type SchemaDiffChange struct {
	Cmd string
}

func NewSchemaDiff(from, to *goschema.Database, statements []string) SchemaDiff {
	return SchemaDiff{
		From:    from,
		To:      to,
		Changes: schemaDiffChanges(statements),
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
	tmpl, err := template.New(name).Funcs(template.FuncMap{
		"sql": schemaDiffSQL,
	}).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
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

func schemaDiffChanges(statements []string) []SchemaDiffChange {
	changes := make([]SchemaDiffChange, 0, len(statements))
	for _, statement := range statements {
		changes = append(changes, SchemaDiffChange{Cmd: schemaDiffStatement(statement)})
	}
	return changes
}

func (r SchemaDiff) MarshalSQL(indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	sql := schemaDiffSQLText(r.Changes)
	if len(indent) == 0 || indent[0] == "" || sql == "" {
		return sql, nil
	}
	return schemaDiffIndentSQL(sql, indent[0]), nil
}

func schemaDiffSQL(result SchemaDiff, indent ...string) (string, error) {
	return result.MarshalSQL(indent...)
}

func schemaDiffSQLText(changes []SchemaDiffChange) string {
	var sql strings.Builder
	for _, change := range changes {
		fmt.Fprintf(&sql, "%s;\n", strings.TrimSuffix(change.Cmd, ";"))
	}
	return sql.String()
}

func schemaDiffStatement(statement string) string {
	return strings.TrimSuffix(statement, ";")
}

func schemaDiffIndentSQL(sql, indent string) string {
	trimmed := strings.TrimSuffix(sql, "\n")
	return indent + strings.ReplaceAll(trimmed, "\n", "\n"+indent) + "\n"
}
