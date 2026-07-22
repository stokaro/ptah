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
