package atlasreport

import (
	"bytes"
	"fmt"
	"io"
	"text/template"
)

type SchemaApply struct {
	Changes []SchemaApplyChange
}

type SchemaApplyChange = SchemaChange

func NewSchemaApply(statements []string) SchemaApply {
	return SchemaApply{
		Changes: schemaChanges(statements),
	}
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

func newSchemaApplyTemplate(name, format string) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(template.FuncMap{
		"sql": schemaApplySQL,
	}).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
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
