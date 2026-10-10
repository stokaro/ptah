package atlasreport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"text/template"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/envbool"
	"ptah.run/internal/featurejson"
	"ptah.run/internal/sqlscript"
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
	From    *schemamodel.Database
	To      *schemamodel.Database
	Changes []SchemaDiffChange
}

type SchemaChange struct {
	Cmd string
}

type SchemaDiffChange = SchemaChange

func NewSchemaDiff(from, to *schemamodel.Database, statements []string) SchemaDiff {
	return SchemaDiff{
		From:    from,
		To:      to,
		Changes: schemaChanges(statements),
	}
}

// WriteSchemaDiff renders a `schema diff --format` template over result. With
// the template helpers enabled, `json` encodes the feature data of `.From` and
// `.To` through codecs, the registry of the runtime that produced the diff.
func WriteSchemaDiff(ctx context.Context, w io.Writer, format string, result SchemaDiff, codecs schemaext.Registry) error {
	return renderSchemaDiffTemplate(ctx, w, atlasSchemaVerbTemplateWording, format, result, codecs)
}

func ValidateSchemaDiffTemplate(format string) error {
	_, err := newSchemaDiffTemplate(context.Background(), atlasSchemaVerbTemplateWording, format, schemaext.Registry{})
	return err
}

// WriteMigrateDiff renders a `migrate diff --format` template over the
// planned statements. It reads the same document as `schema diff` and refuses
// a template in `migrate diff`'s own words; see [templateWording].
//
// The document a migration directory diff renders has no `.From` or `.To`, so
// it carries no feature data and its `json` helper needs no codecs.
func WriteMigrateDiff(w io.Writer, format string, result SchemaDiff) error {
	return renderSchemaDiffTemplate(context.Background(), w, atlasMigrateVerbTemplateWording, format, result, schemaext.Registry{})
}

// ValidateMigrateDiffTemplate parses a `migrate diff --format` template so a
// malformed one fails before the directory is replayed.
func ValidateMigrateDiffTemplate(format string) error {
	_, err := newSchemaDiffTemplate(context.Background(), atlasMigrateVerbTemplateWording, format, schemaext.Registry{})
	return err
}

func renderSchemaDiffTemplate(ctx context.Context, w io.Writer, wording templateWording, format string, data SchemaDiff, codecs schemaext.Registry) error {
	tmpl, err := newSchemaDiffTemplate(ctx, wording, format, codecs)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return wording.executeError(err)
	}
	_, err = w.Write(out.Bytes())
	return err
}

func newSchemaDiffTemplate(ctx context.Context, wording templateWording, format string, codecs schemaext.Registry) (*template.Template, error) {
	funcs, err := schemaDiffTemplateFuncs(ctx, codecs)
	if err != nil {
		return nil, err
	}
	tmpl, err := template.New(wording.name).Funcs(funcs).Parse(format)
	if err != nil {
		return nil, wording.parseError(err)
	}
	return tmpl, nil
}

// schemaDiffTemplateFuncs is `sql` alone by default, and the shared set plus
// `sql` when [SchemaDiffTemplateHelpersEnvVar] is on.
//
// The variable is resolved here rather than at start-up so that a malformed
// value is refused by the command that would have used it, naming the
// template it refused to parse.
//
// The shared `json` helper is replaced by one bound to codecs: `.From` and
// `.To` carry feature data, which only the registry that produced it can
// encode. A template without feature data renders as it does with the shared
// helper.
func schemaDiffTemplateFuncs(ctx context.Context, codecs schemaext.Registry) (template.FuncMap, error) {
	enabled, err := schemaDiffTemplateHelpers.Resolve()
	if err != nil {
		return nil, err
	}
	if !enabled {
		return template.FuncMap{"sql": schemaDiffSQL}, nil
	}
	funcs := atlasTemplateFuncs()
	funcs["sql"] = schemaDiffSQL
	funcs["json"] = featureTemplateJSON(ctx, codecs)
	return funcs, nil
}

// featureTemplateJSON is the `json` template helper over schema documents: the
// argument forms of [atlasTemplateJSON], with feature data encoded through
// codecs in the desired representation both sides of a schema diff use.
func featureTemplateJSON(ctx context.Context, codecs schemaext.Registry) func(any, ...string) (string, error) {
	return func(value any, args ...string) (string, error) {
		var (
			data []byte
			err  error
		)
		switch len(args) {
		case 0:
			data, err = featurejson.Marshal(ctx, codecs, schemaext.Desired, value)
		case 1:
			data, err = featurejson.MarshalIndent(ctx, codecs, schemaext.Desired, value, "", args[0])
		default:
			data, err = featurejson.MarshalIndent(ctx, codecs, schemaext.Desired, value, args[0], args[1])
		}
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
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

// schemaChangesSQLText writes the changes as the script the `sql` template
// function returns. A change that is only comments -- a planner's note with
// nothing to run after it -- ends without a semicolon; see [sqlscript].
func schemaChangesSQLText(changes []SchemaChange) string {
	var sql strings.Builder
	for _, change := range changes {
		cmd := strings.TrimSuffix(change.Cmd, ";")
		fmt.Fprintf(&sql, "%s%s\n", cmd, sqlscript.Terminator(cmd))
	}
	return sql.String()
}

func schemaStatement(statement string) string {
	return strings.TrimSuffix(statement, ";")
}

func schemaIndentSQL(sql, indent string) string {
	trimmed := strings.TrimSuffix(sql, "\n")
	return indentSQL(trimmed, indent) + "\n"
}
