package atlasreport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"

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

	// dialect is the dialect the statements were planned for, which the `sql`
	// helper reads them as when it indents them. It is unexported because
	// Atlas has no such template field, and a field only Ptah offers would let
	// a template succeed here that the community binary refuses.
	dialect string
	// commentsAsWritten is [SchemaDiffOptions.CommentsAsWritten].
	commentsAsWritten bool
}

// SchemaChange is one planned statement. Its fields are those of the community
// binary's template data that Ptah fills, under the same names, so a
// `--format` template that reads them reads them on both binaries.
type SchemaChange struct {
	// Cmd is the statement, without its terminating semicolon.
	Cmd string
	// Comment names the change Cmd makes, the way the community binary names
	// it: `create "users" table`. It is empty for a change nothing names and
	// for every change of a `schema diff`, and the `json` helper leaves an
	// empty one out.
	Comment string `json:",omitempty"`
}

type SchemaDiffChange = SchemaChange

// SchemaDiffOptions describes the diff the report is rendered for.
type SchemaDiffOptions struct {
	// Dialect is the dialect the statements were planned for. The `sql`
	// helper reads the statements as that dialect when it indents them, so a
	// newline inside one of its string literals or quoted identifiers keeps
	// its place.
	Dialect string
	// From and To are the two compared schemas. A migration directory diff
	// renders neither.
	From *schemamodel.Database
	To   *schemamodel.Database
	// Statements is the ordered plan.
	Statements []string
	// Changes is the ordered plan with the comment that names each change.
	// It is read in place of Statements when it is set.
	Changes []SchemaChange
	// CommentsAsWritten makes the `sql` helper write each change's comment
	// as the change carries it. Without it the helper writes the comment's
	// first letter in upper case, as the community binary's helper does:
	// `-- Create "users" table`. A migration file of a layout another tool
	// reads, such as golang-migrate, is written by that binary through a
	// template of its own, which writes the comment unchanged.
	CommentsAsWritten bool
}

// NewSchemaDiff builds the template payload of `schema diff` and `migrate
// diff`.
func NewSchemaDiff(opts SchemaDiffOptions) SchemaDiff {
	changes := schemaChanges(opts.Statements)
	if opts.Changes != nil {
		changes = make([]SchemaChange, 0, len(opts.Changes))
		for _, change := range opts.Changes {
			changes = append(changes, SchemaChange{Cmd: schemaStatement(change.Cmd), Comment: change.Comment})
		}
	}
	return SchemaDiff{
		From:              opts.From,
		To:                opts.To,
		Changes:           changes,
		dialect:           opts.Dialect,
		commentsAsWritten: opts.CommentsAsWritten,
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
	marshal := func(value any) ([]byte, error) {
		return featurejson.Marshal(ctx, codecs, schemaext.Desired, value)
	}
	marshalIndent := func(value any, prefix, indent string) ([]byte, error) {
		return featurejson.MarshalIndent(ctx, codecs, schemaext.Desired, value, prefix, indent)
	}
	return func(value any, args ...string) (string, error) {
		return templateJSON(value, args, marshal, marshalIndent)
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
	spell := capitalized
	if r.commentsAsWritten {
		spell = asWritten
	}
	sql := schemaChangesSQLText(r.Changes, spell)
	if len(indent) == 0 || indent[0] == "" || sql == "" {
		return sql, nil
	}
	return schemaIndentSQL(r.dialect, sql, indent[0]), nil
}

func schemaDiffSQL(result SchemaDiff, indent ...string) (string, error) {
	return result.MarshalSQL(indent...)
}

// schemaChangesSQLText writes the changes as the script the `sql` template
// function returns: each change's comment on a line of its own, spelled by
// spell, then the statement. A change that is only comments -- a planner's
// note with nothing to run after it -- ends without a semicolon; see
// [sqlscript].
func schemaChangesSQLText(changes []SchemaChange, spell func(string) string) string {
	var sql strings.Builder
	for _, change := range changes {
		if change.Comment != "" {
			fmt.Fprintf(&sql, "-- %s\n", spell(change.Comment))
		}
		cmd := strings.TrimSuffix(change.Cmd, ";")
		if sqlscript.CommentOnly(cmd) {
			// A note ends its own line; the script adds the line break.
			cmd = strings.TrimRight(cmd, "\n")
		}
		fmt.Fprintf(&sql, "%s%s\n", cmd, sqlscript.Terminator(cmd))
	}
	return sql.String()
}

// asWritten is text as it is.
func asWritten(text string) string {
	return text
}

// capitalized is text with its first letter in upper case.
func capitalized(text string) string {
	first, size := utf8.DecodeRuneInString(text)
	if first == utf8.RuneError {
		return text
	}
	return string(unicode.ToUpper(first)) + text[size:]
}

func schemaStatement(statement string) string {
	return strings.TrimSuffix(statement, ";")
}

func schemaIndentSQL(dialect, sql, indent string) string {
	trimmed := strings.TrimSuffix(sql, "\n")
	return indentSQL(dialect, trimmed, indent) + "\n"
}
