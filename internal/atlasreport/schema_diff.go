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
{{ .NoChangesText -}}
{{ end -}}
`

const migrateDiffDefaultFormat = `{{ sql . "  " }}`

type SchemaDiff struct {
	From    *schemamodel.Database
	To      *schemamodel.Database
	Changes []SchemaDiffChange
	Limits  []SchemaDiffLimit

	// dialect is the dialect the statements were planned for, which the `sql`
	// helper reads them as when it indents them. It is unexported because
	// Atlas has no such template field, and a field only Ptah offers would let
	// a template succeed here that the community binary refuses.
	dialect string
	// commentsAsWritten is [SchemaDiffOptions.CommentsAsWritten].
	commentsAsWritten bool
	// foreignLayout is [SchemaDiffOptions.ForeignLayout].
	foreignLayout bool
}

// SchemaDiffLimit names a part of one database source that a diff could not
// establish. A limit is informational; it does not produce a SQL statement.
type SchemaDiffLimit struct {
	Side   string `json:"side"`
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

// NoChangesText avoids claiming full agreement when the read covered only a
// visible subset or could not read one kind.
func (d SchemaDiff) NoChangesText() string {
	if len(d.Limits) == 0 {
		return "Schemas are synced, no changes to be made.\n"
	}
	var text strings.Builder
	text.WriteString("No changes found in the visible scope. Comparison limits:\n")
	for _, limit := range d.Limits {
		fmt.Fprintf(&text, "- %s %s: %s\n", limit.Side, limit.Kind, limit.Reason)
	}
	return text.String()
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
	// Limits are the parts of a database source the diff could not establish.
	Limits []SchemaDiffLimit
	// CommentsAsWritten makes the `sql` helper write each change's comment
	// as the change carries it. Without it the helper writes the comment's
	// first letter in upper case, as the community binary's helper does:
	// `-- Create "users" table`. A migration file of a layout another tool
	// reads, such as golang-migrate, is written by that binary through a
	// template of its own, which writes the comment unchanged.
	CommentsAsWritten bool
	// ForeignLayout marks a migration file of a layout another tool reads,
	// such as golang-migrate or goose. The `sql` helper then writes no batch
	// separator: SQL Server's GO is a command of sqlcmd and of Ptah's own
	// splitter, not of the server, and such a tool may send it to the server
	// as written. Without it the helper ends every SQL Server statement's
	// batch with GO; see sqlscript.BatchEnd.
	ForeignLayout bool
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
		Limits:            opts.Limits,
		dialect:           opts.Dialect,
		commentsAsWritten: opts.CommentsAsWritten,
		foreignLayout:     opts.ForeignLayout,
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

func (d SchemaDiff) MarshalSQL(indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	spell := capitalized
	if d.commentsAsWritten {
		spell = asWritten
	}
	end := statementEnd(d.dialect)
	if d.foreignLayout {
		end = sqlscript.Terminator
	}
	sql := schemaChangesSQLText(d.Changes, spell, end)
	if len(indent) == 0 || indent[0] == "" || sql == "" {
		return sql, nil
	}
	return schemaIndentSQL(d.dialect, sql, indent[0]), nil
}

func schemaDiffSQL(result SchemaDiff, indent ...string) (string, error) {
	return result.MarshalSQL(indent...)
}

// schemaChangesSQLText writes the changes as the script the `sql` template
// function returns: each change's comment on a line of its own, spelled by
// spell, then the statement. A change that is only comments -- a planner's
// note with nothing to run after it -- ends without a semicolon; see
// [sqlscript]. What follows each statement is end's answer.
func schemaChangesSQLText(changes []SchemaChange, spell, end func(string) string) string {
	var sql strings.Builder
	for _, change := range changes {
		cmd := strings.TrimSuffix(change.Cmd, ";")
		if sqlscript.CommentOnly(cmd) {
			// A note ends its own line; the script adds the line break.
			cmd = strings.TrimRight(cmd, "\n")
		}
		named := cmd
		if change.Comment != "" {
			named = afterNotes(cmd, "-- "+spell(change.Comment)+"\n")
		}
		fmt.Fprintf(&sql, "%s%s\n", named, end(cmd))
	}
	return sql.String()
}

// statementEnd is [sqlscript.StatementEnd] for dialect: the terminator, and
// for a dialect whose scripts have batches, the end of the statement's batch.
func statementEnd(dialect string) func(string) string {
	return func(statement string) string {
		return sqlscript.StatementEnd(dialect, statement)
	}
}

// ChangeLine is the comment line that names a change above its statement, as the
// `sql` helper writes it: first letter in upper case. It is empty for a change
// nothing names.
func ChangeLine(comment string) string {
	if comment == "" {
		return ""
	}
	return "-- " + capitalized(comment) + "\n"
}

// NameStatement is statement with [ChangeLine] for comment directly above its
// SQL. A statement can open with notes a plan writes beside it, such as the
// changes a diff policy left out or an object the target skipped; they stay
// above the line, so the line names the statement that follows it rather than
// the notes.
func NameStatement(comment, statement string) string {
	return afterNotes(statement, ChangeLine(comment))
}

// afterNotes is statement with line inserted after the comment lines it opens
// with.
func afterNotes(statement, line string) string {
	if line == "" {
		return statement
	}
	at := 0
	for rest := range strings.Lines(statement) {
		if !strings.HasPrefix(strings.TrimSpace(rest), "--") || !strings.HasSuffix(rest, "\n") {
			break
		}
		at += len(rest)
	}
	return statement[:at] + line + statement[at:]
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
