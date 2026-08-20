package atlasreport

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/template"
)

// SchemaPlan is the `schema plan --format` template payload.
//
// Its field names are Ptah's, and that is a decision rather than an omission.
// The other eight compat verbs that answer `--format` shape their payload
// after what the pinned community binary prints, because that binary executes
// those verbs and the field names are therefore measurable. It aborts
// `schema plan`, publishing help text for the flag and nothing a template could
// be written against, so there is no shape here to match -- only one to choose
// and write down (stokaro/ptah#1700).
//
// What it is chosen to match instead is the sibling verb: [SchemaDiff] exposes
// its statements as `.Changes` with a `.Cmd` each, and a template written for
// `schema diff` reads a plan unchanged. The fields the plan has and a diff does
// not -- its name, its fingerprints, its per-statement severity -- are the
// plan file's own fields, under the plan file's own names.
type SchemaPlan struct {
	// Name is the plan name, which is also the default plan file's base name.
	Name string
	// Dialect is the target dialect the statements were rendered for. It is
	// empty for a plan read back from the Atlas `.plan.hcl` format, which has
	// no field for it.
	Dialect string
	// From and To are the plan's source and desired-state fingerprints, the
	// same two values the plan file records.
	From string
	To   string
	// Exclude are the exclusion patterns the plan was computed with.
	Exclude []string `json:",omitempty"`
	// Destructive reports whether any statement was classified destructive.
	Destructive bool
	// Changes are the ordered planned statements.
	Changes []SchemaPlanChange
	// MigrationBody is the plan file's `migration` attribute exactly as it is
	// written to disk. It differs from joining `.Changes` only in separator
	// whitespace, and it is the field to read when the template must reproduce
	// the artifact rather than describe it. Any directives the plan carries
	// are in here, in the first statement's header, because that is where they
	// are written and where the readers that honor them look.
	MigrationBody string
}

// SchemaPlanChange is one planned statement and the safety classification the
// plan recorded for it.
type SchemaPlanChange struct {
	Cmd      string
	Severity string
	Reason   string
}

// SchemaPlanOptions carries the plan-file fields [NewSchemaPlan] renders.
//
// It takes primitives rather than a plan file so this package keeps reporting
// and stays out of the schema-planning dependency graph, the same way
// [NewSchemaDiff] takes statements.
type SchemaPlanOptions struct {
	Name          string
	Dialect       string
	From          string
	To            string
	Exclude       []string
	Destructive   bool
	Statements    []SchemaPlanChange
	MigrationBody string
}

// NewSchemaPlan builds the template payload.
func NewSchemaPlan(opts SchemaPlanOptions) SchemaPlan {
	changes := opts.Statements
	if changes == nil {
		changes = make([]SchemaPlanChange, 0)
	}
	return SchemaPlan{
		Name:          opts.Name,
		Dialect:       opts.Dialect,
		From:          opts.From,
		To:            opts.To,
		Exclude:       opts.Exclude,
		Destructive:   opts.Destructive,
		Changes:       changes,
		MigrationBody: opts.MigrationBody,
	}
}

// WriteSchemaPlan renders the plan through the operator's `--format` template.
func WriteSchemaPlan(w io.Writer, format string, result SchemaPlan) error {
	tmpl, err := newSchemaPlanTemplate(format)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, result); err != nil {
		return fmt.Errorf("execute --format template: %w", err)
	}
	_, err = w.Write(out.Bytes())
	return err
}

// ValidateSchemaPlanTemplate parses the template without rendering it, so a
// malformed one is refused before any database is opened.
func ValidateSchemaPlanTemplate(format string) error {
	_, err := newSchemaPlanTemplate(format)
	return err
}

// newSchemaPlanTemplate registers the full shared helper set plus `sql`.
//
// `schema diff` gates the shared set behind an environment variable because
// the pinned community binary offers `sql` alone there, and rendering more
// would let ptah-compat succeed on a template that binary refuses. No such
// reading exists here: that binary runs no `schema plan` at all, so every
// invocation of this verb is already outside its behavior, and withholding
// helpers would cost a capability without buying compatibility with anything.
func newSchemaPlanTemplate(format string) (*template.Template, error) {
	funcs := atlasTemplateFuncs()
	funcs["sql"] = schemaPlanSQL
	tmpl, err := template.New("atlas-schema-plan-format").Funcs(funcs).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
}

// MarshalSQL renders the planned statements as one executable script, with the
// same optional indent argument `schema diff`'s `sql` takes.
func (r SchemaPlan) MarshalSQL(indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	var sql strings.Builder
	for _, change := range r.Changes {
		fmt.Fprintf(&sql, "%s;\n", strings.TrimSuffix(change.Cmd, ";"))
	}
	text := sql.String()
	if len(indent) == 0 || indent[0] == "" || text == "" {
		return text, nil
	}
	return schemaIndentSQL(text, indent[0]), nil
}

func schemaPlanSQL(result SchemaPlan, indent ...string) (string, error) {
	return result.MarshalSQL(indent...)
}
