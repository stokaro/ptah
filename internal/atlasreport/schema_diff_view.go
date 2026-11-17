package atlasreport

import (
	"context"
	"text/template"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// The community binary hands a `migrate diff` template a migration plan and a
// `schema diff` template a report of two schemas, and a member that data does
// not carry fails the run when the template reads it. Measured on v1.3.0
// against PostgreSQL 18:
//
//	migrate diff --format '{{ .MarshalSQL }}'
//	Error: template: format:1:3: executing "format" at <.MarshalSQL>: can't evaluate field MarshalSQL in type *migrate.Plan
//
//	schema diff --format '{{ range .Changes }}{{ .Cmd }}{{ end }}'
//	Error: template: format:1:23: executing "format" at <.Cmd>: can't evaluate field Cmd in type schema.Change
//
// [SchemaDiff] carries more than either: Ptah's model of both sides, the
// comparison limits, the text a diff with no changes writes, and each change's
// statement. So a template a user wrote reads a view of it that has the members
// the community binary's data has and Ptah fills, and nothing else. A
// template Ptah wrote, and every template when [TemplateExtensionsEnvVar] is
// on, reads the whole report.

// migrateDiffView is what a `migrate diff --format` template reads. The
// community binary's plan has Version, Name, Reversible, Transactional,
// Changes, Delimiter and Directives; Ptah fills Changes, whose members Cmd and
// Comment are that binary's too.
type migrateDiffView struct {
	Changes []SchemaChange

	report SchemaDiff
}

// schemaDiffView is what a `schema diff --format` template reads: the two
// sides, the changes and MarshalSQL, the members the community binary's report
// has.
//
// The sides are Ptah's model, which is not that binary's: `{{ .From.Tables }}`
// renders here and fails there, where the tables are under `.From.Schemas`.
type schemaDiffView struct {
	From    *schemamodel.Database
	To      *schemamodel.Database
	Changes []schemaDiffViewChange

	report SchemaDiff
}

// schemaDiffViewChange is one change of a `schema diff` template. The community
// binary's change is a schema change, such as the creation of a table, not a
// statement, so it has no Cmd and no Comment, and neither does this one. A
// template can still count the changes and test whether there are any.
type schemaDiffViewChange struct{}

// MarshalSQL writes the changes as the `sql` helper does.
func (v schemaDiffView) MarshalSQL(indent ...string) (string, error) {
	return v.report.MarshalSQL(indent...)
}

func migrateDiffViewSQL(view migrateDiffView, indent ...string) (string, error) {
	return view.report.MarshalSQL(indent...)
}

func schemaDiffViewSQL(view schemaDiffView, indent ...string) (string, error) {
	return view.report.MarshalSQL(indent...)
}

// diffVerb is `schema diff` or `migrate diff`: the words a refusal is put in,
// and the view a user's template reads.
type diffVerb struct {
	wording templateWording
	sql     any
	view    func(SchemaDiff) any
}

var (
	schemaDiffVerb = diffVerb{
		wording: atlasSchemaVerbTemplateWording,
		sql:     schemaDiffViewSQL,
		view: func(report SchemaDiff) any {
			return schemaDiffView{
				From:    report.From,
				To:      report.To,
				Changes: make([]schemaDiffViewChange, len(report.Changes)),
				report:  report,
			}
		},
	}
	migrateDiffVerb = diffVerb{
		wording: atlasMigrateVerbTemplateWording,
		sql:     migrateDiffViewSQL,
		view: func(report SchemaDiff) any {
			return migrateDiffView{Changes: report.Changes, report: report}
		},
	}
)

// wholeReport hands a template the report itself.
func wholeReport(report SchemaDiff) any {
	return report
}

// ownDiffFormat reports whether format is one Ptah writes rather than one a
// user wrote: the default `schema diff` template and native `ptah schema
// diff`'s, which read NoChangesText.
func ownDiffFormat(format string) bool {
	return format == schemaDiffDefaultFormat || format == NativeSchemaDiffFormat
}

// diffTemplateSurface is the helpers a diff template parses with and the data
// it reads. A template Ptah wrote reads the whole report with `sql` alone. A
// user's template reads verb's view with `sql` alone, as on the community
// binary, unless [TemplateExtensionsEnvVar] is on, when it reads the whole
// report with the shared helper set.
//
// The shared `json` helper is replaced by one bound to codecs: `.From` and
// `.To` carry feature data, which only the registry that produced it can
// encode. A template without feature data renders as it does with the shared
// helper.
func diffTemplateSurface(ctx context.Context, verb diffVerb, format string, codecs schemaext.Registry) (template.FuncMap, func(SchemaDiff) any, error) {
	if ownDiffFormat(format) {
		return template.FuncMap{"sql": schemaDiffSQL}, wholeReport, nil
	}
	extended, err := ResolveTemplateExtensions()
	if err != nil {
		return nil, nil, err
	}
	if !extended {
		return template.FuncMap{"sql": verb.sql}, verb.view, nil
	}
	funcs := atlasTemplateFuncs()
	funcs["sql"] = schemaDiffSQL
	funcs["json"] = featureTemplateJSON(ctx, codecs)
	return funcs, wholeReport, nil
}
