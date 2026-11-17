package atlasreport

import (
	"text/template"

	"ptah.run/internal/envbool"
)

// TemplateExtensionsEnvVar opens Ptah's own `--format` template surface on the
// compat verbs: the helpers and report members Ptah offers where the pinned
// community binary has none.
//
// The default is the community binary's surface, because a template that
// renders here and fails there is a mistake that passes in one pipeline and
// breaks the next -- the first half of the compatibility policy. Measured on
// v1.3.0 against PostgreSQL 18, by parsing `{{ if false }}{{ NAME }}{{ end }}`
// for each helper name, the helpers each verb defines are:
//
//	migrate diff, schema diff  sql
//	schema apply, migrate apply  add cyan green indent_ln json json_merge red redBgWhiteFg upper yellow
//	migrate lint     add cyan green join json lower maxWidth red redBgWhiteFg repeat sub underline yellow
//	migrate status   cyan default green json json_merge red redBgWhiteFg yellow
//	schema inspect   base64url hcl json mermaid split sql write
//
// and a name outside a verb's list fails at parse time with
// `function "NAME" not defined`. The second half of the policy says
// compatibility must not withhold a capability Ptah has, so a true value
// registers the rest of [atlasTemplateFuncs] on every verb, `sql` on `schema
// apply`, `dbml` on `schema inspect`, and hands a `migrate diff` or `schema
// diff` template the whole [SchemaDiff]. It is a variable rather than a flag,
// leaving the command and flag inventory identical, and it is Gated because a
// true value adds readings the pinned binary does not have.
const TemplateExtensionsEnvVar = "PTAH_ATLAS_TEMPLATE_EXTENSIONS"

var templateExtensions = envbool.New(TemplateExtensionsEnvVar, false, envbool.Gated)

// ResolveTemplateExtensions resolves [TemplateExtensionsEnvVar], refusing a
// value that does not parse as a boolean. A verb whose template surface the
// variable changes resolves it on every run, so a malformed value fails the
// run that has no --format as well as the one that has.
func ResolveTemplateExtensions() (bool, error) {
	return templateExtensions.Resolve()
}

// The shared helpers the community binary does not define on a verb that has
// the rest of [atlasTemplateFuncs]; see [TemplateExtensionsEnvVar].
var (
	migrateLintWithheldHelpers   = []string{"indent_ln", "json_merge", "upper"}
	migrateStatusWithheldHelpers = []string{"add", "indent_ln", "upper"}
)

// verbTemplateFuncs is [atlasTemplateFuncs] without the helpers withheld names,
// or all of it when [TemplateExtensionsEnvVar] is on.
func verbTemplateFuncs(withheld []string) (template.FuncMap, error) {
	funcs := atlasTemplateFuncs()
	if len(withheld) == 0 {
		return funcs, nil
	}
	extended, err := ResolveTemplateExtensions()
	if err != nil {
		return nil, err
	}
	if extended {
		return funcs, nil
	}
	for _, name := range withheld {
		delete(funcs, name)
	}
	return funcs, nil
}
