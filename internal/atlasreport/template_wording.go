package atlasreport

import "fmt"

// templateWording is how one verb's --format refusals read: the name the
// template is parsed under, which Go's text/template quotes in every error,
// and the sentence a parse or execution failure is wrapped in.
//
// The pinned community binary v1.3.0 names every template "format". A parse
// failure reads `parse log format: …` on `schema inspect`, `schema diff` and
// `schema apply`, and `parse format: …` on `migrate lint`, `migrate status`,
// `migrate apply` and `migrate diff`. An execution failure is the template's
// own error with nothing in front of it, except on `migrate apply`, where it
// reads `execute log template: …`. Measured on 2026-09-26 on PostgreSQL 18
// (stokaro/ptah#3689). The only part left different is the Go type an
// execution error names, which is this package's report type there and the
// community binary's own here.
//
// The verbs the community binary does not offer -- `schema clean --dry-run`,
// `migrate down`, the `schema plan` family -- have no answer to match and keep
// [ptahTemplateWording], which names the flag.
type templateWording struct {
	name          string
	parsePrefix   string
	executePrefix string
}

var (
	// atlasSchemaVerbTemplateWording is `schema inspect`, `schema diff` and
	// `schema apply`.
	atlasSchemaVerbTemplateWording = templateWording{name: "format", parsePrefix: "parse log format"}
	// atlasMigrateVerbTemplateWording is `migrate lint`, `migrate status` and
	// `migrate diff`.
	atlasMigrateVerbTemplateWording = templateWording{name: "format", parsePrefix: "parse format"}
	// atlasMigrateApplyTemplateWording is `migrate apply`.
	atlasMigrateApplyTemplateWording = templateWording{
		name:          "format",
		parsePrefix:   "parse format",
		executePrefix: "execute log template",
	}
)

// ptahTemplateWording is the wording of a verb with no community answer to
// match, under the template name it has always been parsed as.
func ptahTemplateWording(name string) templateWording {
	return templateWording{
		name:          name,
		parsePrefix:   "parse --format template",
		executePrefix: "execute --format template",
	}
}

func (w templateWording) parseError(err error) error {
	return fmt.Errorf("%s: %w", w.parsePrefix, err)
}

func (w templateWording) executeError(err error) error {
	if w.executePrefix == "" {
		return err
	}
	return fmt.Errorf("%s: %w", w.executePrefix, err)
}
