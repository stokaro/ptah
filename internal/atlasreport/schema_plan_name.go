package atlasreport

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// SchemaPlanName is the template payload exposed to `atlas schema plan
// --name-format`. The field names follow the only spelling the licensed Atlas
// binary documents for this template — its help text advertises the example
// `plan_{{ slice .ToHash 0 8 }}` — so a Pro pipeline's name template keeps
// working. FromHash is the symmetric counterpart; Atlas documents no example
// using it.
//
// The values are Ptah's own `sha256:<hex>` schema fingerprints, byte-identical
// to the `from` and `to` fields of the plan file the same run writes. Atlas
// computes base64 hashes over a different schema representation, so a template
// that slices a fixed prefix produces a different-looking name here than it
// does under the official binary. That divergence is inherent to the
// fingerprints, not to the template: Atlas's hashes have no local recipe.
type SchemaPlanName struct {
	FromHash string
	ToHash   string
}

// ValidateSchemaPlanNameTemplate reports whether format parses as a plan-name
// template. Commands call it before connecting to a database so a malformed
// template fails with no side effects.
func ValidateSchemaPlanNameTemplate(format string) error {
	_, err := newSchemaPlanNameTemplate(format)
	return err
}

// RenderSchemaPlanName executes the plan-name template against data and
// returns the rendered name with surrounding whitespace removed. Trimming is
// deliberate: templates are commonly written across several lines, and the
// name becomes a file name.
func RenderSchemaPlanName(format string, data SchemaPlanName) (string, error) {
	tmpl, err := newSchemaPlanNameTemplate(format)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return "", fmt.Errorf("execute --name-format template: %w", err)
	}
	return strings.TrimSpace(out.String()), nil
}

// newSchemaPlanNameTemplate parses a plan-name template with the same helper
// set every other Atlas Go-template surface in this package exposes.
//
// Sharing the helpers is the portable direction. #951's premise is that an
// Atlas pipeline keeps working here: a template that runs under the official
// binary and fails here breaks that promise, while a template that runs here
// and fails there only inconveniences someone migrating away. Whether Atlas
// builds *this* template from a bare environment is unmeasured, so the
// permissive choice is the safe one.
//
// No "missingkey" option is set. The payload is a struct, and a struct field
// lookup that misses is already an execution error whatever the option says —
// a mutation removing the option left the suite green, which is what proved it
// inert. A map payload would need the option back.
func newSchemaPlanNameTemplate(format string) (*template.Template, error) {
	tmpl, err := template.New("atlas-schema-plan-name-format").Funcs(atlasTemplateFuncs()).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --name-format template: %w", err)
	}
	return tmpl, nil
}
