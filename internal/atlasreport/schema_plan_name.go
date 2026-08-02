package atlasreport

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"text/template"

	digest "github.com/opencontainers/go-digest"
)

// SchemaPlanName is the template payload exposed to `atlas schema plan
// --name-format`. The field names and representation were measured against
// Atlas: .FromHash and .ToHash are the untagged
// standard-Base64 digests written to the plan block's from and to attributes.
// This makes Atlas's documented `plan_{{ slice .ToHash 0 8 }}` example carry
// 48 bits of fingerprint entropy. Standard Base64 can contain `/`, so callers
// that use the rendered value as a default file name must reject separators or
// require a separately supplied output path.
//
// Ptah computes hashes over its independent schema representation. The digest
// values therefore differ from Atlas's, but NewSchemaPlanName exposes Ptah's
// digest bytes in the same untagged Base64 representation.
type SchemaPlanName struct {
	FromHash string
	ToHash   string
}

// NewSchemaPlanName converts Ptah's tagged hexadecimal schema fingerprints to
// the untagged Base64 hash representation exposed by Atlas name templates.
func NewSchemaPlanName(fromFingerprint, toFingerprint string) (SchemaPlanName, error) {
	fromHash, err := schemaPlanTemplateHash("from", fromFingerprint)
	if err != nil {
		return SchemaPlanName{}, err
	}
	toHash, err := schemaPlanTemplateHash("to", toFingerprint)
	if err != nil {
		return SchemaPlanName{}, err
	}
	return SchemaPlanName{FromHash: fromHash, ToHash: toHash}, nil
}

func schemaPlanTemplateHash(field, fingerprint string) (string, error) {
	parsed, err := digest.Parse(fingerprint)
	if err != nil {
		return "", fmt.Errorf("parse %s schema fingerprint for --name-format: %w", field, err)
	}
	raw, err := hex.DecodeString(parsed.Encoded())
	if err != nil {
		return "", fmt.Errorf("decode %s schema fingerprint for --name-format: %w", field, err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
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
