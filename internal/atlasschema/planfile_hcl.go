package atlasschema

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	digest "github.com/opencontainers/go-digest"
	"github.com/zclconf/go-cty/cty"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/migration/safety"
)

// PlanFormat identifies the on-disk encoding of a local plan file.
type PlanFormat string

const (
	// PlanFormatJSON is Ptah's native plan-file encoding: the format_version-1
	// JSON document with sha256 fingerprints, dialect, exclude patterns, and
	// per-statement safety classification.
	PlanFormatJSON PlanFormat = "json"

	// PlanFormatHCL is the Atlas `.plan.hcl` encoding, measured: one
	// `plan "<name>"` block with `from`, `to`, and
	// `migration` attributes. It carries no dialect, exclude patterns, or
	// per-statement metadata; Ptah re-derives those at read time.
	PlanFormatHCL PlanFormat = "hcl"
)

// PlanFileSuffixHCL is the Atlas plan-file name suffix. The Atlas-compatible
// command tree saves and defaults to this format; the JSON format stays the
// native `ptah` plan-file contract.
const PlanFileSuffixHCL = ".plan.hcl"

// hclPlanHeredocDelimiter is the heredoc delimiter Atlas uses for the
// migration attribute; Ptah mirrors it byte-for-byte.
const hclPlanHeredocDelimiter = "SQL"

// hclPlanBodyIndent is the two-space indentation of the plan block body and
// the `<<-SQL` heredoc content in Atlas-written plan files.
const hclPlanBodyIndent = "  "

// utf8BOM is stripped before format detection so a BOM-prefixed JSON plan is
// not misread as an HCL document.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// DetectPlanFormat classifies plan-file contents. A JSON document starts with
// '{' (after an optional UTF-8 BOM and leading whitespace); everything else is
// treated as the Atlas HCL plan format, whose top level is always a `plan`
// block.
func DetectPlanFormat(contents []byte) PlanFormat {
	contents = bytes.TrimPrefix(contents, utf8BOM)
	for _, b := range contents {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return PlanFormatJSON
		default:
			return PlanFormatHCL
		}
	}
	return PlanFormatHCL
}

// ReadPlanDocument loads and validates a local plan file in either supported
// encoding, reporting which format it was. The Atlas-compatible apply path
// uses it so Atlas-authored `.plan.hcl` files and Ptah's native `.plan.json`
// files are both readable; the native command tree keeps [ReadPlanFile].
func ReadPlanDocument(path string) (PlanFile, PlanFormat, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return PlanFile{}, "", fmt.Errorf("read plan file: %w", err)
	}
	format := DetectPlanFormat(contents)
	switch format {
	case PlanFormatJSON:
		plan, err := decodePlanJSON(contents, path)
		return plan, format, err
	default:
		plan, err := decodePlanHCL(contents, path)
		return plan, format, err
	}
}

// IsNativeFingerprint reports whether fingerprint is a Ptah-computed schema
// fingerprint (`sha256:<hex>`, see [SchemaFingerprint]) that apply-time
// verification can recompute. Atlas plan files carry base64 hashes with no
// algorithm prefix; those are foreign — Ptah has no local recipe for them and
// verifies such plans semantically against the --to desired state instead.
func IsNativeFingerprint(fingerprint string) bool {
	parsed, err := digest.Parse(fingerprint)
	return err == nil && parsed.Algorithm() == digest.SHA256
}

// TimestampPlanName returns the Atlas-style timestamp plan name
// (YYYYMMDDHHMMSS in UTC) used as the default name for `.plan.hcl` plans,
// mirroring the names Atlas writes.
func TimestampPlanName(now time.Time) string {
	return now.UTC().Format("20060102150405")
}

// decodePlanHCL parses an Atlas-format plan document into the plan structure.
// The parser is strict, mirroring the JSON reader's DisallowUnknownFields:
// exactly one `plan` block with one label, exactly the `from`, `to`, and
// `migration` attributes, nothing else. Per-statement severity, the
// plan-level destructive marker, and the statement list are re-derived from
// the migration SQL, because the Atlas format does not record them; the
// dialect and format_version fields stay empty for the same reason.
//
// The derived severity and Destructive values are ADVISORY, not authoritative.
// The Atlas format records no dialect, and reading happens before any database
// connection exists, so the split here is dialect-blind and can differ — in
// statement count, for MySQL-family backslash SQL — from the dialect-aware
// split the apply path executes. Nothing on the compat apply path consumes
// them, deliberately. Any future gate that acts on severity (an approval
// prompt, a destructive-change refusal) must recompute it from the
// dialect-aware statement list that [ApplyStatements] runs, never from these
// fields.
func decodePlanHCL(contents []byte, path string) (PlanFile, error) {
	file, diags := hclsyntax.ParseConfig(contents, path, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return PlanFile{}, fmt.Errorf("parse plan file %s: %s", path, diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return PlanFile{}, fmt.Errorf("parse plan file %s: unexpected HCL body type %T", path, file.Body)
	}
	block, err := singlePlanBlock(body)
	if err != nil {
		return PlanFile{}, fmt.Errorf("invalid plan file %s: %w", path, err)
	}

	attributes, err := planBlockAttributes(block.Body)
	if err != nil {
		return PlanFile{}, fmt.Errorf("invalid plan file %s: %w", path, err)
	}
	plan := PlanFile{
		Name:            block.Labels[0],
		FromFingerprint: attributes["from"],
		ToFingerprint:   attributes["to"],
		Statements:      planStatementsFromSQL(sqlutil.SplitSQLStatements(attributes["migration"])),
	}
	for _, statement := range plan.Statements {
		plan.Destructive = plan.Destructive || statement.Severity == safety.Destructive
	}
	if err := validatePlanHCL(plan); err != nil {
		return PlanFile{}, fmt.Errorf("invalid plan file %s: %w", path, err)
	}
	return plan, nil
}

// CheckPlanFormatSupported reports whether plan can be written in format
// without losing information that the format has no field for. It depends only
// on the plan's metadata, never on its statement text, so a command can call it
// before doing work it would have to throw away — opening an editor, for
// instance — and [MarshalPlanFileHCL] calls it too, so the writer stays the
// authority and the two cannot drift.
func CheckPlanFormatSupported(plan PlanFile, format PlanFormat) error {
	if format == PlanFormatHCL && len(plan.Exclude) > 0 {
		return errors.New(
			"the Atlas .plan.hcl format has no field for exclude patterns, so a plan computed with --exclude " +
				"cannot be represented faithfully; write the native JSON plan format (--output <name>" + PlanFileSuffix + ") " +
				"or drop --exclude")
	}
	return nil
}

// MarshalPlanFileHCL renders the plan in the Atlas `.plan.hcl` shape measured
// from Atlas: one `plan "<name>"` block with aligned `from`,
// `to`, and `migration` attributes and a `<<-SQL` heredoc holding the ordered
// statements. The from/to values are Ptah's own sha256 fingerprints — the
// file parses in Atlas's reader, but Atlas verifies its own
// base64 hashes, which Ptah cannot compute locally. The output is
// deterministic for identical plan contents.
func MarshalPlanFileHCL(plan PlanFile) ([]byte, error) {
	if err := CheckPlanFormatSupported(plan, PlanFormatHCL); err != nil {
		return nil, err
	}
	if err := validateHCLString("plan name", plan.Name); err != nil {
		return nil, err
	}
	if err := validateHCLString("plan from fingerprint", plan.FromFingerprint); err != nil {
		return nil, err
	}
	if err := validateHCLString("plan to fingerprint", plan.ToFingerprint); err != nil {
		return nil, err
	}
	if err := validatePlanHCL(plan); err != nil {
		return nil, fmt.Errorf("render HCL plan file: %w", err)
	}

	var migration strings.Builder
	for _, statement := range plan.Statements {
		text := strings.TrimSuffix(strings.TrimSpace(statement.SQL), ";") + ";"
		for line := range strings.Lines(text + "\n") {
			// Only the line separator is removed here: a carriage return must
			// still reach the validator, which refuses it instead of letting
			// the writer silently rewrite CRLF content to LF.
			line = strings.TrimSuffix(line, "\n")
			if err := validateHCLHeredocLine(line); err != nil {
				return nil, err
			}
			// Empty lines stay empty instead of becoming indentation: HCL
			// excludes blank lines when computing how much indentation `<<-`
			// strips, so an unindented blank line round-trips exactly while
			// an indented one would come back as whitespace the operator
			// never wrote.
			if line != "" {
				migration.WriteString(hclPlanBodyIndent)
				migration.WriteString(line)
			}
			migration.WriteString("\n")
		}
	}

	var out strings.Builder
	fmt.Fprintf(&out, "plan %q {\n", plan.Name)
	fmt.Fprintf(&out, "%sfrom      = %q\n", hclPlanBodyIndent, plan.FromFingerprint)
	fmt.Fprintf(&out, "%sto        = %q\n", hclPlanBodyIndent, plan.ToFingerprint)
	fmt.Fprintf(&out, "%smigration = <<-%s\n", hclPlanBodyIndent, hclPlanHeredocDelimiter)
	out.WriteString(migration.String())
	fmt.Fprintf(&out, "%s%s\n}\n", hclPlanBodyIndent, hclPlanHeredocDelimiter)
	return []byte(out.String()), nil
}

// PlanFileSuffixFor returns the conventional plan-file suffix for format.
func PlanFileSuffixFor(format PlanFormat) string {
	if format == PlanFormatHCL {
		return PlanFileSuffixHCL
	}
	return PlanFileSuffix
}

// MarshalPlanFileAs renders the plan document in the requested format.
func MarshalPlanFileAs(plan PlanFile, format PlanFormat) ([]byte, error) {
	if format == PlanFormatHCL {
		return MarshalPlanFileHCL(plan)
	}
	return MarshalPlanFile(plan)
}

// planStatementsFromSQL assigns advisory safety metadata while decoding an
// Atlas plan. Atlas plan files do not record a dialect, so correctness-sensitive
// callers must reclassify the SQL after resolving the target dialect.
func planStatementsFromSQL(raw []string) []PlanStatement {
	statements, _ := classifyPlanStatements(raw, "")
	return statements
}

func singlePlanBlock(body *hclsyntax.Body) (*hclsyntax.Block, error) {
	if len(body.Attributes) > 0 {
		return nil, fmt.Errorf("unexpected top-level attribute %q; an Atlas plan file contains exactly one plan block", firstAttributeName(body.Attributes))
	}
	if len(body.Blocks) != 1 {
		return nil, fmt.Errorf("expected exactly one plan block, found %d blocks", len(body.Blocks))
	}
	block := body.Blocks[0]
	if block.Type != "plan" {
		return nil, fmt.Errorf("unexpected block %q; an Atlas plan file contains exactly one plan block", block.Type)
	}
	if len(block.Labels) != 1 {
		return nil, fmt.Errorf("plan block requires exactly one name label, found %d", len(block.Labels))
	}
	return block, nil
}

func planBlockAttributes(body *hclsyntax.Body) (map[string]string, error) {
	if len(body.Blocks) > 0 {
		return nil, fmt.Errorf("unexpected nested %q block inside the plan block", body.Blocks[0].Type)
	}
	required := []string{"from", "to", "migration"}
	for _, name := range slices.Sorted(maps.Keys(body.Attributes)) {
		if !slices.Contains(required, name) {
			return nil, fmt.Errorf("unknown plan attribute %q", name)
		}
	}
	attributes := make(map[string]string, len(required))
	for name, attribute := range body.Attributes {
		value, diags := attribute.Expr.Value(nil)
		if diags.HasErrors() {
			return nil, fmt.Errorf("evaluate plan attribute %q: %s", name, diags.Error())
		}
		if value.Type() != cty.String {
			return nil, fmt.Errorf("plan attribute %q must be a string", name)
		}
		attributes[name] = value.AsString()
	}
	for _, name := range required {
		if _, ok := attributes[name]; !ok {
			return nil, fmt.Errorf("plan attribute %q is required", name)
		}
	}
	return attributes, nil
}

func validatePlanHCL(plan PlanFile) error {
	if strings.TrimSpace(plan.Name) == "" {
		return errors.New("plan name is required")
	}
	if strings.TrimSpace(plan.FromFingerprint) == "" {
		return errors.New("plan from fingerprint is required")
	}
	if strings.TrimSpace(plan.ToFingerprint) == "" {
		return errors.New("plan to fingerprint is required")
	}
	if len(plan.Statements) == 0 {
		return errors.New("plan migration contains no statements")
	}
	for i, statement := range plan.Statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("plan statement %d has empty sql", i+1)
		}
	}
	return nil
}

// validateHCLString rejects values that would need escaping inside a quoted
// HCL string. Every value Ptah writes (timestamp or fingerprint-derived
// names, sha256 fingerprints) is plain ASCII, so hitting this means the
// caller passed something the Atlas plan shape cannot carry verbatim.
func validateHCLString(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8 and cannot be written into an Atlas .plan.hcl document", field)
	}
	if strings.ContainsAny(value, "\"\\\n\r\t") || strings.Contains(value, "${") || strings.Contains(value, "%{") {
		return fmt.Errorf("%s %q contains characters that cannot be written verbatim into an Atlas .plan.hcl quoted string", field, value)
	}
	return nil
}

// validateHCLHeredocLine rejects migration content that the `<<-SQL` heredoc
// cannot carry verbatim. The writer's contract is that unrepresentable
// content is refused, never silently rewritten: a line consisting of the
// heredoc delimiter would terminate the document early, HCL template
// interpolation sequences would be evaluated instead of read back as SQL, a
// carriage return would not survive the line-by-line re-indentation, and
// invalid UTF-8 produces a file neither parser can read.
func validateHCLHeredocLine(line string) error {
	if !utf8.ValidString(line) {
		return errors.New(
			"plan migration contains invalid UTF-8 and cannot be written as an Atlas .plan.hcl document; " +
				"write the native JSON plan format instead")
	}
	if strings.TrimSpace(line) == hclPlanHeredocDelimiter {
		return fmt.Errorf(
			"plan migration contains a line consisting of the heredoc delimiter %q and cannot be written as an Atlas .plan.hcl document; "+
				"write the native JSON plan format instead", hclPlanHeredocDelimiter)
	}
	if strings.Contains(line, "\r") {
		return errors.New(
			"plan migration contains a carriage return, which an Atlas .plan.hcl heredoc cannot carry back verbatim; " +
				"normalize the statement to LF line endings or write the native JSON plan format instead")
	}
	// A NUL byte is valid UTF-8 but truncates the heredoc line on the way
	// back in, so a write/read round trip would yield different, still
	// parseable SQL with no error at all.
	if strings.ContainsRune(line, 0) {
		return errors.New(
			"plan migration contains a NUL byte, which an Atlas .plan.hcl heredoc cannot carry back verbatim; " +
				"remove it or write the native JSON plan format instead")
	}
	// HCL excludes whitespace-only lines from `<<-` indent stripping and
	// leaves them untouched, so the two spaces the writer adds would come
	// back as part of the statement.
	if line != "" && strings.TrimSpace(line) == "" {
		return errors.New(
			"plan migration contains a whitespace-only line, which an Atlas .plan.hcl heredoc cannot carry back " +
				"verbatim; leave the line empty or write the native JSON plan format instead")
	}
	if strings.Contains(line, "${") || strings.Contains(line, "%{") {
		return errors.New(
			"plan migration contains an HCL template interpolation sequence (${ or %{) and cannot be written verbatim into an Atlas .plan.hcl heredoc; " +
				"write the native JSON plan format instead")
	}
	return nil
}

func firstAttributeName(attributes hclsyntax.Attributes) string {
	return slices.Min(slices.Collect(maps.Keys(attributes)))
}
