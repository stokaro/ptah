package atlasschema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	digest "github.com/opencontainers/go-digest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/sqlsafety"
	"go.5x5.cz/ptah/migration/risk"
	"go.5x5.cz/ptah/migration/safety"
)

const (
	// PlanFormatVersion identifies the local plan-file JSON contract produced
	// by `atlas schema plan` and consumed by `atlas schema apply --plan`.
	PlanFormatVersion = 1

	// PlanFileSuffix is the conventional local plan-file name suffix,
	// mirroring Atlas's `<name>.plan.hcl` with a JSON payload.
	PlanFileSuffix = ".plan.json"

	// planNameHashLength is the number of fingerprint-hash hex characters used
	// in a derived default plan name.
	planNameHashLength = 12
)

// PlanStatement is one ordered SQL statement of a saved declarative plan,
// together with the risk classification recorded when the plan was computed.
type PlanStatement struct {
	SQL      string        `json:"sql"`
	Severity risk.Severity `json:"severity"`
	Reason   string        `json:"reason"`
}

// PlanFile is the local pre-approved declarative migration plan document.
// It binds the ordered SQL statements to the fingerprint of the database
// state they were computed against, so a stale plan is detectable before
// execution.
type PlanFile struct {
	FormatVersion   int             `json:"format_version"`
	Name            string          `json:"name"`
	Dialect         string          `json:"dialect"`
	FromFingerprint string          `json:"from_fingerprint"`
	ToFingerprint   string          `json:"to_fingerprint"`
	Exclude         []string        `json:"exclude,omitempty"`
	Destructive     bool            `json:"destructive"`
	Statements      []PlanStatement `json:"statements"`
}

// PlanFileOptions configures PreparePlanFile.
type PlanFileOptions struct {
	// Name is the plan name recorded in the file. When empty, a deterministic
	// name is derived from the source and target fingerprints.
	Name string
	// DevURL, when set, must match the target connection's dialect.
	DevURL  string
	ToURLs  []string
	Exclude []string
	Policy  DiffPolicy
	// Desired supplies a pre-loaded desired schema model; see
	// [ApplyOptions.Desired]. When set, ToURLs are ignored.
	Desired *goschema.Database
}

// StalePlanError reports that the target database no longer matches the
// fingerprint the plan was computed against.
type StalePlanError struct {
	PlanFingerprint     string
	DatabaseFingerprint string
}

func (e *StalePlanError) Error() string {
	return fmt.Sprintf(
		"pre-planned migration is stale: the target database schema does not match the plan's source fingerprint "+
			"(plan %s, database %s); the database changed since the plan was computed, so re-run `schema plan` "+
			"against the current database and review the fresh plan",
		e.PlanFingerprint, e.DatabaseFingerprint)
}

// PreparePlanFile computes the declarative migration plan from the connected
// database (the plan source and later apply target) to the desired schema
// files, and packages it as a fingerprinted local plan document. A plan with
// no statements means the schema is synced; callers should not save it.
func PreparePlanFile(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts PlanFileOptions,
) (PlanFile, error) {
	if conn == nil {
		return PlanFile{}, errors.New("schema plan requires database connection")
	}
	if err := atlasurl.ValidateDialectMatch(opts.DevURL, conn.Info().Dialect); err != nil {
		return PlanFile{}, err
	}

	computation, err := computeApplyPlan(ctx, conn, ApplyOptions{
		ToURLs:  opts.ToURLs,
		Exclude: opts.Exclude,
		Policy:  opts.Policy,
		// A saved plan fingerprints local desired-state files; URL sources
		// stay a `schema plan` follow-up gap.
		LocalFilesOnly: true,
		Desired:        opts.Desired,
	})
	if err != nil {
		return PlanFile{}, err
	}

	fromFingerprint, err := SchemaFingerprint(computation.current)
	if err != nil {
		return PlanFile{}, fmt.Errorf("fingerprint current schema: %w", err)
	}
	toFingerprint, err := desiredSchemaFingerprint(computation.desired)
	if err != nil {
		return PlanFile{}, fmt.Errorf("fingerprint desired schema: %w", err)
	}
	name := strings.TrimSpace(opts.Name)
	if name == "" {
		name = defaultPlanName(fromFingerprint, toFingerprint)
	}

	statements, destructive := classifyPlanStatements(computation.statements, conn.Info().Dialect)

	return PlanFile{
		FormatVersion:   PlanFormatVersion,
		Name:            name,
		Dialect:         conn.Info().Dialect,
		FromFingerprint: fromFingerprint,
		ToFingerprint:   toFingerprint,
		Exclude:         opts.Exclude,
		Destructive:     destructive,
		Statements:      statements,
	}, nil
}

// classifyPlanStatements records the safety assessment of each raw statement
// and reports whether any of them is destructive. It is the single classifier
// behind every plan statement list, so a plan read from the Atlas format and a
// plan re-derived from operator-edited SQL carry the same per-statement
// metadata a freshly planned one records.
func classifyPlanStatements(raw []string, dialect string) (statements []PlanStatement, destructive bool) {
	statements = make([]PlanStatement, 0, len(raw))
	for _, statement := range raw {
		// Statements can carry comments and MySQL-family executable comments.
		// Classify the SQL the plan's dialect may execute, including guarded
		// executable-comment bodies regardless of server version.
		assessment := safety.AssessSQL(strings.TrimSpace(sqlsafety.SQLForAssessment(statement, dialect)))
		destructive = destructive || assessment.Severity == safety.Destructive
		statements = append(statements, PlanStatement{
			SQL:      statement,
			Severity: assessment.Severity,
			Reason:   assessment.Reason,
		})
	}
	return statements, destructive
}

// splitPlanStatements splits sqlText into plan statements, keeping each
// statement's text verbatim — leading comments included — and dropping only
// those with no executable body.
//
// Keeping the comments is what makes an edit round-trip lossless.
// [SplitApplyStatements] strips them, which is right for text about to be
// executed but wrong for text about to be *saved*: the statements
// [PreparePlanFile] records carry their generated comments, and the Atlas
// `.plan.hcl` shape has no severity field, so for a plan written in that shape
// the "-- WARNING: This will delete all data" line is the only in-artifact
// signal that the plan destroys data. Stripping it would let an operator who
// opened the editor and quit without typing anything turn a plan that warns
// into one that does not.
func splitPlanStatements(sqlText, dialect string) []string {
	raw := sqlutil.SplitSQLStatementsForDialect(sqlText, dialect)
	statements := make([]string, 0, len(raw))
	for _, statement := range raw {
		statement = strings.TrimSpace(statement)
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)) == "" {
			continue
		}
		statements = append(statements, statement)
	}
	return statements
}

// WithStatementsFromSQL returns a copy of the plan whose statement list,
// per-statement severity, and plan-level destructive marker are re-derived
// from sqlText, split with the plan's own dialect. It backs `schema plan
// --edit`, where the operator rewrites the planned SQL before it is saved.
//
// Re-deriving the severity metadata is the point: an edit that introduces a
// DROP must not be saved under the destructive=false marker the pre-edit plan
// carried.
//
// Statement text is preserved verbatim, so feeding back an unmodified
// [PlanFile.SQL] reproduces the same statements — an editor the operator quits
// without changing anything yields the same plan document as no edit at all.
//
// The fingerprints are deliberately NOT recomputed. `from` is the fingerprint
// of the live source database, which an edit cannot change. `to` is the
// fingerprint of the desired schema the plan was computed against, and edited
// SQL may no longer reach it — that claim is verified where it can be, at
// apply time: an Atlas-format plan is replayed on a dev database and required
// to converge on `--to` before the target is touched. A native JSON plan
// carries no such replay, so an edited JSON plan is only as good as the review
// it received. [MarshalPlanFileAs] callers that accept edits must say so.
func (p PlanFile) WithStatementsFromSQL(sqlText string) PlanFile {
	p.Statements, p.Destructive = classifyPlanStatements(splitPlanStatements(sqlText, p.Dialect), p.Dialect)
	return p
}

// HasChanges reports whether the plan contains any statement.
func (p PlanFile) HasChanges() bool {
	return len(p.Statements) > 0
}

// SQL returns the plan statements as one executable SQL script.
func (p PlanFile) SQL() string {
	return FormatMigrationSQL(p.StatementSQL())
}

// StatementSQL returns the ordered plan statement SQL texts.
func (p PlanFile) StatementSQL() []string {
	statements := make([]string, 0, len(p.Statements))
	for _, statement := range p.Statements {
		statements = append(statements, statement.SQL)
	}
	return statements
}

// MarshalPlanFile renders the canonical plan document. The output is
// deterministic for identical plan contents.
func MarshalPlanFile(plan PlanFile) ([]byte, error) {
	document, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("render plan file: %w", err)
	}
	return append(document, '\n'), nil
}

// ReadPlanFile loads and validates a local JSON plan document. Unknown fields
// are rejected so a hand-edited or truncated plan fails loudly instead of
// being partially honored. The native plan format is JSON only: an Atlas
// `.plan.hcl` document is refused with a named error naming the command that
// does read it, rather than the raw JSON decoder complaint. The
// Atlas-compatible command tree reads both encodings through
// [ReadPlanDocument].
func ReadPlanFile(path string) (PlanFile, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return PlanFile{}, fmt.Errorf("read plan file: %w", err)
	}
	if DetectPlanFormat(contents) == PlanFormatHCL {
		return PlanFile{}, fmt.Errorf(
			"plan file %s is in the Atlas %s format, which the native `ptah schema apply --plan` does not read; "+
				"apply it with `ptah-compat schema apply --plan file://%s --to <desired state>`, "+
				"or produce a native plan with `ptah schema plan --output <name>%s`",
			path, PlanFileSuffixHCL, path, PlanFileSuffix)
	}
	return decodePlanJSON(contents, path)
}

// decodePlanJSON parses and validates the native format_version-1 JSON plan
// document.
func decodePlanJSON(contents []byte, path string) (PlanFile, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var plan PlanFile
	if err := decoder.Decode(&plan); err != nil {
		return PlanFile{}, fmt.Errorf("parse plan file %s: %w", path, err)
	}
	if err := validatePlanFile(plan); err != nil {
		return PlanFile{}, fmt.Errorf("invalid plan file %s: %w", path, err)
	}
	return plan, nil
}

// VerifyPlanTarget checks that the connected database is the state the plan
// was computed against: the dialects must be compatible and the current
// schema, filtered with the plan's recorded exclude patterns, must match the
// plan's source fingerprint. A fingerprint mismatch returns *StalePlanError.
func VerifyPlanTarget(conn *dbschema.DatabaseConnection, plan PlanFile) error {
	if conn == nil {
		return errors.New("plan verification requires database connection")
	}
	// Atlas-format plan files carry no dialect field; for those the rehearsal
	// and desired-state verification pin the semantics instead.
	if plan.Dialect != "" && !planDialectsCompatible(plan.Dialect, conn.Info().Dialect) {
		return fmt.Errorf("plan file targets dialect %q, but the --url database dialect is %q",
			plan.Dialect, conn.Info().Dialect)
	}

	current, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return fmt.Errorf("read database schema: %w", err)
	}
	current, err = atlasfilter.ExcludeDatabase(current, plan.Exclude)
	if err != nil {
		return fmt.Errorf("apply plan exclude patterns to current schema: %w", err)
	}
	fingerprint, err := SchemaFingerprint(current)
	if err != nil {
		return fmt.Errorf("fingerprint current schema: %w", err)
	}
	if fingerprint != plan.FromFingerprint {
		return &StalePlanError{
			PlanFingerprint:     plan.FromFingerprint,
			DatabaseFingerprint: fingerprint,
		}
	}
	return nil
}

// SchemaFingerprint returns the deterministic fingerprint of an introspected
// schema: the SHA-256 digest of its canonical JSON encoding (Go's
// encoding/json emits struct fields in declaration order and map keys
// sorted), in `sha256:<hex>` form. The same mechanism binds migration-plan
// OCI attachments to live schema state (internal/planartifact).
func SchemaFingerprint(schema *types.DBSchema) (string, error) {
	if schema == nil {
		return "", errors.New("schema fingerprint requires schema")
	}
	payload, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("marshal schema fingerprint input: %w", err)
	}
	return digest.FromBytes(payload).String(), nil
}

// desiredSchemaFingerprint fingerprints the loaded desired schema model. It is
// informational: apply --plan executes the recorded statements and only
// verifies the source fingerprint, but the target fingerprint lets tooling
// detect that a plan no longer corresponds to the desired sources.
func desiredSchemaFingerprint(desired *goschema.Database) (string, error) {
	if desired == nil {
		return "", errors.New("desired schema fingerprint requires schema")
	}
	payload, err := json.Marshal(desired)
	if err != nil {
		return "", fmt.Errorf("marshal desired schema fingerprint input: %w", err)
	}
	return digest.FromBytes(payload).String(), nil
}

func defaultPlanName(fromFingerprint, toFingerprint string) string {
	sum := digest.FromString(fromFingerprint + "\n" + toFingerprint).Encoded()
	return "plan_" + sum[:planNameHashLength]
}

func validatePlanFile(plan PlanFile) error {
	if plan.FormatVersion != PlanFormatVersion {
		return fmt.Errorf("unsupported plan format_version %d (this Ptah build supports %d)",
			plan.FormatVersion, PlanFormatVersion)
	}
	if strings.TrimSpace(plan.Dialect) == "" {
		return errors.New("plan dialect is required")
	}
	if strings.TrimSpace(plan.FromFingerprint) == "" {
		return errors.New("plan from_fingerprint is required")
	}
	if len(plan.Statements) == 0 {
		return errors.New("plan contains no statements")
	}
	for i, statement := range plan.Statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("plan statement %d has empty sql", i+1)
		}
	}
	return nil
}

func planDialectsCompatible(planDialect, connDialect string) bool {
	return normalizePlanDialect(planDialect) == normalizePlanDialect(connDialect)
}

func normalizePlanDialect(dialect string) string {
	normalized := strings.ToLower(strings.TrimSpace(dialect))
	if normalized == "postgresql" {
		return "postgres"
	}
	return normalized
}
