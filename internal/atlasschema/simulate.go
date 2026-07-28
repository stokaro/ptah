package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// SimulationError reports that the generated apply plan failed to rehearse on
// the --dev-url dev database. The caller must refuse the target apply; the
// target database has not been modified.
type SimulationError struct {
	// Stage is the simulation phase that failed: "reset" (cleaning the dev
	// database), "baseline" (recreating the target's current schema), or
	// "plan" (executing the ordered plan statements).
	Stage string
	Err   error
}

func (e *SimulationError) Error() string {
	return fmt.Sprintf(
		"dev database simulation failed during %s: %v; the target database was left unchanged",
		e.Stage, e.Err,
	)
}

func (e *SimulationError) Unwrap() error {
	return e.Err
}

// IsSimulationFailure reports whether err wraps a dev database simulation
// failure.
func IsSimulationFailure(err error) bool {
	var target *SimulationError
	return errors.As(err, &target)
}

// SimulateOptions configures the pre-apply dev database simulation.
type SimulateOptions struct {
	// DevURL is the dev database the plan is rehearsed on. Empty skips the
	// simulation.
	DevURL string
	// TargetURL guards against pointing the simulation at the target itself:
	// the dev database is reset destructively before the plan is rehearsed.
	TargetURL string
	// DesiredURLs are the raw --to values; a database-URL desired state must
	// not double as the dev database, because the reset would destroy the
	// desired-state source right after it was introspected.
	DesiredURLs []string
	// Statements overrides the rehearsed statements; empty rehearses the
	// prepared plan. `schema apply --edit` passes the edited statements so the
	// simulation covers exactly what would be applied.
	Statements []string
}

// SimulateOnDev rehearses the exact ordered apply plan on the --dev-url dev
// database before the target is touched. The dev database is reset
// deterministically, the target's introspected current schema is recreated on
// it, and the planned statements then execute in order under the same
// transaction mode as the target apply. Any failure surfaces as a
// *SimulationError and the caller must refuse the target apply; the target
// database has not been modified. Empty DevURL and empty plans are no-ops.
func (p ApplyRuntimePlan) SimulateOnDev(ctx context.Context, opts SimulateOptions) error {
	devURL := strings.TrimSpace(opts.DevURL)
	if devURL == "" {
		return nil
	}
	statements := opts.Statements
	if len(statements) == 0 {
		statements = p.plan.statements
	}
	if len(statements) == 0 {
		return nil
	}
	if p.conn == nil {
		return errors.New("schema apply simulation requires database connection")
	}

	targetInfo := p.conn.Info()
	if err := atlasurl.ValidateDialectMatch(devURL, targetInfo.Dialect); err != nil {
		return err
	}
	if isDockerSimulationURL(devURL) {
		return errors.New("docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for schema apply simulation")
	}
	if devURL == strings.TrimSpace(opts.TargetURL) {
		return errors.New("--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it")
	}
	for _, desired := range opts.DesiredURLs {
		if devURL == strings.TrimSpace(desired) {
			return fmt.Errorf("--dev-url must not point at the --to desired-state database %q: the dev database is reset destructively before the plan is rehearsed on it", desired)
		}
	}

	devConn, err := dbschema.ConnectToDatabase(ctx, devURL)
	if err != nil {
		return fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)

	devInfo := devConn.Info()
	if platform.NormalizeDialect(devInfo.Dialect) != platform.NormalizeDialect(targetInfo.Dialect) {
		return fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", devInfo.Dialect, targetInfo.Dialect)
	}
	if err := checkSimulationSchemaScope(devInfo, targetInfo); err != nil {
		return err
	}

	devConn.SchemaWriter().SetDryRun(false)
	if err := devConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return &SimulationError{Stage: "reset", Err: err}
	}
	if err := recreateCurrentSchema(ctx, devConn, p.current); err != nil {
		return &SimulationError{Stage: "baseline", Err: err}
	}
	if err := applyStatements(ctx, devConn, p.txMode, statements); err != nil {
		return &SimulationError{Stage: "plan", Err: err}
	}
	return nil
}

// checkSimulationSchemaScope fails explicitly when the dev database's schema
// scope differs from the target's. Only dialects whose connection schema names
// a namespace inside the database participate: for MySQL, MariaDB, and
// ClickHouse it names the database itself, which legitimately differs between
// a target and its dev counterpart.
func checkSimulationSchemaScope(devInfo, targetInfo dbschematypes.DBInfo) error {
	switch platform.NormalizeDialect(targetInfo.Dialect) {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		return nil
	}
	if devInfo.Schema == targetInfo.Schema {
		return nil
	}
	return fmt.Errorf(
		"--dev-url schema scope %q does not match --url schema scope %q; point --dev-url at a dev database using the same schema",
		devInfo.Schema, targetInfo.Schema,
	)
}

// recreateCurrentSchema converges the freshly reset dev database to the
// target's introspected (and scope/exclude-filtered) current schema, so the plan is
// rehearsed against the same starting state it was computed for.
func recreateCurrentSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	current *dbschematypes.DBSchema,
) error {
	if current == nil {
		return nil
	}
	baseline := dbschematogo.ConvertDBSchemaToGoSchema(current)
	normalizeBaselineSerialColumns(baseline, devConn.Info().Dialect)
	devCurrent, err := dbschema.ReadSchemaWithSchemas(devConn, nil)
	if err != nil {
		return fmt.Errorf("read dev database schema: %w", err)
	}
	info := devConn.Info()
	diff, err := schemadiff.CompareWithDatabase(ctx, devConn, baseline, devCurrent, nil)
	if err != nil {
		return fmt.Errorf("compare current schema with dev database: %w", err)
	}
	if !diff.HasChanges() {
		return nil
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, baseline, info.Dialect, planner.Options{
		Capabilities: info.Capabilities,
	})
	if err != nil {
		return fmt.Errorf("generate current schema DDL for dev database: %w", err)
	}
	return executeApplyStatements(ctx, devConn.Writer(), statements)
}

// normalizeBaselineSerialColumns rewrites introspected PostgreSQL-family
// SERIAL columns back to their SERIAL spelling for baseline recreation.
// Introspection deliberately omits the implicit sequences owned by SERIAL
// columns, so replaying the raw "integer DEFAULT nextval('...')" form on an
// empty dev database would reference a sequence that never gets created.
// Columns whose nextval default names an explicitly introspected sequence
// keep their default: that sequence is part of the baseline and is created.
func normalizeBaselineSerialColumns(baseline *goschema.Database, dialect string) {
	if !platform.IsPostgresFamily(dialect) {
		return
	}
	sequences := make(map[string]bool, len(baseline.Sequences))
	for _, sequence := range baseline.Sequences {
		sequences[strings.ToLower(sequence.Name)] = true
	}
	for i := range baseline.Fields {
		field := &baseline.Fields[i]
		sequenceName := nextvalSequenceName(field.DefaultExpr)
		if sequenceName == "" || sequences[strings.ToLower(sequenceName)] {
			continue
		}
		serialType, ok := serialTypeForBaseline(field.Type)
		if !ok {
			continue
		}
		field.Type = serialType
		field.DefaultExpr = ""
	}
}

// nextvalSequenceName extracts the unqualified sequence name from a
// PostgreSQL nextval('<name>'::regclass) column default, or "" when the
// default is not a nextval call.
func nextvalSequenceName(defaultExpr string) string {
	trimmed := strings.TrimSpace(defaultExpr)
	if !strings.HasPrefix(strings.ToLower(trimmed), "nextval(") {
		return ""
	}
	start := strings.Index(trimmed, "'")
	if start < 0 {
		return ""
	}
	end := strings.Index(trimmed[start+1:], "'")
	if end < 0 {
		return ""
	}
	name := trimmed[start+1 : start+1+end]
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	return strings.Trim(name, `"`)
}

func serialTypeForBaseline(columnType string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(columnType)) {
	case "integer", "int", "int4":
		return "SERIAL", true
	case "bigint", "int8":
		return "BIGSERIAL", true
	case "smallint", "int2":
		return "SMALLSERIAL", true
	}
	return "", false
}

func isDockerSimulationURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	return err == nil && parsed.Scheme == "docker"
}
