package shadow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/readscope"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/internal/shadowdb"
	"ptah.run/migration/migrator"
	"ptah.run/migration/schemadiff"
)

// AppliedVerifyOptions configures [VerifyApplied].
type AppliedVerifyOptions struct {
	// Runtime selects feature services and codecs for verification. It is required.
	Runtime schemadiff.DatabaseRuntime
	// ShadowDatabaseURL is an ephemeral database the verification replays the
	// applied history into. It must hold nothing the reset would drop when the
	// verification starts, it is empty again when the verification returns,
	// and its live realm must be distinct from TargetConn's.
	ShadowDatabaseURL string
	// TargetConn is the already-open database whose applied history is
	// checked. It is introspected and compared against, never written, and it
	// is required.
	TargetConn *dbschema.DatabaseConnection
	// Migrations are the migrations the target's history records as applied,
	// in replay order, as [migrator.AppliedHistory] holds them. Empty is a
	// history that applied nothing, and the target is then compared with an
	// empty database.
	Migrations []*migrator.Migration
	// Dialect is the target dialect the shadow database must match. The two
	// spellings are compared normalized, so an alias matches its canonical
	// name.
	Dialect string
	// Capabilities, when non-nil, must equal the shadow connection's resolved
	// capabilities exactly; a difference fails the verification. Nil skips the
	// check.
	Capabilities capability.Capabilities
	// CompareOptions tunes the schema comparison; nil selects
	// config.DefaultCompareOptions.
	CompareOptions *config.CompareOptions
	// Schemas scopes both introspections to the named schemas; empty reads
	// the connection's default scope. On PostgreSQL the named non-public
	// schemas are also dropped from the shadow database before the replay.
	Schemas []string
	// MetadataTables are the target's own bookkeeping tables, which no
	// migration creates: the revision table and its log under the names the
	// migrator was configured with. They are left out of the target read
	// before the comparison. The readers already leave out the default names.
	MetadataTables []string
	// Scope, when set, projects both sides of the comparison: the target read
	// and the schema the replay produced. A caller that excludes objects from
	// the check passes its selection here, and the projection must apply the
	// same selection to both sides.
	Scope func(ctx context.Context, target *catalog.Database, replayed *schemamodel.Database) (*catalog.Database, *schemamodel.Database, error)
	// ConnectTimeout bounds the shadow database connection attempt; zero or
	// negative means no bound beyond the caller's context.
	ConnectTimeout time.Duration
}

// AppliedResult is what [VerifyApplied] found when it could compare the target
// with its replayed history.
type AppliedResult struct {
	// Drift is every difference between the target and the schema its applied
	// history produces, in deterministic category and object order. A
	// missing_* kind is something the history created that the target lacks;
	// extra_* is something the target holds that no applied migration created.
	Drift []Mismatch `json:"drift,omitempty"`
	// Undecided is every object the replay created that the target read could
	// not describe, so the comparison could not say whether the target holds
	// it.
	Undecided []Mismatch `json:"undecided,omitempty"`
}

// Clean reports whether the target matched its history over everything the
// comparison could decide, and the comparison decided everything.
func (r AppliedResult) Clean() bool {
	return len(r.Drift) == 0 && len(r.Undecided) == 0
}

// VerifyApplied replays the migrations a target records as applied on the
// shadow database and compares the schema they produce with the target. The
// result lists the differences; an error is a verification that could not
// compare, and it supports errors.As with [VerificationError], whose stage and
// kind say why. Neither is a clean result.
//
// The target is read and never written. Call it while holding the target's
// migration lock, so the history it was given is the history the target
// still has when the comparison reads the target.
//
// A shadow database that holds anything the reset would drop is refused at
// the claim stage, before anything resets it, and the shadow database is
// emptied again on every return. A failure to empty it is joined to the error.
func VerifyApplied(ctx context.Context, opts AppliedVerifyOptions) (result AppliedResult, resultErr error) {
	if err := validateAppliedOptions(ctx, opts); err != nil {
		return AppliedResult{}, err
	}
	connectCtx, cancelConnect := connectContext(ctx, opts.ConnectTimeout)
	shadowConn, err := dbschema.ConnectToDatabase(connectCtx, opts.ShadowDatabaseURL)
	cancelConnect()
	if err != nil {
		return AppliedResult{}, appliedError("connect", "connect_error", "connect to shadow database", err)
	}
	defer dbschema.CloseAndWarn(shadowConn)

	if err := validateConnection(
		ctx,
		opts.TargetConn,
		shadowConn,
		opts.Dialect,
		opts.Capabilities,
		"target database connection is required",
	); err != nil {
		return AppliedResult{}, wrapAppliedError(err)
	}
	targetSchema, verr := readTargetSchema(ctx, shadowConn, targetRead{
		conn: opts.TargetConn, runtime: opts.Runtime, compare: opts.CompareOptions, dialect: opts.Dialect, schemas: opts.Schemas,
	})
	if verr != nil {
		return AppliedResult{}, wrapAppliedError(verr)
	}

	lease, err := shadowdb.Claim(ctx, shadowConn)
	if err != nil {
		return AppliedResult{}, appliedErrorWithDisplayMessage("claim", "not_clean", err.Error(), err)
	}
	defer func() {
		if releaseErr := lease.Release(ctx); releaseErr != nil {
			result, resultErr = AppliedResult{}, errors.Join(resultErr, releaseErr)
		}
	}()
	if err := shadowConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return AppliedResult{}, appliedError("drop-all", "drop_all_error", "drop all objects", err)
	}
	if err := shadowdb.ResetSchemas(ctx, shadowConn, opts.Schemas); err != nil {
		return AppliedResult{}, appliedErrorWithDisplayMessage("reset-schemas", "schema_reset_error", err.Error(), err)
	}

	mig := migrator.NewMigrator(shadowConn, migrator.NewRegisteredMigrationProvider(opts.Migrations...))
	lease.DropsMetadata(mig.MigrationsTableIdentifier(), mig.MigrationLogTableIdentifier())
	if err := mig.MigrateUp(ctx); err != nil {
		if description := shadowdb.DescribeReplayError(err); description != "" {
			return AppliedResult{}, appliedErrorWithDisplayMessage("replay", "replay_error", description, err)
		}
		return AppliedResult{}, appliedError("replay", "replay_error", "replay the applied migrations", err)
	}
	if err := shadowdb.DropMigrationMetadata(
		ctx, shadowConn, mig.MigrationsTableIdentifier(), mig.MigrationLogTableIdentifier(),
	); err != nil {
		return AppliedResult{}, appliedErrorWithDisplayMessage("drop-metadata", "drop_metadata_error", err.Error(), err)
	}

	shadowSchema, err := dbschema.ReadSchemaWithSchemasContext(readscope.Authored(ctx), shadowConn, opts.Schemas)
	if err != nil {
		return AppliedResult{}, appliedError("re-introspect", "re_introspect_error", "read shadow schema", err)
	}
	replayed, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, shadowSchema, opts.TargetConn.Info().Dialect, opts.Runtime)
	if err != nil {
		return AppliedResult{}, appliedError("schema-match", "conversion_error", "convert shadow schema", err)
	}
	targetSchema = revisiontable.Without(targetSchema, opts.MetadataTables)
	if opts.Scope != nil {
		targetSchema, replayed, err = opts.Scope(ctx, targetSchema, replayed)
		if err != nil {
			return AppliedResult{}, appliedError("scope", "scope_error", "select the objects to compare", err)
		}
	}
	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(
		ctx,
		opts.TargetConn,
		replayed,
		targetSchema,
		opts.CompareOptions,
		opts.Runtime,
	)
	if err != nil {
		return AppliedResult{}, appliedError(
			"schema-match",
			"identifier_resolution_error",
			"resolve target identifier semantics",
			err,
		)
	}
	identifierSemanticsMatch, err := identifierSemanticsAgree(ctx, shadowConn, opts.Dialect, *diff.IdentifierSemantics)
	if err != nil {
		return AppliedResult{}, appliedError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve replayed shadow identifier semantics",
			err,
		)
	}
	if !identifierSemanticsMatch {
		return AppliedResult{}, appliedError(
			"identifier-semantics-check",
			"identifier_semantics_mismatch",
			fmt.Sprintf("replayed shadow identifier semantics do not match target %s catalog semantics", opts.Dialect),
			nil,
		)
	}
	if diff.HasChanges() {
		result.Drift = collectMismatches(diff)
	}
	result.Undecided = undecidedMismatches(undecided)
	return result, nil
}

func appliedError(stage, kind, message string, err error) error {
	return wrapAppliedError(newVerificationError(stage, kind, message, err))
}

func appliedErrorWithDisplayMessage(stage, kind, message string, err error) error {
	return wrapAppliedError(newVerificationErrorWithDisplayMessage(stage, kind, message, err))
}

func wrapAppliedError(err *VerificationError) error {
	return fmt.Errorf("applied history %w", err)
}

func validateAppliedOptions(ctx context.Context, opts AppliedVerifyOptions) error {
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return appliedError("configuration", "runtime_error", "select feature runtime", err)
	}
	dialect := opts.Dialect
	if opts.TargetConn != nil {
		dialect = opts.TargetConn.Info().Dialect
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return appliedError(
			"configuration",
			"invalid_sqlite_virtual_table_drop_toggle",
			"validate SQLite virtual-table drop toggle",
			err,
		)
	}
	if err := targetConnectionRequiredError(opts.TargetConn, "target database connection is required"); err != nil {
		return wrapAppliedError(err)
	}
	return nil
}
