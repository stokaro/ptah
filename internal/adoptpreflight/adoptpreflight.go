// Package adoptpreflight answers one question about a database that already
// carries migration history: may native Ptah take over as its writer?
//
// Project-file compatibility is not enough to answer it. A project whose every
// construct means the same thing in a native Ptah file can still sit in front
// of a database whose revision rows native Ptah would read as an empty history
// -- and an empty history is a writer that re-runs SQL which already ran.
// stokaro/ptah#1215 asks for the database half to be decided before the writer
// is switched, and for it to be decided WITHOUT any of the five outcomes it
// names: marking SQL applied that did not run, re-running applied SQL,
// discarding revision rows, inferring ownership from ambiguous evidence, or
// rewriting persisted state to make a check pass.
//
// The last one is why nothing here builds its own reader. Every finding comes
// from the migrator that owns the layout it is reading -- the same code that
// wrote those rows, through migration/migrator -- and the connection is put in
// dry-run mode before anything touches it, because the migrator's read entry
// points call Initialize, and a live Initialize CREATES an absent revision
// table and ALTERS an existing one into the current layout. A preflight that
// ran through the ordinary path would rewrite the state it came to inspect.
package adoptpreflight

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/revisiontable"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/migrator"
)

// Severity is what a finding asks of the operator.
type Severity string

const (
	// SeverityOK is a dimension native Ptah can operate as it stands.
	SeverityOK Severity = "ok"
	// SeverityAction is a step that must happen before the writer is switched.
	// The adoption is possible; it is not automatic.
	SeverityAction Severity = "action"
	// SeverityRefuse is a state adoption cannot be performed from. #1215 asks
	// that this be said and explained rather than worked around.
	SeverityRefuse Severity = "refuse"
)

// The dimensions #1215 names. They are the report's grouping, and they are
// exported so a caller can say which one it acted on.
const (
	// DimensionRepresentation is which layout the history is written in and
	// where it lives.
	DimensionRepresentation = "revision representation"
	// DimensionIdentity is whether the recorded revisions and the migration
	// directory name the same migrations.
	DimensionIdentity = "migration identity"
	// DimensionChecksum is whether the recorded checksums are ones the current
	// directory accounts for.
	DimensionChecksum = "checksum representation"
	// DimensionState is dirty and partially applied revisions.
	DimensionState = "dirty/partial state"
	// DimensionSemantics is baseline and manually-set rows: SQL the history
	// records as applied that this database never ran.
	DimensionSemantics = "baseline/repeatable semantics"
	// DimensionMetadata is what the rows say about who wrote them.
	DimensionMetadata = "execution metadata"
)

// Finding is one dimension's answer.
type Finding struct {
	// Dimension is one of the constants above.
	Dimension string `json:"dimension"`
	// Severity is what it asks of the operator.
	Severity Severity `json:"severity"`
	// Summary is the one-line answer.
	Summary string `json:"summary"`
	// Detail says what to do about it, or why it cannot be done. It is empty
	// only when the summary is already the whole answer.
	Detail string `json:"detail,omitempty"`
}

// Report is what the preflight decided.
type Report struct {
	// Format is the layout the history was found in, "ptah" or "atlas". It is
	// empty when the database holds no revision history at all.
	Format string `json:"format,omitempty"`
	// Schema and Table are where it was found, as the preflight looked for it.
	// Schema is empty where the dialect has none to name, or where the
	// connection's own is used.
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	// Revisions is how many rows the history holds.
	Revisions int `json:"revisions"`
	// Findings are the dimensions, in the constant order above.
	Findings []Finding `json:"findings"`
	// Ready reports that native Ptah may take over with no further step. It is
	// false when anything is an action or a refusal.
	Ready bool `json:"ready"`
	// Refused reports that adoption cannot be performed from this state.
	Refused bool `json:"refused"`
}

// Actions is how many findings ask for a step before the writer is switched.
// It is the count #1215's example report prints as "database-state action".
func (r Report) Actions() int {
	count := 0
	for _, finding := range r.Findings {
		if finding.Severity != SeverityOK {
			count++
		}
	}
	return count
}

// Options is what the preflight needs to look in the right place.
type Options struct {
	// Conn is the database to inspect. The preflight puts its writer in
	// dry-run mode and never takes it out again: the connection is the
	// caller's, and a caller that wanted to write through it afterwards would
	// be doing so in a process that just promised not to.
	Conn *dbschema.DatabaseConnection
	// DatabaseURL is the URL Conn was opened from. It decides where the
	// Atlas-compatible layout keeps its table on the PostgreSQL family, which
	// is a property of the URL rather than of the connection: a URL pinning a
	// search_path keeps its bookkeeping there.
	DatabaseURL string
	// MigrationsFS is the migration directory. It may be nil, and a project
	// that names none is not an error: the dimensions that compare the history
	// against the files then report that they could not be decided, rather
	// than reporting that there was nothing wrong.
	MigrationsFS fs.FS
	// DirFormat is the directory's layout.
	DirFormat migrationfile.DirFormat
	// RevisionsSchema and RevisionsTable are what the project declares. Empty
	// means the layout's own default, which is what lets the preflight find a
	// history whose location was never configured.
	RevisionsSchema string
	RevisionsTable  string
	// RevisionFormat is the layout the project declares, empty when it
	// declares none. Declaring one settles which table the preflight reads;
	// declaring none is what makes the two defaults distinguishable evidence.
	RevisionFormat migrator.RevisionTableFormat
}

// Analyze inspects the database and decides. It writes nothing.
func Analyze(ctx context.Context, opts Options) (Report, error) {
	if opts.Conn == nil {
		return Report{}, errors.New("adoption preflight needs a database connection")
	}
	// Before any statement. Every read entry point on the migrator calls
	// Initialize, and outside dry-run Initialize creates the revision table
	// when it is absent and alters an existing one into the current layout --
	// so this line is what separates a preflight from a conversion nobody
	// asked for.
	opts.Conn.SchemaWriter().SetDryRun(true)

	report := Report{}
	found, finding, err := locate(ctx, opts)
	if err != nil {
		return Report{}, err
	}
	report.Findings = append(report.Findings, finding)
	report.Format = string(found.format)
	report.Schema = found.schema
	report.Table = found.table

	if found.present {
		if err := inspect(ctx, opts, found, &report); err != nil {
			return Report{}, err
		}
	}

	for _, f := range report.Findings {
		if f.Severity == SeverityRefuse {
			report.Refused = true
		}
	}
	report.Ready = report.Actions() == 0
	return report, nil
}

// location is where a history was found, and in whose layout.
type location struct {
	format  migrator.RevisionTableFormat
	schema  string
	table   string
	present bool
}

// locate finds the revision history.
//
// The two layouts have different default table names, so when a project
// declares no format the presence of one default and the absence of the other
// IS the evidence of which tool has been writing here. Both present is not a
// preference to resolve: it is two histories, and picking one would be
// inferring ownership from ambiguous evidence, which #1215 forbids by name.
//
// A project that declares its format is read under that format and no
// inference happens at all. A project that declares a table name but no format
// leaves nothing to distinguish the layouts by, and is refused for the same
// reason rather than probed under whichever default the migrator happens to
// carry.
func locate(ctx context.Context, opts Options) (location, Finding, error) {
	if opts.RevisionFormat != "" {
		found, err := probe(ctx, opts, opts.RevisionFormat)
		if err != nil {
			return location{}, Finding{}, err
		}
		return found, representationFinding(found), nil
	}

	if strings.TrimSpace(opts.RevisionsTable) != "" {
		return location{}, Finding{
			Dimension: DimensionRepresentation,
			Severity:  SeverityRefuse,
			Summary: fmt.Sprintf(
				"the project names revision table %q and no format for it", opts.RevisionsTable),
			Detail: "the layouts are told apart by their default table names, and a declared name removes that evidence: " +
				"set the migration revision format to the layout these rows were written in",
		}, nil
	}

	native, err := probe(ctx, opts, migrator.RevisionTableFormatPtah)
	if err != nil {
		return location{}, Finding{}, err
	}
	atlas, err := probe(ctx, opts, migrator.RevisionTableFormatAtlas)
	if err != nil {
		return location{}, Finding{}, err
	}

	switch {
	case native.present && atlas.present:
		return location{}, Finding{
			Dimension: DimensionRepresentation,
			Severity:  SeverityRefuse,
			Summary:   "this database holds two migration histories",
			Detail: fmt.Sprintf(
				"%s and %s both exist, and nothing here says which one describes the schema: "+
					"remove the one that does not, or name the surviving one explicitly",
				qualify(native), qualify(atlas)),
		}, nil
	case atlas.present:
		return atlas, representationFinding(atlas), nil
	case native.present:
		return native, representationFinding(native), nil
	default:
		return location{}, Finding{
			Dimension: DimensionRepresentation,
			Severity:  SeverityOK,
			Summary:   "this database records no migration history",
			Detail:    "there is no persisted state for adoption to carry",
		}, nil
	}
}

// representationFinding says what the located layout means for adoption.
//
// It depends only on WHICH layout holds the rows, never on whether the project
// file named it. Those two are not distinguishable here and the difference does
// not change the step: an atlas.hcl `migration` block defaults revision_format
// to atlas and marks it present, so a project naming no format at all arrives
// here looking exactly like one that named it -- measured on PostgreSQL 17
// against a history ptah-compat wrote, where an earlier version of this
// function reported that the project declared a format its file never mentions.
//
// The Atlas layout is an action either way, because the native project file the
// adoption produces is a different file: whatever atlas.hcl defaulted does not
// carry across, and a native project that does not name this layout writes its
// own and reads the database as never migrated.
func representationFinding(found location) Finding {
	if !found.present {
		return Finding{
			Dimension: DimensionRepresentation,
			Severity:  SeverityOK,
			Summary:   fmt.Sprintf("this database records no migration history in %s", qualify(found)),
			Detail:    "there is no persisted state for adoption to carry",
		}
	}
	if found.format == migrator.RevisionTableFormatAtlas {
		return Finding{
			Dimension: DimensionRepresentation,
			Severity:  SeverityAction,
			Summary:   fmt.Sprintf("the history is in the Atlas-compatible layout, in %s", qualify(found)),
			Detail: "native Ptah reads this layout only when the project names it: set the migration " +
				"revision format to atlas in the native project file, or Ptah writes its own layout, " +
				"reads this database as never migrated, and applies every recorded migration again",
		}
	}
	return Finding{
		Dimension: DimensionRepresentation,
		Severity:  SeverityOK,
		Summary:   fmt.Sprintf("the history is in Ptah's native layout, in %s", qualify(found)),
	}
}

// probe asks whether one layout's table is there. It is the only question that
// can be asked without Initialize, which is why it is asked first and why
// nothing here has yet read a row.
func probe(ctx context.Context, opts Options, format migrator.RevisionTableFormat) (location, error) {
	schema := opts.RevisionsSchema
	if format == migrator.RevisionTableFormatAtlas {
		schema = revisiontable.Schema(schema, opts.DatabaseURL)
	}
	mig, err := build(opts, format, schema)
	if err != nil {
		return location{}, err
	}
	present, err := mig.MetadataPresent(ctx)
	if err != nil {
		return location{}, fmt.Errorf("failed to look for the %s revision table: %w", format, err)
	}
	return location{
		format:  format,
		schema:  schema,
		table:   tableName(opts, format),
		present: present,
	}, nil
}

// build makes the migrator that owns one layout.
//
// A project naming no migration directory still gets a migrator, with an empty
// provider: presence and the row-level dimensions are answerable without files,
// and the dimensions that are not say so rather than passing.
func build(opts Options, format migrator.RevisionTableFormat, schema string) (*migrator.Migrator, error) {
	if opts.MigrationsFS == nil {
		return migrator.NewMigrator(opts.Conn, migrator.NewRegisteredMigrationProvider()).
			WithMigrationsTable(schema, opts.RevisionsTable).
			WithRevisionTableFormat(format), nil
	}
	mig, err := migrator.NewFSMigrator(opts.Conn, opts.MigrationsFS,
		migrator.WithMigrationDirFormat(opts.DirFormat))
	if err != nil {
		return nil, fmt.Errorf("failed to read the migration directory: %w", err)
	}
	return mig.
		WithMigrationsTable(schema, opts.RevisionsTable).
		WithRevisionTableFormat(format), nil
}

// tableName is where the layout keeps its rows, as the preflight looked.
func tableName(opts Options, format migrator.RevisionTableFormat) string {
	if name := strings.TrimSpace(opts.RevisionsTable); name != "" {
		return name
	}
	if format == migrator.RevisionTableFormatAtlas {
		return revisiontable.Atlas
	}
	return revisiontable.Ptah
}

// qualify names a table the way an operator would have to type it.
func qualify(found location) string {
	if found.schema == "" {
		return found.table
	}
	return found.schema + "." + found.table
}

// inspect reads the located history and decides the remaining dimensions.
func inspect(ctx context.Context, opts Options, found location, report *Report) error {
	mig, err := build(opts, found.format, found.schema)
	if err != nil {
		return err
	}
	// Asked before the rows, because it decides what several of the reads
	// below are actually able to answer.
	baseLayout, err := mig.RevisionLayoutBase(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the revision table layout: %w", err)
	}
	snapshot, err := mig.GetMigrationStatusSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the migration history: %w", err)
	}
	report.Revisions = len(snapshot.Revisions)

	if baseLayout {
		report.Findings = append(report.Findings, Finding{
			Dimension: DimensionRepresentation,
			Severity:  SeverityAction,
			Summary:   fmt.Sprintf("%s carries only the base columns", qualify(found)),
			Detail: "the first native write adds the rest with ALTER TABLE: take the outage that implies " +
				"deliberately rather than on whatever migration runs first",
		})
	}

	// The checksum answer is chosen HERE rather than inside the finding,
	// because the two come from different places: one is read from the
	// database, the other is what there is to say when the table has no
	// checksum column to read.
	checksum := baseLayoutChecksumFinding()
	if !baseLayout {
		checksum = checksumFinding(ctx, opts, mig)
	}

	report.Findings = append(report.Findings,
		stateFinding(snapshot),
		identityFinding(opts, mig, snapshot),
		checksum,
		semanticsFinding(snapshot),
		metadataFinding(snapshot),
	)
	return nil
}

// baseLayoutChecksumFinding is the checksum answer for a table that has no
// checksum column.
//
// The applied-checksum rule returns clean without looking at anything there,
// and reporting that clean return as a clean result would be a check that
// passes by not running.
func baseLayoutChecksumFinding() Finding {
	return Finding{
		Dimension: DimensionChecksum,
		Severity:  SeverityAction,
		Summary:   "the recorded revisions carry no checksums to verify",
		Detail: "this table has no checksum column, so nothing here proves the migrations that ran " +
			"are the ones in this directory",
	}
}

// stateFinding decides the dirty/partial dimension.
//
// A dirty revision is a migration that started and did not finish. Switching
// writers across one asks the new writer to decide what the old one was in the
// middle of, from a row the old one had not finished writing -- so this is a
// refusal, and the repair belongs to the writer that made the row.
func stateFinding(snapshot migrator.MigrationStatusSnapshot) Finding {
	var unfinished []string
	for _, revision := range snapshot.Revisions {
		if reason := unfinishedReason(revision); reason != "" {
			unfinished = append(unfinished, fmt.Sprintf("%s (%s)", revisionName(revision), reason))
		}
	}
	if len(unfinished) == 0 {
		return Finding{
			Dimension: DimensionState,
			Severity:  SeverityOK,
			Summary:   "every recorded revision finished",
		}
	}
	return Finding{
		Dimension: DimensionState,
		Severity:  SeverityRefuse,
		Summary:   fmt.Sprintf("%d revision(s) did not finish", len(unfinished)),
		Detail: "repair them with the writer that made them, then run this preflight again: " +
			strings.Join(unfinished, ", "),
	}
}

// unfinishedReason says why one row is not a finished migration, or "" when it
// is. Each clause is a separate way for a run to stop, and a row can carry
// more than one -- the first that matches is the one worth printing.
func unfinishedReason(revision migrator.MigrationRevision) string {
	switch {
	case revision.Dirty:
		return "dirty"
	case revision.Error != "":
		return "failed: " + firstLine(revision.Error)
	case revision.Total > 0 && revision.Applied < revision.Total:
		return fmt.Sprintf("%d of %d statements applied", revision.Applied, revision.Total)
	case revision.Direction == migrator.MigrationDirectionDown:
		return "left behind by a rollback"
	}
	return ""
}

// identityFinding decides whether the history and the directory name the same
// migrations.
//
// A revision naming a migration the directory does not contain is the case
// that matters: native Ptah cannot verify it, cannot roll it back, and cannot
// tell whether the schema it describes is the one in front of it.
//
// The comparison is against the PROVIDER's migrations -- the files -- and not
// against the status's applied list, which is derived from these same revision
// rows and would therefore contain every one of them by construction. A check
// written that way passes on a directory with no files in it at all.
//
// Rows and files are matched on the exact persisted identity, which is the key
// the migrator matches them on itself: a converted history's rows carry an
// opaque token rather than the numeric ordering key, and two of them can share
// a version.
func identityFinding(
	opts Options,
	mig *migrator.Migrator,
	snapshot migrator.MigrationStatusSnapshot,
) Finding {
	if opts.MigrationsFS == nil {
		return Finding{
			Dimension: DimensionIdentity,
			Severity:  SeverityAction,
			Summary:   "the project names no migration directory, so the recorded revisions were compared with nothing",
			Detail: "adoption needs to know that the history and the files name the same migrations: " +
				"point the project at its migration directory and run this again",
		}
	}

	files := mig.MigrationProvider().Migrations()
	known := make(map[string]struct{}, len(files))
	for _, file := range files {
		known[file.RevisionVersion()] = struct{}{}
	}
	var orphaned []string
	for _, revision := range snapshot.Revisions {
		if _, ok := known[revision.RevisionVersion()]; ok {
			continue
		}
		orphaned = append(orphaned, revisionName(revision))
	}
	sort.Strings(orphaned)

	if len(orphaned) > 0 {
		return Finding{
			Dimension: DimensionIdentity,
			Severity:  SeverityRefuse,
			Summary:   fmt.Sprintf("%d recorded revision(s) name migrations this directory does not contain", len(orphaned)),
			Detail: "adoption cannot carry a history it cannot read the SQL for: " +
				strings.Join(orphaned, ", "),
		}
	}
	if len(snapshot.Status.OutOfOrderMigrations) > 0 {
		return Finding{
			Dimension: DimensionIdentity,
			Severity:  SeverityAction,
			Summary: fmt.Sprintf("%d migration(s) sit before the applied high-water mark and have not run",
				len(snapshot.Status.OutOfOrderMigrations)),
			Detail: "native Ptah refuses these unless the project's execution order allows them: " +
				"apply them, remove them, or declare the order that admits them",
		}
	}
	return Finding{
		Dimension: DimensionIdentity,
		Severity:  SeverityOK,
		Summary:   "every recorded revision names a migration this directory contains",
	}
}

// checksumFinding decides whether the recorded checksums are ones the current
// directory accounts for.
//
// The question is asked through the migrator rather than answered here. An
// Atlas-written history records an atlas.sum running hash, which chains over
// every preceding file and which no per-file content hash reproduces; the rule
// that accepts it lives with the writer, and a second copy here would disagree
// with it the first time either learned something.
func checksumFinding(ctx context.Context, opts Options, mig *migrator.Migrator) Finding {
	if opts.MigrationsFS == nil {
		return Finding{
			Dimension: DimensionChecksum,
			Severity:  SeverityAction,
			Summary:   "the recorded checksums were compared with nothing",
			Detail:    "there is no migration directory to compute them from",
		}
	}
	reconcile, err := mig.VerifyAppliedChecksums(ctx)
	if mismatch, ok := errors.AsType[*migrator.ChecksumMismatchError](err); ok {
		return Finding{
			Dimension: DimensionChecksum,
			Severity:  SeverityRefuse,
			Summary: fmt.Sprintf("revision %d records a checksum this directory does not produce",
				mismatch.Version),
			Detail: "the file has changed since it was applied, or the history belongs to a different " +
				"directory: adoption cannot decide which, and will not record either as true",
		}
	}
	if err != nil {
		return Finding{
			Dimension: DimensionChecksum,
			Severity:  SeverityRefuse,
			Summary:   "the recorded checksums could not be verified",
			Detail:    err.Error(),
		}
	}
	if reconcile {
		return Finding{
			Dimension: DimensionChecksum,
			Severity:  SeverityOK,
			Summary:   "the recorded checksums verify against an applied-history projection",
			Detail: "they are atlas.sum running hashes over the migrations that had been applied when each " +
				"row was written; the first native write reconciles them to per-file checksums",
		}
	}
	// Exactly as strong as a clean return: the rule names the first row it can
	// refute, and skips rows it cannot judge -- one with no recorded checksum,
	// one that is not applied. Silence is the absence of a contradiction, and
	// saying more than that would be a claim about rows nothing looked at.
	return Finding{
		Dimension: DimensionChecksum,
		Severity:  SeverityOK,
		Summary:   "no recorded checksum contradicts the migration it names",
	}
}

// semanticsFinding decides the baseline dimension.
//
// A baseline or manually-set row records SQL as applied that this database
// never ran. That is a legitimate thing for a history to contain and it is not
// a defect -- but it is the one fact an operator must know before switching
// writers, because it is the difference between "this schema was built by
// these migrations" and "this schema was declared to match them".
func semanticsFinding(snapshot migrator.MigrationStatusSnapshot) Finding {
	var declared []string
	for _, revision := range snapshot.Revisions {
		if role := declaredRole(revision.AtlasType); role != "" {
			declared = append(declared, fmt.Sprintf("%s (%s)", revisionName(revision), role))
		}
	}
	if len(declared) == 0 {
		return Finding{
			Dimension: DimensionSemantics,
			Severity:  SeverityOK,
			Summary:   "every recorded revision ran its SQL against this database",
		}
	}
	return Finding{
		Dimension: DimensionSemantics,
		Severity:  SeverityAction,
		Summary:   fmt.Sprintf("%d revision(s) are recorded as applied without having run here", len(declared)),
		Detail: "confirm the schema those migrations describe is present before native Ptah trusts the mark: " +
			strings.Join(declared, ", "),
	}
}

// declaredRole names the way a row was recorded as applied without running,
// or "" when it ran.
func declaredRole(atlasType migrator.AtlasRevisionType) string {
	switch {
	case atlasType&migrator.AtlasRevisionTypeManuallySet != 0:
		return "manually set"
	case atlasType&migrator.AtlasRevisionTypeBaseline != 0:
		return "baseline"
	}
	return ""
}

// metadataFinding reports who wrote the rows.
//
// Ptah stamps its own marker in operator_version, and reads meaning back out of
// it: the Atlas layout has no direction column, so a rollback that stopped
// halfway is recorded there. Rows carrying another tool's marker are readable
// -- that is what the compatibility work is for -- and saying so is how an
// operator learns that this history was not written by the tool about to take
// it over.
func metadataFinding(snapshot migrator.MigrationStatusSnapshot) Finding {
	foreign := 0
	for _, revision := range snapshot.Revisions {
		if !writtenByPtah(revision.OperatorVersion) {
			foreign++
		}
	}
	if foreign == 0 {
		return Finding{
			Dimension: DimensionMetadata,
			Severity:  SeverityOK,
			Summary:   "every recorded revision was written by Ptah",
		}
	}
	return Finding{
		Dimension: DimensionMetadata,
		Severity:  SeverityOK,
		Summary:   fmt.Sprintf("%d of %d revision(s) were written by another tool", foreign, len(snapshot.Revisions)),
		Detail: "native Ptah reads them and stamps its own marker on what it writes from here on; " +
			"the rows already there keep the marker they have",
	}
}

// writtenByPtah reports whether an operator_version is one Ptah writes.
func writtenByPtah(operatorVersion string) bool {
	switch strings.TrimSpace(operatorVersion) {
	case revisiontable.PtahOperatorVersion,
		revisiontable.SourceBaselineOperatorVersion,
		revisiontable.SourceIdentityOperatorVersion:
		return true
	}
	return false
}

// revisionName is how a row is named to an operator: its exact persisted
// identity where it has one, because a converted history's rows are told apart
// by a token rather than by the numeric ordering key.
func revisionName(revision migrator.MigrationRevision) string {
	if revision.AtlasVersion != "" {
		return revision.AtlasVersion
	}
	return fmt.Sprintf("%d", revision.Version)
}

// firstLine keeps a driver's multi-line error from taking over a report line.
func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return strings.TrimSpace(line)
}
