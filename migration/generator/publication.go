package generator

// A plan's one publication attempt, and the directory handle it holds until
// that attempt returns. Recovery after a partial write lives here too: the
// handle is what makes the plan a claim on a filesystem object rather than on
// a pathname.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
)

var (
	// ErrMigrationDirectoryChanged reports that migration artifacts changed
	// after the history used for planning or replay was captured and before
	// publication.
	ErrMigrationDirectoryChanged = errors.New(
		"migration directory changed before publication",
	)
	// ErrMigrationPlanInUse reports concurrent publication through one plan.
	ErrMigrationPlanInUse = errors.New("migration plan is already being written")
)

func captureAuthorizedPriorMigrations(fsys fs.FS) (fsnapshot.Snapshot, error) {
	if fsys == nil {
		return fsnapshot.Snapshot{}, nil
	}
	snapshot, err := migrationsnapshot.Capture(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture authorized prior migrations: %w", err)
	}
	return snapshot, nil
}

// recoverMigrationPublication resolves an interrupted publication left by an
// earlier run, before this one starts planning. It resolves the directory by
// pathname because it is the start of its own transaction rather than a step
// inside this one; the levels above it are still created through the confining
// root, so a recovery run cannot materialize directories outside it.
func recoverMigrationPublication(
	ctx context.Context,
	allowedOutputRoot, outputDir string,
) error {
	root, err := openOutputRoot(allowedOutputRoot)
	if err != nil {
		return err
	}
	return errors.Join(
		recoverMigrationPublicationWithin(ctx, root, outputDir),
		closeOutputRoot(root),
	)
}

func recoverMigrationPublicationWithin(
	ctx context.Context,
	root *pathguard.OpenedDirectory,
	outputDir string,
) error {
	if err := atlasmigrate.EnsureMigrationParent(root, outputDir); err != nil {
		return err
	}
	if err := atlasmigrate.RecoverPendingPublication(ctx, outputDir); err != nil {
		return fmt.Errorf("recover migration publication before planning: %w", err)
	}
	return nil
}

// Close releases the plan's claim on the migration directory without
// publishing anything. It is what a caller that decides not to publish should
// use, and it is safe to defer next to PlanMigration: publishing already
// releases the handles, so a Close after WriteFilesContext does nothing.
// Closing twice is likewise a no-op, and Close never reports an error, so it
// composes with defer.
//
// A plan holds the directory open from the moment it is built. Without this
// call an abandoned plan keeps holding it until the garbage collector runs a
// finalizer, and on Windows an open directory handle blocks removing or
// renaming that directory -- so the release point has to be one the caller
// chooses rather than one the runtime chooses (stokaro/ptah#1549).
func (p *MigrationPlan) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.release()
}

// WriteFiles publishes the migration artifacts represented by the plan. A plan
// is single-use after a successful publication.
func (p *MigrationPlan) WriteFiles() (*MigrationFiles, error) {
	return p.WriteFilesContext(context.Background())
}

// WriteFilesContext publishes the migration artifacts represented by the plan.
// The context bounds waiting for the migration-directory publication lock. A
// concurrent call through the same plan returns [ErrMigrationPlanInUse]
// rather than waiting; errors.Is is the supported check.
//
// The plan already holds the migration directory. This call does not reopen it:
// under the lock it revalidates the handle it was given, compares the contents
// against what planning recorded, and publishes through that same handle. A
// directory that no longer matches the recorded snapshot -- and, when
// PriorMigrationsFS was set, the authorized history -- refuses publication
// with [ErrMigrationDirectoryChanged], again for errors.Is.
//
// One call is one use of the plan. Whatever this call returns, it releases the
// migration directory handles before returning, so a failed publication ends
// the plan's hold on the directory at a moment the caller can observe instead
// of at the next garbage collection. A plan whose attempt already happened is
// reported rather than retried: its recorded contents and its chosen version
// both describe a directory as it was before the attempt, so the honest retry
// is a fresh PlanMigration.
//
// The handle, not a pathname, is what settles which directory this commits
// to. Reopening by pathname and comparing an fs.FileInfo captured before any
// handle existed is only as good as the operating system's promise not to
// reissue an identifier, and it makes no such promise: measured on ext4, a
// directory removed and recreated at the same pathname took its inode number
// back in 20 of 20 cycles, so a guard built that way stays silent on exactly
// the substitution an attacker performs most easily (stokaro/ptah#1118).
func (p *MigrationPlan) WriteFilesContext(ctx context.Context) (*MigrationFiles, error) {
	if p == nil {
		return nil, fmt.Errorf("migration plan is nil")
	}
	if !p.mu.TryLock() {
		return nil, ErrMigrationPlanInUse
	}
	defer p.mu.Unlock()
	if p.written {
		return nil, fmt.Errorf("migration plan has already been written")
	}
	if p.closed {
		return nil, fmt.Errorf("migration plan was closed")
	}
	if p.dir == nil {
		return nil, fmt.Errorf("migration plan was released by a failed publication")
	}
	// The plan is single-use, so the handles have no reader left once this call
	// returns -- on the failure paths as much as on the successful one.
	defer p.release()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var files *MigrationFiles
	err := atlasmigrate.WithMigrationDirectoryLock(ctx, p.outputDir, 0, func(context.Context) error {
		published, publishErr := p.publishLocked(ctx)
		files = published
		return publishErr
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// release closes the migration directory handles the plan holds and marks the
// plan spent. It is idempotent and runs with p.mu held.
//
// Deterministic release is the point. os.Root closes its descriptor from a
// finalizer, so an unreleased handle survives until the next collection; on
// Windows that is also the window in which nothing else can rename or remove
// the migration directory, and a failed publication would otherwise leave that
// window open with no event that ends it (stokaro/ptah#1118).
func (p *MigrationPlan) release() {
	if p.dir == nil {
		return
	}
	_ = p.dir.Close()
	p.dir = nil
}

func (p *MigrationPlan) publishLocked(ctx context.Context) (*MigrationFiles, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := p.dir.Revalidate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMigrationDirectoryChanged, err)
	}
	currentContents, err := captureMigrationDirectoryContents(p.dir)
	if err != nil {
		return nil, fmt.Errorf("capture migration directory before publication: %w", err)
	}
	// The contents check is the concurrency half of the guard: another writer
	// that added a migration while this plan was outstanding. Which filesystem
	// object is being committed to was settled by Revalidate above.
	if !p.plannedContents.Equal(currentContents) {
		return nil, ErrMigrationDirectoryChanged
	}
	if err := p.verifyAuthorizedPriorMigrations(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	notifyMigrationPublicationVerified()
	files, err := publishPlannedMigration(ctx, p.dir, p.reportFormat, p.specs)
	if err != nil {
		return nil, fmt.Errorf("error creating migration files: %w", err)
	}
	p.written = true
	return files, nil
}

func (p *MigrationPlan) verifyAuthorizedPriorMigrations() error {
	if !p.hasAuthorizedPriorMigrations {
		return nil
	}
	var currentMigrations fsnapshot.Snapshot
	if p.dir.Exists() {
		fsys, err := p.dir.FS()
		if err != nil {
			return fmt.Errorf("open migration directory before publication: %w", err)
		}
		currentMigrations, err = migrationsnapshot.Capture(fsys)
		if err != nil {
			return fmt.Errorf("capture migration directory before publication: %w", err)
		}
	}
	if !p.authorizedPriorMigrations.Equal(currentMigrations) {
		return ErrMigrationDirectoryChanged
	}
	return nil
}

// captureMigrationDirectoryContents reads the migration directory through the
// bound handle, so what the publication compares is the object it is about to
// commit to rather than whatever the pathname resolves to at comparison time.
//
// A directory the writer bound as absent reads as the empty snapshot. It cannot
// have appeared since -- Revalidate refuses that before this runs -- so the two
// captures either both describe the bound object or both describe nothing.
func captureMigrationDirectoryContents(
	writer *atlasmigrate.MigrationWriter,
) (fsnapshot.Snapshot, error) {
	if !writer.Exists() {
		return fsnapshot.Snapshot{}, nil
	}
	fsys, err := writer.FS()
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	snapshot, err := migrationsnapshot.CaptureStable(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, explainMigrationDirectoryRead(writer, err)
	}
	return snapshot, nil
}

// explainMigrationDirectoryRead names the entries that make a migration
// directory unreadable through the handle the run bound, when that is what went
// wrong. Any other failure is returned unchanged.
//
// The reachable cause is a migration file that is a symbolic link out of the
// migration directory -- a shared migration linked in from elsewhere. Reading
// the directory by pathname followed such a link, and reading it through the
// bound handle does not, so this is a refusal rather than an accident: every
// read, checksum and publication of the directory goes through the object the
// run opened, and a file whose bytes live outside it cannot be part of a
// directory Ptah is willing to seal (stokaro/ptah#1118). A link that resolves
// inside the migration directory stays supported and never reaches this.
//
// The diagnosis runs only after a failed capture, so the successful path pays
// nothing for it.
func explainMigrationDirectoryRead(writer *atlasmigrate.MigrationWriter, cause error) error {
	escaping := escapingMigrationEntries(writer)
	if len(escaping) == 0 {
		return cause
	}
	return fmt.Errorf(
		"migration directory %s: symbolic links resolving outside it: %s;"+
			" a migration file linked in from another directory is refused because"+
			" the whole directory is read, checksummed and published through the"+
			" directory itself: %w",
		writer.Path(),
		strings.Join(escaping, ", "),
		cause,
	)
}

// escapingMigrationEntries lists the migration directory's symbolic links that
// do not resolve inside it, asked through the bound handle: the link itself is
// visible as an entry, and a stat that cannot follow it is the escape.
func escapingMigrationEntries(writer *atlasmigrate.MigrationWriter) []string {
	entries, err := writer.Entries()
	if err != nil {
		return nil
	}
	fsys, err := writer.FS()
	if err != nil {
		return nil
	}
	var escaping []string
	for _, entry := range entries {
		if entry.Type()&fs.ModeSymlink == 0 {
			continue
		}
		if _, statErr := fs.Stat(fsys, entry.Name()); statErr != nil {
			escaping = append(escaping, entry.Name())
		}
	}
	return escaping
}
