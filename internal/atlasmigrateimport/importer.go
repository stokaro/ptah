// Package atlasmigrateimport converts migration directories from
// Atlas-supported tools (golang-migrate, Goose, Flyway, Liquibase, dbmate) to
// Atlas single-file migration layout.
//
// It is the shared format-loading layer for both apply-side and import-side
// Atlas commands. LoadFS converts a caller-owned filesystem snapshot into
// in-memory Atlas single-file entries; LoadDir securely snapshots a local
// directory first. Import writes converted entries plus atlas.sum. Because
// Atlas single-file migrations are up-only, conversion keeps each migration's
// up SQL and drops its down/rollback section, so a converted directory can be
// executed by the Atlas-format migrator without silently changing the source
// tool's semantics.
package atlasmigrateimport

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"go.5x5.cz/ptah/atlascompat"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/migrator"
)

// Format is a supported source migration directory format.
type Format string

const (
	FormatAtlas         Format = "atlas"
	FormatGolangMigrate Format = "golang-migrate"
	FormatGoose         Format = "goose"
	FormatFlyway        Format = "flyway"
	FormatLiquibase     Format = "liquibase"
	FormatDBMate        Format = "dbmate"
)

// Options configures a migration directory import.
type Options struct {
	FromURL   string
	ToURL     string
	DirFormat string
}

// Result describes the files written by Import.
type Result struct {
	Files   []string
	SumFile string
}

type localDirURL struct {
	Dir   string
	Query url.Values
}

// Entry is one migration file converted to Atlas single-file layout. Name is
// the Atlas file name (for example "1_init.sql") and Data is the normalized,
// up-only SQL body.
type Entry struct {
	Name string
	Data []byte
}

// Loaded is a source migration directory read and converted to in-memory Atlas
// single-file entries. It is produced by LoadFS or LoadDir and shared by the
// import (write) path and the apply (in-memory execution) path.
type Loaded struct {
	// Format is the resolved source format the entries were converted from.
	Format Format
	// Dir is the resolved source directory the entries were read from.
	Dir string
	// Entries are the converted Atlas single-file migrations, sorted by name.
	Entries []Entry
}

// FS returns an in-memory fs.FS containing the converted Atlas single-file
// migrations. The result is suitable for migrator.NewFSMigrator with
// migrator.MigrationDirFormatAtlas. It contains no atlas.sum, which is correct:
// external source formats carry no Atlas integrity file, so the Atlas migrator
// derives each revision checksum from the converted up SQL instead.
func (l *Loaded) FS() fs.FS {
	return newMemFS(l.Entries)
}

// Atlas CE identifies a Flyway migration by an opaque version STRING — the
// token between the prefix letter and the "__" separator — and orders the
// directory by that token scored into numeric components. Ptah's migrator
// identifies a migration by an int64, so converting a Flyway directory means
// projecting CE's version space onto int64. The projection below is the whole
// reason the two cannot be identical, and every constant in it was chosen
// against the pinned oracle:
//
//   - CE emits the surviving baseline FIRST, ahead of version order, so a
//     baseline is not orderable against the survivors by its own components.
//     B10__base.sql runs before V2__x.sql. Bands, not arithmetic, carry that.
//   - CE gives EVERY repeatable the empty version, whatever its token: R__a.sql,
//     R1__a.sql and Rfoo.sql all execute as version "". Two of them in one
//     directory is a duplicate CE cannot execute, so one reserved slot at the
//     top is exactly the right shape.
//   - CE distinguishes version tokens that score to the same components —
//     Vx__a.sql and Vy__b.sql both score {0} and both execute, ordered by walk
//     position. No function of a single file can reproduce that, so the
//     ordering key carries a small tie budget filled in walk order.
const (
	// flywayComponentSlot is the fixed width of one trailing version component.
	// 0 encodes "this component is absent" and 1..100 encode the values 0..99.
	// The absent code is what keeps V1 ordered strictly BEFORE V1.0: CE compares
	// components element-wise and then by count, so zero-extending the shorter
	// token would collapse a pair the oracle keeps distinct and ordered.
	flywayComponentSlot = 101
	// flywayMaxComponents is how many components the ordering key carries.
	flywayMaxComponents = 3
	// flywayTieSlots is how many files sharing one ordering key can still be
	// told apart, filled in the walk order CE breaks such ties by. Only the
	// pathological class needs it — tokens that score identically without being
	// equal, such as "x" and "y", or "1" and "01".
	flywayTieSlots = 4
	// flywayBandSize splits int64 into a baseline band and a versioned band, so
	// the baseline sorts below every survivor regardless of its own version.
	flywayBandSize = math.MaxInt64 / 2
	// flywayVersionedBand is where non-baseline versioned migrations start.
	flywayVersionedBand = flywayBandSize
	// flywayRepeatableVersion is the reserved slot every repeatable lands on, at
	// the very top because CE emits repeatables after every versioned file.
	flywayRepeatableVersion = math.MaxInt64
	// flywayMaxTrailingKey is the largest contribution the two trailing slots can
	// make to an ordering key, both holding component 99.
	flywayMaxTrailingKey = (flywayComponentSlot-1)*flywayComponentSlot + (flywayComponentSlot - 1)
	// flywayMaxLeadingComponent is what is left for the leading component once
	// the bands, the trailing slots and the tie budget are taken out. It stays
	// above 99999999999999, so the 14-digit yyyyMMddHHmmss timestamps Flyway
	// projects commonly use are all representable.
	//
	// The bound accounts for the trailing slots and the tie budget rather than
	// only for the band width. Dropping either term lets a leading component
	// just under the limit push the whole key past flywayBandSize, and a
	// versioned migration one band up from there overflows int64 into a negative
	// version — silently, since nothing else range-checks the result.
	flywayMaxLeadingComponent = ((flywayBandSize-flywayTieSlots)/flywayTieSlots-flywayMaxTrailingKey)/
		(flywayComponentSlot*flywayComponentSlot) - 1
)

var (
	golangMigrateFileRe = regexp.MustCompile(`^([0-9]+)_(.+)\.(up|down)\.sql$`)
	golangMigrateLikeRe = regexp.MustCompile(`^[0-9]+_.+\.(up|down)\.sql$`)
	gooseGoFileRe       = regexp.MustCompile(`^[0-9]+_.+\.go$`)
	liquibaseRootRe     = regexp.MustCompile(`(?m)<databaseChangeLog|"databaseChangeLog"|^\s*databaseChangeLog\s*:`)
	numberedSQLFileRe   = regexp.MustCompile(`^[0-9]+_.+\.sql$`)
	unsafeNameRe        = regexp.MustCompile(`[^A-Za-z0-9_.-]`)
)

var liquibaseChangelogExtensions = map[string]bool{
	".json": true,
	".xml":  true,
	".yaml": true,
	".yml":  true,
}

// Import converts a local source migration directory to Atlas single-file
// migrations and writes atlas.sum in the target directory.
func Import(opts Options) (*Result, error) {
	from, err := parseLocalDirURL(defaultString(opts.FromURL, "file://migrations"))
	if err != nil {
		return nil, fmt.Errorf("import --from: %w", err)
	}
	to, err := parseLocalDirURL(defaultString(opts.ToURL, "file://migrations"))
	if err != nil {
		return nil, fmt.Errorf("import --to: %w", err)
	}
	format, err := sourceFormat(from.Query.Get("format"), opts.DirFormat)
	if err != nil {
		return nil, err
	}
	// Importing a directory that is already in the target format is a no-op
	// dressed as work: it copies files and writes a fresh sum over a directory
	// whose previous contents nothing verified.
	//
	// Measured on the pinned community binary, which refuses it with this
	// wording. Both spellings that resolve to atlas -- the default with no
	// format given, and an explicit `?format=atlas` -- exit 1 there and exited
	// 0 here, which is the direction that lets a mistake pass silently.
	if format == FormatAtlas {
		return nil, fmt.Errorf("cannot import a migration directory already in %q format", string(FormatAtlas))
	}

	loaded, err := LoadDir(from.Dir, format)
	if err != nil {
		return nil, err
	}
	entries := loaded.Entries
	if err := os.MkdirAll(to.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create target migration directory %s: %w", to.Dir, err)
	}

	if err := preflightTarget(from.Dir, to.Dir, format, entries); err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		target := filepath.Join(to.Dir, entry.Name)
		// Migration files are committed and shared, so 0644 matches generated
		// migration/sum files elsewhere in Ptah.
		if err := os.WriteFile(target, entry.Data, 0o644); err != nil { //nolint:gosec // migration files are intended to be shared.
			return nil, fmt.Errorf("write imported migration %s: %w", target, err)
		}
		files = append(files, target)
	}
	sum, err := atlascompat.WriteSum(to.Dir, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return nil, err
	}
	sumFile := filepath.Join(to.Dir, atlascompat.AtlasSumFileName)
	if len(sum.Entries) != len(entries) {
		return nil, fmt.Errorf("atlas.sum contains %d entries, want %d", len(sum.Entries), len(entries))
	}
	return &Result{Files: files, SumFile: sumFile}, nil
}

// LoadDir securely snapshots the source migration directory at dir and
// converts it to in-memory Atlas single-file entries. A source that yields no
// importable migration is an error here, because the only caller is the import
// (write) path: importing nothing would write an empty target directory and
// report success.
func LoadDir(dir string, format Format) (*Loaded, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("read source migration directory %s: %w", dir, err)
	}
	snapshot, captureErr := CaptureFS(root.FS(), format)
	closeErr := root.Close()
	if captureErr != nil {
		if closeErr != nil {
			captureErr = errors.Join(captureErr, fmt.Errorf("close source migration directory: %w", closeErr))
		}
		return nil, fmt.Errorf("read source migration directory %s: %w", dir, captureErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close source migration directory: %w", closeErr)
	}
	loaded, err := LoadFS(snapshot, dir, format)
	if err != nil {
		return nil, err
	}
	if len(loaded.Entries) == 0 {
		return nil, fmt.Errorf("no importable migration files found in %s for format %q", dir, format)
	}
	return loaded, nil
}

// CaptureFS captures the files that define a source directory in format:
// the root-level files the converter reads, plus everything the format's Atlas
// integrity file covers. Unsupported Goose and Liquibase inputs remain in the
// immutable snapshot so LoadFS can reject them instead of silently ignoring
// them.
//
// The snapshot is also what an apply-time integrity gate verifies, which is why
// it holds both halves rather than only what conversion consumes. Two files it
// captures are read by nothing else:
//
//   - atlas.sum. A directory written by another migration tool still carries
//     the Atlas integrity file beside its own migrations, and a snapshot
//     without it has nothing to verify against.
//   - For Flyway, nested *.sql files. Flyway is the one format whose covered
//     set reaches below the top level (see [SumFileNames]); Atlas CE hashes
//     sub/V2__nested.sql, so a snapshot missing it would recompute a smaller
//     set than the oracle recorded and refuse a directory Atlas CE applies.
//
// The nested selection is deliberately a superset of the covered set — every
// non-hidden nested *.sql rather than only the V/B/R-prefixed ones. A capture
// that is narrower than the selection fails to read a covered file; a wider one
// only carries bytes nothing hashes.
func CaptureFS(fsys fs.FS, format Format) (fsnapshot.Snapshot, error) {
	if fsys == nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("source migration filesystem is required")
	}
	if format == FormatAtlas {
		return migrationsnapshot.CaptureStable(fsys)
	}
	if err := validateExternalFormat(format); err != nil {
		return fsnapshot.Snapshot{}, err
	}
	include := func(name string, _ fs.DirEntry) bool {
		return captureIncluded(format, name)
	}
	first, err := fsnapshot.CaptureMatching(fsys, include)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	second, err := fsnapshot.CaptureMatching(fsys, include)
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	if !first.Equal(second) {
		return fsnapshot.Snapshot{}, migrationsnapshot.ErrChangedDuringCapture
	}
	return second, nil
}

func validateExternalFormat(format Format) error {
	switch format {
	case FormatGolangMigrate, FormatGoose, FormatFlyway, FormatLiquibase, FormatDBMate:
		return nil
	case FormatAtlas:
		return nil
	default:
		return fmt.Errorf("unknown migration import format %q", format)
	}
}

// captureIncluded reports whether a source path belongs in the captured
// snapshot of a directory read as format. name is a slash path relative to the
// directory root.
func captureIncluded(format Format, name string) bool {
	if path.Dir(name) != "." {
		return sumCoversNestedFile(format, name)
	}
	if name == atlascompat.AtlasSumFileName {
		return true
	}
	return sourceExtensionIncluded(format, strings.ToLower(path.Ext(name)))
}

func sourceExtensionIncluded(format Format, extension string) bool {
	switch format {
	case FormatGoose:
		return extension == ".sql" || extension == ".go"
	case FormatLiquibase:
		return extension == ".sql" || liquibaseChangelogExtensions[extension]
	case FormatGolangMigrate, FormatFlyway, FormatDBMate:
		return extension == ".sql"
	case FormatAtlas:
		return false
	}
	return false
}

// LoadFS converts one caller-owned migration filesystem into in-memory Atlas
// single-file entries for the given format. It never opens a pathname, writes
// to disk, or opens a database connection. Unknown formats, malformed layouts,
// and unsupported migrations return an error.
//
// A source with NOTHING TO CONVERT is not an error: it returns a Loaded with no
// entries, which executes as "No migration files to execute", exit 0, matching
// the pinned community binary v1.3.0 on an empty directory, one holding only a
// README, and one whose only SQL sits in a subdirectory — with and without
// atlas.sum, all six rows measured (stokaro/ptah#980).
//
// "Nothing to convert" is keyed on the COVERED SET being empty, not on the
// converted entries being empty, and the difference is the whole point. The two
// are not the same predicate: a Goose directory holding only `foo.sql` has a
// covered set of exactly that file — the community binary applies it, as version
// "foo" — while Ptah's converter produces no entry for it. Keying on the entries
// would report "nothing to execute" and exit 0 for that directory, replacing a
// loud refusal with a silent no-op that skips a migration the source tool runs.
// Measured: on that directory the community binary creates the table and Ptah
// must not claim there was nothing to do. So a covered set that is non-empty
// keeps the refusal below.
//
// [LoadDir], which feeds the import (write) path, refuses an empty result
// outright: importing nothing would write an empty target and report success.
func LoadFS(fsys fs.FS, display string, format Format) (*Loaded, error) {
	if fsys == nil {
		return nil, fmt.Errorf("source migration filesystem is required")
	}
	entries, err := loadEntries(fsys, display, format)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		covered, err := SumFileNames(fsys, format)
		if err != nil {
			return nil, err
		}
		if len(covered) > 0 {
			return nil, fmt.Errorf("no importable migration files found in %s for format %q", display, format)
		}
	}
	// External formats convert each source file to a single up-only Atlas
	// migration whose version is the leading number of the file name. Distinct
	// source names can still collapse to the same Atlas version (for example
	// goose 1_x.sql and 01_x.sql), which the Atlas migrator would otherwise only
	// reject after opening the database. Catch it here, before any database work.
	// The native Atlas format is skipped: it legitimately pairs up/down files
	// under one version and is validated by the migrator's own loader.
	if format != FormatAtlas {
		if err := checkDuplicateConvertedVersions(entries); err != nil {
			return nil, err
		}
	}
	return &Loaded{Format: format, Dir: display, Entries: entries}, nil
}

func checkDuplicateConvertedVersions(entries []Entry) error {
	seen := make(map[int64]string, len(entries))
	for _, entry := range entries {
		file, err := migrator.ParseAtlasMigrationFileName(entry.Name)
		if err != nil {
			return fmt.Errorf("converted migration %s is not a valid Atlas migration file name: %w", entry.Name, err)
		}
		if prev, ok := seen[file.Version]; ok {
			return fmt.Errorf("migration files %s and %s map to the same version %d", prev, entry.Name, file.Version)
		}
		seen[file.Version] = entry.Name
	}
	return nil
}

func preflightTarget(fromDir, toDir string, format Format, entries []Entry) error {
	if sameLocalDir(fromDir, toDir) {
		return fmt.Errorf("import --to must be different from --from for format %q", format)
	}
	if err := ensureEmptyMigrationTarget(toDir); err != nil {
		return err
	}

	seenNames := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.Name == "" {
			return fmt.Errorf("import produced an empty migration file name")
		}
		if _, ok := seenNames[entry.Name]; ok {
			return fmt.Errorf("import produced duplicate migration file name %s", entry.Name)
		}
		seenNames[entry.Name] = struct{}{}
	}
	return nil
}

func ensureEmptyMigrationTarget(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read target migration directory %s: %w", dir, err)
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		switch {
		case file.Name() == atlascompat.AtlasSumFileName:
			return fmt.Errorf("target migration directory already contains %s: %s", atlascompat.AtlasSumFileName, filepath.Join(dir, file.Name()))
		case filepath.Ext(file.Name()) == ".sql":
			return fmt.Errorf("target migration directory already contains SQL file: %s", filepath.Join(dir, file.Name()))
		}
	}
	return nil
}

func sameLocalDir(a, b string) bool {
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	if errA == nil && errB == nil {
		return filepath.Clean(absA) == filepath.Clean(absB)
	}
	return filepath.Clean(a) == filepath.Clean(b)
}

func parseLocalDirURL(raw string) (localDirURL, error) {
	base, rawQuery, _ := strings.Cut(raw, "?")
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return localDirURL{}, err
	}
	if strings.Contains(base, "://") && !strings.HasPrefix(base, "file://") {
		return localDirURL{}, fmt.Errorf("only local file:// migration directories are supported")
	}
	dir := strings.TrimPrefix(base, "file://")
	if dir == "" {
		dir = "."
	}
	dir, err = url.PathUnescape(dir)
	if err != nil {
		return localDirURL{}, err
	}
	return localDirURL{Dir: filepath.Clean(dir), Query: query}, nil
}

func sourceFormat(queryFormat, dirFormat string) (Format, error) {
	value := strings.TrimSpace(queryFormat)
	if value == "" {
		value = strings.TrimSpace(dirFormat)
	}
	if value == "" {
		value = string(FormatAtlas)
	}
	format := Format(strings.ToLower(value))
	switch format {
	case FormatAtlas, FormatGolangMigrate, FormatGoose, FormatFlyway, FormatLiquibase, FormatDBMate:
		return format, nil
	default:
		return "", fmt.Errorf("unknown migration import format %q", value)
	}
}

func loadEntries(fsys fs.FS, display string, format Format) ([]Entry, error) {
	files, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source migration directory %s: %w", display, err)
	}
	switch format {
	case FormatAtlas:
		return loadAtlasEntries(fsys, files)
	case FormatGolangMigrate:
		return loadGolangMigrateEntries(fsys, files)
	case FormatGoose:
		return loadGooseEntries(fsys, files)
	case FormatDBMate:
		return loadDirectiveSectionEntries(fsys, files, dbmateUpSQL)
	case FormatLiquibase:
		return loadLiquibaseEntries(fsys, files)
	case FormatFlyway:
		// Flyway does not get the top-level listing: it is the one layout Atlas
		// CE recurses into, and its selection is a state machine over the walk.
		// See loadFlywayEntries.
		return loadFlywayEntries(fsys)
	default:
		return nil, fmt.Errorf("unknown migration import format %q", format)
	}
}

func loadGooseEntries(fsys fs.FS, files []fs.DirEntry) ([]Entry, error) {
	for _, file := range files {
		if !file.IsDir() && gooseGoFileRe.MatchString(file.Name()) {
			return nil, fmt.Errorf("Go-based Goose migration %q is not supported (SQL migrations only)", file.Name())
		}
	}
	return loadDirectiveSectionEntries(fsys, files, gooseUpSQL)
}

func loadLiquibaseEntries(fsys fs.FS, files []fs.DirEntry) ([]Entry, error) {
	var changelogFiles []string
	for _, file := range files {
		if file.IsDir() || !liquibaseChangelogExtensions[strings.ToLower(filepath.Ext(file.Name()))] {
			continue
		}
		data, err := fs.ReadFile(fsys, file.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration file %s: %w", file.Name(), err)
		}
		if liquibaseRootRe.Match(data) {
			changelogFiles = append(changelogFiles, file.Name())
		}
	}
	if len(changelogFiles) > 0 {
		return nil, fmt.Errorf(
			"liquibase XML/YAML/JSON changelogs are not yet supported (only formatted-SQL changelogs beginning with %q); found %s",
			"--liquibase formatted sql",
			strings.Join(changelogFiles, ", "),
		)
	}
	return loadDirectiveSectionEntries(fsys, files, liquibaseSQL)
}

func loadAtlasEntries(fsys fs.FS, files []fs.DirEntry) ([]Entry, error) {
	var entries []Entry
	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".sql" {
			continue
		}
		data, err := readImportSQLFile(fsys, file.Name())
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Name: file.Name(), Data: data})
	}
	sortEntries(entries)
	return entries, nil
}

func loadGolangMigrateEntries(fsys fs.FS, files []fs.DirEntry) ([]Entry, error) {
	var entries []Entry
	for _, file := range files {
		match := golangMigrateFileRe.FindStringSubmatch(file.Name())
		if file.IsDir() {
			continue
		}
		if match == nil {
			if strings.HasSuffix(file.Name(), ".sql") && golangMigrateLikeRe.MatchString(file.Name()) {
				return nil, fmt.Errorf("unsupported golang-migrate migration file name %s", file.Name())
			}
			continue
		}
		if match[3] != "up" {
			continue
		}
		data, err := readImportSQLFile(fsys, file.Name())
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Name: match[1] + "_" + match[2] + ".sql", Data: data})
	}
	sortEntries(entries)
	return entries, nil
}

// loadDirectiveSectionEntries reads directive-sectioned migration files (goose,
// dbmate, liquibase) and keeps only their up SQL. extract owns the whole
// decision: it returns the up SQL, or an error naming why the file cannot be
// converted. Each format's rules differ enough that a shared "is the marker
// present" flag could not express them — see [gooseUpSQL] and [dbmateUpSQL].
//
// extract receives the file's RAW bytes, not normalized ones. That matters for
// the one case where the community binary preserves a file verbatim (a goose
// file carrying no directives): normalizing before extraction would drop blank
// lines between statements, changing the converted bytes and therefore the
// atlas.sum entry, so the two tools would disagree about a directory both call
// clean. Every extractor normalizes what it returns.
//
// An empty up section is an entry, not a skip. An intentionally empty migration
// is a legitimate thing to write, and the community binary records it as an
// applied revision with 0 statements; dropping it here removed it from the
// converted directory AND from atlas_schema_revisions while still exiting 0,
// so a later run would never apply it. Measured on goose, dbmate and liquibase
// alike (community binary v1.3.0).
func loadDirectiveSectionEntries(
	fsys fs.FS,
	files []fs.DirEntry,
	extract func(name string, data []byte) ([]byte, error),
) ([]Entry, error) {
	var entries []Entry
	for _, file := range files {
		if file.IsDir() || !numberedSQLFileRe.MatchString(file.Name()) {
			continue
		}
		data, err := readRawImportSQLFile(fsys, file.Name())
		if err != nil {
			return nil, err
		}
		up, err := extract(file.Name(), data)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Name: file.Name(), Data: up})
	}
	sortEntries(entries)
	return entries, nil
}

// loadFlywayEntries converts exactly the files a Flyway directory's atlas.sum
// covers, in the order Atlas CE executes them.
//
// It shares [flywayCoveredFiles] with [SumFileNames] rather than filtering the
// directory itself, which is what makes "everything executed was covered by the
// checksum that was verified" a structural property instead of a claim two
// separate rules have to keep agreeing on. #982 is what the claim was worth: a
// superseded baseline and a lowercase-prefixed file both executed on a
// directory whose atlas.sum covered neither, and which both tools called clean.
//
// The remaining work is projection, not selection. Atlas CE keys a migration on
// an opaque version STRING, Ptah's migrator on an int64, and the mapping between
// them is the band-and-slot encoding documented above flywayComponentSlot. It
// refuses rather than truncates when a version cannot be represented, because
// silently reusing a slot would execute a migration under another one's
// identity.
func loadFlywayEntries(fsys fs.FS) ([]Entry, error) {
	covered, err := flywayCoveredFiles(fsys)
	if err != nil {
		return nil, err
	}

	if err := rejectDuplicateFlywayVersions(covered); err != nil {
		return nil, err
	}

	versions, err := flywayConvertedVersions(covered)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, 0, len(covered))
	for i, file := range covered {
		data, err := readImportSQLFile(fsys, file.name)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Name: flywayEntryName(versions[i], file.description), Data: data})
	}
	return entries, nil
}

// flywayConvertedVersions assigns each covered file its int64 Atlas version,
// positionally aligned with covered. It is the single place the tie index is
// counted, so the versions the importer executes under and the versions
// [LegacyFlywayAtlasVersions] reports cannot disagree.
func flywayConvertedVersions(covered []flywaySumFile) ([]int64, error) {
	versions := make([]int64, len(covered))
	var previous *flywaySumFile
	tie := 0
	for i := range covered {
		file := covered[i]
		// Files whose tokens score to the same components arrive adjacent, in
		// the walk order CE ranks them by, because flywaySumFiles stable-sorts
		// the survivors. So the tie index is the position within that run.
		if previous != nil && file.kind == previous.kind && slices.Equal(file.components, previous.components) {
			tie++
		} else {
			tie = 0
		}
		previous = &covered[i]

		version, err := flywayAtlasVersion(file, tie)
		if err != nil {
			return nil, err
		}
		versions[i] = version
	}
	return versions, nil
}

// rejectDuplicateFlywayVersions refuses two covered files that carry the same
// Atlas CE version. Atlas CE does not report such a directory: it executes the
// migrations up to the collision and then panics with an index-out-of-range, so
// V1__a.sql beside V1__b.sql leaves table a behind and exits 2. Refusing before
// the database is touched is strictly safer, not merely different.
//
// The check is on CE's version STRING, and that operand is the whole point.
// Scoring the tokens into components instead would merge V1.5__a.sql with
// V1_5__b.sql and Vx__a.sql with Vy__b.sql, both of which CE runs — two pairs
// the previous implementation refused. Comparing the strings separates
// "identical to Atlas CE" from "merely ordered identically", and only the
// second one is a tie for flywayAtlasVersion to break.
func rejectDuplicateFlywayVersions(covered []flywaySumFile) error {
	seen := make(map[string]string, len(covered))
	for _, file := range covered {
		version := flywayCEVersion(file)
		if previous, ok := seen[version]; ok {
			if version == "" {
				// Reached by two repeatables, and by a repeatable beside a file
				// whose own token is empty such as V.sql, so the message names
				// the empty version rather than either file's role.
				return fmt.Errorf("Flyway migrations %s and %s both carry the empty Atlas version and cannot be executed together", previous, file.name)
			}
			return fmt.Errorf("Flyway migrations %s and %s both carry the Atlas version %q", previous, file.name, version)
		}
		seen[version] = file.name
	}
	return nil
}

// flywayCEVersion is the version string Atlas CE gives a covered file. A
// repeatable gets the empty string whatever its own token: R__a.sql, R1__a.sql
// and Rfoo.sql all execute as version "", which is why two repeatables collide
// with each other and with a file whose token is genuinely empty, such as V.sql.
func flywayCEVersion(file flywaySumFile) string {
	if file.kind == flywaySumRepeatable {
		return ""
	}
	return file.version
}

// FlywayMigrationVersion pairs a Flyway source file with the int64 Atlas
// version it converts to.
//
// Both halves are needed by every caller: the version is what a revision row
// can be tested against, and the file name is the only one of the two an
// operator can find in their directory. A refusal that names 4611686018427510315
// tells them nothing they can act on.
type FlywayMigrationVersion struct {
	// Source is the file name, relative to the migration directory.
	Source string
	// Version is the Atlas version that file converts to.
	Version int64
}

// FlywayBaseline describes the surviving baseline of a Flyway directory.
type FlywayBaseline struct {
	// Source is the baseline file name, relative to the migration directory.
	Source string
	// Version is the version token Atlas CE keys the file on: "10" for
	// B10__base.sql. It is the operand CE's own pending-ness comparison uses,
	// so the refusal quotes it rather than the int64 below.
	Version string
	// AtlasVersion is the converted version the entry executes and records
	// under. It sits in the band below every survivor.
	AtlasVersion int64
	// SameVersion are the migrations in this directory whose version TOKEN is
	// the baseline's own, byte for byte, each paired with the Atlas version a
	// database that ran it recorded. It answers "has a migration of this very
	// version already been applied here?", which the baseline's own band
	// position cannot. See flywaySameVersionMigrations for why the operand is
	// the token and not the ordering key.
	SameVersion []FlywayMigrationVersion
	// Covered are every OTHER covered file and its converted version, so a
	// caller can ask which of them a database has already recorded.
	//
	// Leaving the baseline itself out is a statement of the contract rather
	// than something a caller can currently observe, and no mutation covers it:
	// the apply path only asks this about versions a database has recorded, and
	// it asks only when the baseline is still pending, so including it would
	// answer the same either way. It becomes observable the moment a caller
	// asks about a baseline that has already run.
	Covered []FlywayMigrationVersion
}

// FlywaySurvivingBaseline returns the surviving baseline of a Flyway directory,
// or nil when it holds none.
//
// The caller is the apply path, and it needs this for two decisions. The
// converted version encodes Atlas CE's SUM order, in which a surviving baseline
// comes FIRST whatever its own version — measured, and measured to hold across
// runs, not only within one conversion. So the baseline is placed in a band
// below every survivor, and on a database that already has migrations recorded
// it therefore sorts below all of them. That LOOKS like a migration authored
// before what is already applied, and it is not: it is an artifact of projecting
// a sum order onto an int64, which is why the version is exempted from the
// linear guard. Whether the baseline may then run at all is a second question,
// answered by checkFlywayBaselineHistory in cmd/atlas.
//
// It shares flywaySumFiles and flywayConvertedVersions with the importer, so
// the version reported here is the one the entry is actually executed under. The
// walk is done here rather than through flywayCoveredFiles because the raw names
// are needed a second time, to re-run the selection without this baseline.
func FlywaySurvivingBaseline(fsys fs.FS, format Format) (*FlywayBaseline, error) {
	if format != FormatFlyway {
		return nil, nil
	}
	names, err := treeNames(fsys, skipHiddenDir)
	if err != nil {
		return nil, err
	}
	covered := flywaySumFiles(names)
	versions, err := flywayConvertedVersions(covered)
	if err != nil {
		return nil, err
	}
	for i, file := range covered {
		if file.kind != flywaySumBaseline {
			continue
		}
		// flywaySumFiles emits at most one baseline, and always first.
		return &FlywayBaseline{
			Source:       file.name,
			Version:      file.version,
			AtlasVersion: versions[i],
			SameVersion:  flywaySameVersionMigrations(names, file),
			Covered:      flywayMigrationVersions(slices.Concat(covered[:i], covered[i+1:]), slices.Concat(versions[:i], versions[i+1:])),
		}, nil
	}
	return nil, nil
}

// flywaySameVersionMigrations reports the migrations in this directory that
// carry the baseline's OWN version token, and the Atlas version each converts to
// when the baseline is not there to squash it.
//
// IDENTITY AND ORDER ARE DIFFERENT QUESTIONS, and this is the identity one. Do
// not answer it with flywayOrderingKey. That key deliberately collapses tokens
// that must ORDER together — "2", "02" and "002" all score to components {2},
// which is correct, because Atlas CE runs them in that one position, and the tie
// budget in flywayAtlasVersion exists precisely because tokens that score
// identically still have to be told apart by walk position. Projecting the
// baseline's token through that key and comparing the result to a revision row
// asks "does this sort where something applied sorts?" while the caller needs
// "is this the same migration?". Atlas CE answers identity by comparing the
// version TOKENS as strings — measured, its own stdout for V02 applied plus
// B2__base.sql is `Migrating to version 2 from 02`, and it executes the baseline
// — and so does the comparison below.
//
// The domain is this directory WITHOUT the surviving baseline, because that is
// the only shape a revision row can have come from. A versioned file carrying
// the baseline's exact token is squashed out of the covered set by construction
// (flywaySumFiles drops it on `file.version <= baseline.version`, which holds on
// equality), so its converted version is not among the covered ones; re-running
// the selection with the baseline removed puts it back at the tie slot it was
// recorded under, rather than assuming slot 0.
//
// A pre-baseline directory this build cannot convert yields nothing, because no
// build could have applied it either and so no revision row of its making
// exists. Reporting the conversion error instead would fail an apply over a file
// set that is not the one being executed.
func flywaySameVersionMigrations(names []string, baseline flywaySumFile) []FlywayMigrationVersion {
	before := flywaySumFiles(slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		return name == baseline.name
	}))
	sameVersion := func(file flywaySumFile) bool {
		return file.kind == flywaySumVersioned && file.version == baseline.version
	}
	if !slices.ContainsFunc(before, sameVersion) {
		return nil
	}
	versions, err := flywayConvertedVersions(before)
	if err != nil {
		return nil
	}

	var out []FlywayMigrationVersion
	for i, file := range before {
		if sameVersion(file) {
			out = append(out, FlywayMigrationVersion{Source: file.name, Version: versions[i]})
		}
	}
	return out
}

// flywayMigrationVersions zips a covered selection with its converted versions.
func flywayMigrationVersions(files []flywaySumFile, versions []int64) []FlywayMigrationVersion {
	out := make([]FlywayMigrationVersion, len(files))
	for i, file := range files {
		out[i] = FlywayMigrationVersion{Source: file.name, Version: versions[i]}
	}
	return out
}

// FlywaySourceVersions reports the Flyway version TOKEN every converted
// migration was projected from, keyed by the int64 Atlas version it executes
// and is recorded under. It returns nil for every other layout.
//
// It exists because two different comparisons decide two different questions
// about the same directory, and Ptah answered both with the first
// (stokaro/ptah#1098):
//
//   - ORDER, for atlas.sum and for execution, is NUMERIC on the version
//     components: V2 runs before V10. [flywayOrderingKey] reproduces exactly
//     that, and it is measured to match — a fresh apply of a directory holding
//     V2__a.sql and V10__b.sql runs version 2 then version 10 on the pinned
//     community binary v1.3.0.
//   - LINEARITY, "was this file added after everything already applied", is
//     decided on the version token as a STRING. `"10" < "2"`, so a V10 added
//     beside an applied V2 is refused on the same binary with
//     `migration file V10__y.sql was added out of order`.
//
// Keying the linear guard on the ordering int64 answers the second question
// with the first one's answer, which is how a Flyway project's tenth migration
// came to apply here at exit 0 where the oracle exits 1. The token is therefore
// carried ALONGSIDE the executed version rather than replacing it: renumbering
// would strand every revision row already recorded, which is a cost
// [LegacyFlywayAtlasVersions] exists to document.
//
// The value is Atlas CE's version string for the file, so a repeatable reports
// the empty string whatever its own token — see [flywayCEVersion]. That is not
// a gap in the map: CE compares the empty string like any other token, which is
// why it refuses an `R__r.sql` added to a database that has already applied
// `V2__x.sql`, and reproducing the token faithfully reproduces that answer too.
//
// It shares flywayCoveredFiles and flywayConvertedVersions with the importer,
// so the token reported for a version belongs to the entry actually executed
// under it.
func FlywaySourceVersions(fsys fs.FS, format Format) (map[int64]string, error) {
	if format != FormatFlyway {
		return nil, nil
	}
	covered, err := flywayCoveredFiles(fsys)
	if err != nil {
		return nil, err
	}
	versions, err := flywayConvertedVersions(covered)
	if err != nil {
		return nil, err
	}
	sources := make(map[int64]string, len(covered))
	for i, file := range covered {
		sources[versions[i]] = flywayCEVersion(file)
	}
	return sources, nil
}

// flywayEntryName renders the converted Atlas single-file name. A Flyway file
// need not carry a description at all — V1.sql and Video.sql are both ordinary
// migrations to Atlas CE — so the name degrades to the bare version rather than
// growing a trailing separator with nothing after it.
func flywayEntryName(version int64, description string) string {
	if name := sanitizeName(description); name != "" {
		return fmt.Sprintf("%d_%s.sql", version, name)
	}
	return fmt.Sprintf("%d.sql", version)
}

// flywayAtlasVersion projects one covered Flyway file onto the int64 Atlas
// version Ptah's migrator orders and identifies migrations by. tie is the file's
// position among the covered files that share its ordering key.
//
// The projection is a function of the file alone except for tie, which is a
// function of its position. That is not a shortcut: CE itself separates such
// files only by walk position, so no per-file rule could reproduce it. Every
// file whose token scores uniquely — which is every file in a directory using
// ordinary numeric versions — keeps tie 0 and therefore a version that does not
// move when a sibling is added, so inserting a mid-sequence migration does not
// renumber the others and recorded revisions stay valid.
func flywayAtlasVersion(file flywaySumFile, tie int) (int64, error) {
	if file.kind == flywaySumRepeatable {
		// Every repeatable is version "" to Atlas CE regardless of its token, so
		// they all land on the one reserved slot and a second one is caught as a
		// collision below — which is what CE's own version space says it is.
		return flywayRepeatableVersion, nil
	}
	if tie >= flywayTieSlots {
		return 0, fmt.Errorf(
			"Flyway migration %s shares its version ordering key with more than the %d files Ptah can tell apart (version %q)",
			file.name, flywayTieSlots, file.version,
		)
	}

	key, err := flywayOrderingKey(file)
	if err != nil {
		return 0, err
	}
	version := key*flywayTieSlots + int64(tie)
	if file.kind == flywaySumBaseline {
		// The surviving baseline is emitted, and executed, ahead of every
		// survivor whatever its own version: CE runs B10__base.sql before
		// V2__x.sql. A band keeps that true without making the baseline's
		// version depend on the files around it.
		return version, nil
	}
	return flywayVersionedBand + version, nil
}

// flywayOrderingKey packs a version token's components into one integer that
// compares the way compareFlywaySumVersions compares the components themselves.
func flywayOrderingKey(file flywaySumFile) (int64, error) {
	components := file.components
	if len(components) > flywayMaxComponents {
		return 0, fmt.Errorf(
			"Flyway migration %s has version %q with more than %d components and cannot map to an int64 Atlas version",
			file.name, file.version, flywayMaxComponents,
		)
	}
	// A negative component orders below every ordinary version on the oracle
	// (V-5__x.sql runs before V2__two.sql) and there is no room below zero in a
	// positive fixed-width slot.
	if components[0] < 0 {
		return 0, fmt.Errorf("Flyway migration %s has version %q with a negative component and cannot map to an int64 Atlas version", file.name, file.version)
	}
	if components[0] > flywayMaxLeadingComponent {
		return 0, fmt.Errorf("Flyway migration %s has version %q that is too large to map to an int64 Atlas version", file.name, file.version)
	}

	key := components[0] + 1
	for i := 1; i < flywayMaxComponents; i++ {
		slot := int64(0)
		if i < len(components) {
			if components[i] < 0 || components[i] >= flywayComponentSlot-1 {
				return 0, fmt.Errorf(
					"Flyway migration %s has version %q whose component %d (%d) is outside 0..%d and cannot map to an int64 Atlas version",
					file.name, file.version, i+1, components[i], flywayComponentSlot-2,
				)
			}
			slot = components[i] + 1
		}
		key = key*flywayComponentSlot + slot
	}
	return key, nil
}

func readImportSQLFile(fsys fs.FS, name string) ([]byte, error) {
	data, err := readRawImportSQLFile(fsys, name)
	if err != nil {
		return nil, err
	}
	return normalizeSQL(data), nil
}

// readRawImportSQLFile reads a source migration without normalizing it, for the
// callers that must decide normalization for themselves. See
// [loadDirectiveSectionEntries].
func readRawImportSQLFile(fsys fs.FS, name string) ([]byte, error) {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("read source migration %s: %w", name, err)
	}
	return data, nil
}

// goosePragma is one of the four goose directives that change which section the
// parser is in.
//
// "NO TRANSACTION" is deliberately absent, and so is every other "-- +goose ..."
// line. Goose leaves them in the migration body, and because they cannot open or
// close a section, recognizing them would change nothing: measured on the
// community binary v1.3.0, both "-- +goose NO TRANSACTION" and the meaningless
// "-- +goose Frobnicate" survive into the SQL it executes, whether they sit above
// the Up directive or inside the up section.
type goosePragma string

const (
	goosePragmaUp             goosePragma = "Up"
	goosePragmaDown           goosePragma = "Down"
	goosePragmaStatementBegin goosePragma = "StatementBegin"
	goosePragmaStatementEnd   goosePragma = "StatementEnd"
)

// goosePragmaPrefix is matched case-sensitively, including its single trailing
// space. See [goosePragmaOf] for why the spacing is load-bearing.
const goosePragmaPrefix = "-- +goose "

var goosePragmas = []goosePragma{
	goosePragmaUp,
	goosePragmaDown,
	goosePragmaStatementBegin,
	goosePragmaStatementEnd,
}

// gooseSection is which directive section the parser is currently inside.
type gooseSection int

const (
	gooseSectionNone gooseSection = iota
	gooseSectionUp
	gooseSectionDown
)

// goosePragmaOf reports the section-changing goose directive a line carries.
//
// The spelling rules are the community binary's, and each was measured rather
// than assumed, because every one of them decides whether a line is a directive
// or executable SQL:
//
//   - surrounding whitespace is ignored, so "  -- +goose Up  " is the Up directive;
//   - the prefix is case-sensitive, so "-- +Goose Up" is NOT a directive;
//   - the name runs to the first space, so "-- +goose Up extra" IS the Up directive;
//   - exactly one space follows "+goose", so "-- +goose  Up" is NOT a directive;
//   - the name itself is case-sensitive, so "-- +goose up" is NOT a directive.
//
// The last two are the dangerous ones: the community binary silently treats such
// a line as a comment and folds the SQL under it into the up migration. See
// [gooseNearMissPragma].
func goosePragmaOf(line string) (goosePragma, bool) {
	rest, ok := gooseDirectiveRemainder(strings.TrimSpace(line))
	if !ok {
		return "", false
	}
	name, _, _ := strings.Cut(rest, " ")
	name = strings.TrimSpace(name)
	for _, pragma := range goosePragmas {
		if string(pragma) == name {
			return pragma, true
		}
	}
	return "", false
}

// gooseNearMissPragma reports the section directive a line was evidently meant to
// be but misspells — "-- +goose down" for Down, "-- +goose  Up" for Up.
//
// This is a DELIBERATE DIVERGENCE, and the reason is that the community binary's
// handling of these lines is a defect rather than a decision. It does not
// recognize the typo, folds the line into the migration body as a comment, and
// then executes everything below it. Measured on v1.3.0: a file whose only fault
// is a lowercase "-- +goose down" has its table created and then DROPPED by its
// own rollback section, and the migration is recorded as successfully applied.
// A case error in a directive should not silently roll back a migration, so Ptah
// refuses. Refusing is stricter than the community binary, which exits 0 here, so
// this cannot make ptah-compat accept anything the community binary rejects.
//
// The guard is scoped to the four section-changing names, and requires the
// remainder to be exactly the name: "-- +goose Frobnicate" stays a comment
// because it cannot change sections, and "-- +goose up to date" stays a comment
// because it reads as prose rather than a mistyped directive. Refusing those
// would reject files the community binary runs safely, for no benefit.
func gooseNearMissPragma(line string) (goosePragma, bool) {
	rest, ok := gooseDirectiveRemainderFold(strings.TrimSpace(line))
	if !ok {
		return "", false
	}
	name := strings.TrimSpace(rest)
	for _, pragma := range goosePragmas {
		if strings.EqualFold(string(pragma), name) {
			return pragma, true
		}
	}
	return "", false
}

// gooseUpSQL extracts the up section of a goose migration.
//
// It is a state machine over the directive set above, not a line filter, because
// the community binary is one too and the difference is observable in both
// directions. Its accept/reject set was measured on v1.3.0 and is reproduced
// here:
//
//   - Up is rejected while already inside an up section (a second Up), but
//     accepted after a Down — "Up, Down, Up" is a file the community binary runs.
//   - Down is rejected before any Up has opened a section, but accepted after
//     another Down.
//   - StatementBegin is rejected outside a section; an unterminated block is fine.
//   - StatementEnd is rejected unless a StatementBegin is open.
//   - Inside a StatementBegin block only StatementEnd is accepted; a directive of
//     any other kind there is an error rather than passed-through body text.
//
// Each of those rejections is a file ptah-compat used to accept and execute, so
// this is where the never-looser half of the parity rule is paid.
//
// The up body runs from the FILE START through the first Down, minus the
// directive lines themselves. Starting at the file start is what stops SQL
// written above the Up directive from being silently dropped; stopping at the
// first Down is the community binary's own reading, so the second up section of
// an "Up, Down, Up" file is not executed.
//
// A file carrying no section directive at all is NOT an error: its whole body is
// the migration, byte for byte. That is #981. Such a file has no rollback section
// that could leak onto the apply path, so the caution that governs a MALFORMED
// directive set does not apply to a file that simply has none, and the community
// binary executes it, records it honestly, and drops nothing.
func gooseUpSQL(name string, data []byte) ([]byte, error) {
	var body []string
	section := gooseSectionNone
	inStatement := false
	collecting := true

	for i, line := range strings.Split(string(data), "\n") {
		lineNo := i + 1
		pragma, ok := goosePragmaOf(line)
		if !ok {
			if near, isNear := gooseNearMissPragma(line); isNear {
				return nil, fmt.Errorf(
					"migration file %s line %d: %q is not a goose directive because directive names are case- and space-sensitive; "+
						"as written it is an ordinary comment and the SQL below it would be executed as part of the up migration. Write %q instead",
					name, lineNo, strings.TrimSpace(line), goosePragmaPrefix+string(near),
				)
			}
			if collecting {
				body = append(body, line)
			}
			continue
		}
		if inStatement {
			// A StatementBegin block ends only at StatementEnd. Any other
			// directive inside one is an error, not body text.
			if pragma != goosePragmaStatementEnd {
				return nil, gooseUnexpectedPragma(name, lineNo, pragma, "it appears inside a \"-- +goose StatementBegin\" block")
			}
			inStatement = false
			continue
		}
		switch pragma {
		case goosePragmaUp:
			if section == gooseSectionUp {
				return nil, gooseUnexpectedPragma(name, lineNo, pragma, "an up section is already open")
			}
			section = gooseSectionUp
		case goosePragmaDown:
			if section == gooseSectionNone {
				return nil, gooseUnexpectedPragma(name, lineNo, pragma, "no up section has been opened yet")
			}
			section = gooseSectionDown
			collecting = false
		case goosePragmaStatementBegin:
			if section == gooseSectionNone {
				return nil, gooseUnexpectedPragma(name, lineNo, pragma, "no up section has been opened yet")
			}
			inStatement = true
		case goosePragmaStatementEnd:
			return nil, gooseUnexpectedPragma(name, lineNo, pragma, "no \"-- +goose StatementBegin\" block is open")
		}
	}

	if section == gooseSectionNone {
		// No section directive appeared anywhere: the file IS the migration.
		// Reached only when nothing errored above, and Down, StatementBegin and
		// StatementEnd all error while the section is None, so this really does
		// mean "carries no goose directives" rather than "carries a broken set".
		return trimSQL(data), nil
	}
	return normalizeSQL([]byte(strings.Join(body, "\n"))), nil
}

// gooseUnexpectedPragma reports a directive that cannot appear where it does.
// The community binary refuses each of these too; naming the line and the reason
// is the only difference.
func gooseUnexpectedPragma(name string, line int, pragma goosePragma, reason string) error {
	return fmt.Errorf(
		"migration file %s line %d: unexpected %q directive because %s",
		name, line, goosePragmaPrefix+string(pragma), reason,
	)
}

// dbmateUpSQL extracts the up section of a dbmate migration. Directive lines are
// matched whole and dropped entirely, so trailing options such as
// "-- migrate:up transaction:false" never leak into the executable SQL.
//
// A file with no "-- migrate:up" directive is refused. This is a DELIBERATE
// DIVERGENCE and it is the mirror image of the goose decision above: there,
// matching the community binary was right; here, matching it would be wrong.
//
// Measured on v1.3.0, a dbmate file that carries no "-- migrate:up":
//
//   - migrate apply exits 0, records revision 1 with 0 of 0 statements, and
//     creates nothing. The table is permanently absent AND the migration is
//     marked done, so no later apply will ever run it.
//   - migrate import exits 0 and writes a ZERO-BYTE file where 47 authored bytes
//     were, then hashes the empty file into atlas.sum as if it were the migration.
//
// That is silently discarding what the author wrote and corrupting recorded state
// in one behavior, so Ptah keeps refusing. Note the direction: the community
// binary exits 0 and ptah-compat exits 1, so the never-looser half of the parity
// rule would PERMIT copying this — nothing forces the refusal except that
// reproducing a defect to be identical to it is the wrong answer.
//
// Unlike goose, a dbmate file cannot be read as "the body is the migration":
// dbmate's own format requires the directive, and a file that has one but leaves
// its section empty is a different, legitimate thing that IS converted (to an
// empty migration, exactly as the community binary records it).
func dbmateUpSQL(name string, data []byte) ([]byte, error) {
	var out []string
	inUp := false
	foundUp := false
	for line := range strings.SplitSeq(string(data), "\n") {
		if directive, ok := dbmateDirective(line); ok {
			inUp = directive == "up"
			if inUp {
				foundUp = true
			}
			continue
		}
		if inUp {
			out = append(out, line)
		}
	}
	if !foundUp {
		return nil, fmt.Errorf(
			"migration file %s carries no %q directive, so none of its SQL would be executed; "+
				"Ptah refuses rather than recording an empty migration that can never be re-run",
			name, "-- migrate:up",
		)
	}
	return normalizeSQL([]byte(strings.Join(out, "\n"))), nil
}

// dbmateDirective reports whether line is a dbmate "-- migrate:<name>" directive
// and returns the lowercased directive name. Any options after the name (such as
// "transaction:false") are part of the directive line, not executable SQL.
func dbmateDirective(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	const prefix = "-- migrate:"
	if !strings.HasPrefix(strings.ToLower(trimmed), prefix) {
		return "", false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	name := rest
	if idx := strings.IndexAny(rest, " \t"); idx >= 0 {
		name = rest[:idx]
	}
	return strings.ToLower(name), true
}

// liquibaseSQL keeps a Liquibase formatted-SQL body, dropping the header and any
// --rollback directive lines. Liquibase has no up/down section marker, so the
// remainder is the up SQL and there is nothing here that can fail.
func liquibaseSQL(_ string, data []byte) ([]byte, error) {
	var out []string
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		if trimmed == "--liquibase formatted sql" || strings.HasPrefix(trimmed, "--rollback") {
			continue
		}
		out = append(out, line)
	}
	return normalizeSQL([]byte(strings.Join(out, "\n"))), nil
}

// trimSQL normalizes a body the way the community binary normalizes a goose file
// it found no directives in: surrounding whitespace goes, a single trailing
// newline is added, and interior blank lines are KEPT.
//
// [normalizeSQL] cannot be used for that case. It also drops blank lines between
// statements, which for a verbatim-preserved file would change the converted
// bytes and therefore its atlas.sum entry — leaving `atlas migrate import` and
// `ptah-compat migrate import` to disagree about a directory both call clean.
// Measured: a directive-free file of 83 bytes with one interior blank line is
// converted to 83 bytes by the community binary and would be 82 through
// normalizeSQL. Trailing blank lines and a missing final newline ARE normalized
// by both, which is why trimming is right and copying the bytes untouched is not.
func trimSQL(data []byte) []byte {
	sql := strings.TrimSpace(string(data))
	if sql == "" {
		return nil
	}
	return []byte(sql + "\n")
}

func normalizeSQL(data []byte) []byte {
	sql := strings.Join(dropSeparatorBlankLines(string(data)), "\n")
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil
	}
	return []byte(sql + "\n")
}

func dropSeparatorBlankLines(sql string) []string {
	var out []string
	state := sqlLiteralState{}
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.TrimSpace(line) != "" || state.insideLiteral() {
			out = append(out, line)
		}
		state.update(line)
	}
	return out
}

type sqlLiteralState struct {
	inSingleQuote  bool
	inBlockComment bool
	dollarQuote    string
}

func (s sqlLiteralState) insideLiteral() bool {
	return s.inSingleQuote || s.inBlockComment || s.dollarQuote != ""
}

func (s *sqlLiteralState) update(line string) {
	for i := 0; i < len(line); i++ {
		if s.dollarQuote != "" {
			idx := strings.Index(line[i:], s.dollarQuote)
			if idx < 0 {
				return
			}
			i += idx + len(s.dollarQuote) - 1
			s.dollarQuote = ""
			continue
		}
		if s.inBlockComment {
			idx := strings.Index(line[i:], "*/")
			if idx < 0 {
				return
			}
			i += idx + len("*/") - 1
			s.inBlockComment = false
			continue
		}
		if s.inSingleQuote {
			if line[i] == '\'' {
				if i+1 < len(line) && line[i+1] == '\'' {
					i++
					continue
				}
				s.inSingleQuote = false
			}
			continue
		}
		if strings.HasPrefix(line[i:], "--") {
			return
		}
		if strings.HasPrefix(line[i:], "/*") {
			s.inBlockComment = true
			i++
			continue
		}
		if line[i] == '\'' {
			s.inSingleQuote = true
			continue
		}
		if tag, ok := dollarQuoteTag(line[i:]); ok {
			s.dollarQuote = tag
			i += len(tag) - 1
		}
	}
}

func dollarQuoteTag(s string) (string, bool) {
	if !strings.HasPrefix(s, "$") {
		return "", false
	}
	end := strings.Index(s[1:], "$")
	if end < 0 {
		return "", false
	}
	tag := s[:end+2]
	if tag == "$$" {
		return tag, true
	}
	for _, r := range tag[1 : len(tag)-1] {
		if r != '_' && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return "", false
		}
	}
	return tag, true
}

func sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = unsafeNameRe.ReplaceAllString(name, "")
	return strings.Trim(name, "_.-")
}

func sortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		return strings.Compare(a.Name, b.Name)
	})
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// gooseDirectiveRemainder returns what follows a `-- +goose` marker, or reports
// that the line does not carry one.
//
// The separator is ANY run of whitespace, not one literal space. Requiring the
// space made a tab-separated directive invisible: the file then looked
// directive-free, and the raw-SQL path executed it whole. Measured on
// `-- +goose Up / CREATE TABLE t / -- +goose<TAB>Down / DROP TABLE t`, the
// community binary parses the Down and keeps the table at exit 0, while Ptah
// exited 0 having DROPPED it -- the same silent-rollback harm the near-miss
// guard exists to prevent, one keystroke away from it.
func gooseDirectiveRemainder(line string) (string, bool) {
	return gooseDirectiveRest(line, strings.HasPrefix)
}

// gooseDirectiveRemainderFold is [gooseDirectiveRemainder] for the near-miss
// guard, which matches the marker case-insensitively.
func gooseDirectiveRemainderFold(line string) (string, bool) {
	return gooseDirectiveRest(line, func(s, prefix string) bool {
		return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
	})
}

func gooseDirectiveRest(line string, hasPrefix func(string, string) bool) (string, bool) {
	marker := strings.TrimRight(goosePragmaPrefix, " ")
	if !hasPrefix(line, marker) {
		return "", false
	}
	rest := line[len(marker):]
	trimmed := strings.TrimLeft(rest, " \t")
	if trimmed == rest {
		// No separator at all: `-- +gooseUp` is not a directive.
		return "", false
	}
	return trimmed, true
}
