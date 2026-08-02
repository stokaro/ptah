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

	"github.com/stokaro/ptah/atlascompat"
	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/migration/migrator"
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
	// flywayMaxLeadingComponent is what is left for the leading component once
	// the bands, the trailing slots and the tie budget are taken out. It stays
	// above 99999999999999, so the 14-digit yyyyMMddHHmmss timestamps Flyway
	// projects commonly use are all representable.
	flywayMaxLeadingComponent = flywayBandSize/(flywayComponentSlot*flywayComponentSlot*flywayTieSlots) - 1
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
// converts it to in-memory Atlas single-file entries.
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
	return LoadFS(snapshot, dir, format)
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
// unsupported migrations, and empty results return an error.
func LoadFS(fsys fs.FS, display string, format Format) (*Loaded, error) {
	if fsys == nil {
		return nil, fmt.Errorf("source migration filesystem is required")
	}
	entries, err := loadEntries(fsys, display, format)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no importable migration files found in %s for format %q", display, format)
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
		return loadDirectiveSectionEntries(fsys, files, "-- migrate:up", dbmateUpSQL)
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
	return loadDirectiveSectionEntries(fsys, files, "-- +goose Up", gooseUpSQL)
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
	return loadDirectiveSectionEntries(fsys, files, "", liquibaseSQL)
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
// dbmate, liquibase) and keeps only their up SQL. extract reports found=false
// when a required up directive is absent; that is a hard error rather than a
// fall back to executing the raw file, so a malformed directive can never cause
// the down/rollback section to run against a live database. directive names the
// expected marker for the error message (empty when the format has no required
// up directive, such as liquibase).
func loadDirectiveSectionEntries(
	fsys fs.FS,
	files []fs.DirEntry,
	directive string,
	extract func([]byte) ([]byte, bool),
) ([]Entry, error) {
	var entries []Entry
	for _, file := range files {
		if file.IsDir() || !numberedSQLFileRe.MatchString(file.Name()) {
			continue
		}
		data, err := readImportSQLFile(fsys, file.Name())
		if err != nil {
			return nil, err
		}
		up, found := extract(data)
		if !found {
			return nil, fmt.Errorf("migration file %s has no %q section", file.Name(), directive)
		}
		if len(up) == 0 {
			continue
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

	entries := make([]Entry, 0, len(covered))
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
		data, err := readImportSQLFile(fsys, file.name)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Name: flywayEntryName(version, file.description), Data: data})
	}
	return entries, nil
}

// rejectDuplicateFlywayVersions refuses two covered files that carry the same
// Atlas CE version, which is a directory Atlas CE cannot execute at all: it
// panics with an index-out-of-range rather than reporting anything.
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

	key := int64(components[0]) + 1
	for i := 1; i < flywayMaxComponents; i++ {
		slot := int64(0)
		if i < len(components) {
			if components[i] < 0 || components[i] >= flywayComponentSlot-1 {
				return 0, fmt.Errorf(
					"Flyway migration %s has version %q whose component %d (%d) is outside 0..%d and cannot map to an int64 Atlas version",
					file.name, file.version, i+1, components[i], flywayComponentSlot-2,
				)
			}
			slot = int64(components[i]) + 1
		}
		key = key*flywayComponentSlot + slot
	}
	return key, nil
}

func readImportSQLFile(fsys fs.FS, name string) ([]byte, error) {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("read source migration %s: %w", name, err)
	}
	return normalizeSQL(data), nil
}

// gooseUpSQL extracts the up section of a goose migration. It tracks
// StatementBegin/StatementEnd blocks: inside a block goose suspends annotation
// parsing, so only StatementEnd is honored and every other line (including a
// stray -- +goose Up/Down that appears inside a function body) is passed through
// verbatim. found is false when the file has no -- +goose Up marker; the caller
// must treat that as an error rather than execute the raw file, so a malformed
// directive can never leak the down section onto the apply path.
func gooseUpSQL(data []byte) ([]byte, bool) {
	var out []string
	inUp := false
	foundUp := false
	inStatement := false
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		if inStatement {
			if trimmed == "-- +goose statementend" {
				inStatement = false
				continue
			}
			if inUp {
				out = append(out, line)
			}
			continue
		}
		switch trimmed {
		case "-- +goose up":
			inUp = true
			foundUp = true
			continue
		case "-- +goose down":
			inUp = false
			continue
		case "-- +goose statementbegin":
			inStatement = true
			continue
		case "-- +goose statementend":
			continue
		}
		if inUp {
			out = append(out, line)
		}
	}
	if !foundUp {
		return nil, false
	}
	return normalizeSQL([]byte(strings.Join(out, "\n"))), true
}

// dbmateUpSQL extracts the up section of a dbmate migration. Directive lines are
// matched whole and dropped entirely, so trailing options such as
// "-- migrate:up transaction:false" never leak into the executable SQL. found is
// false when there is no -- migrate:up directive.
func dbmateUpSQL(data []byte) ([]byte, bool) {
	var out []string
	inUp := false
	foundUp := false
	for line := range strings.SplitSeq(string(data), "\n") {
		if name, ok := dbmateDirective(line); ok {
			inUp = name == "up"
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
		return nil, false
	}
	return normalizeSQL([]byte(strings.Join(out, "\n"))), true
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
// remainder is the up SQL; found is always true.
func liquibaseSQL(data []byte) ([]byte, bool) {
	var out []string
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		if trimmed == "--liquibase formatted sql" || strings.HasPrefix(trimmed, "--rollback") {
			continue
		}
		out = append(out, line)
	}
	return normalizeSQL([]byte(strings.Join(out, "\n"))), true
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
