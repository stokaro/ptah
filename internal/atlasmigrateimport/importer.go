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
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/atlascompat"
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

type flywayEntry struct {
	source   string
	name     string
	version  flywayVersion
	baseline bool
}

// flywayVersion is a Flyway version parsed into numeric components. Flyway uses
// '.' and '_' interchangeably as component separators and compares components
// numerically, treating a shorter version as if zero-padded. So V1.5 < V2
// (1 < 2) and V2 == V2.0, while V2.0 and V20 are distinct.
type flywayVersion struct {
	components []int
	raw        string
}

// String returns the original version text, for diagnostics.
func (v flywayVersion) String() string { return v.raw }

// canonical drops trailing zero components so that equal Flyway versions (V2 and
// V2.0) share a representation while distinct ones (V2.0 and V20) do not.
func (v flywayVersion) canonical() []int {
	end := len(v.components)
	for end > 0 && v.components[end-1] == 0 {
		end--
	}
	return v.components[:end]
}

// key is a canonical equality key built from the canonical components.
func (v flywayVersion) key() string {
	comps := v.canonical()
	parts := make([]string, len(comps))
	for i, c := range comps {
		parts[i] = strconv.Itoa(c)
	}
	return strings.Join(parts, ".")
}

const (
	// flywayMaxComponents is how many numeric components a Flyway version may have
	// to be encodable as one int64 Atlas version (major.minor.patch).
	flywayMaxComponents = 3
	// flywayComponentBase is the fixed decimal width of each trailing component:
	// minor and patch each occupy two digits (0-99). Two digits keeps a full
	// 14-digit yyyyMMddHHmmss timestamp version representable in the leading slot
	// while the whole value stays within int64.
	flywayComponentBase = 100
)

// atlasVersion encodes the Flyway version as a stable, order-preserving int64
// Atlas version. The encoding is a fixed-width positional number
// (major*100^2 + minor*100 + patch) over the canonical (trailing-zero-trimmed)
// components. Because it depends only on this version — never on the other files
// in the directory — a given source file always maps to the same Atlas version,
// so inserting a mid-sequence migration never renumbers the others and Atlas
// revision checksums stay valid. V2 and V2.0 map to the same value. It returns a
// clear error rather than truncating or colliding when a version cannot be
// represented within int64.
func (v flywayVersion) atlasVersion() (int64, error) {
	comps := v.canonical()
	if len(comps) == 0 {
		return 0, fmt.Errorf("Flyway version %s has no numeric components", v)
	}
	if len(comps) > flywayMaxComponents {
		return 0, fmt.Errorf("Flyway version %s has more than %d components and cannot map to an int64 Atlas version", v, flywayMaxComponents)
	}
	padded := make([]int, flywayMaxComponents)
	copy(padded, comps)
	for i := 1; i < flywayMaxComponents; i++ {
		if padded[i] >= flywayComponentBase {
			return 0, fmt.Errorf("Flyway version %s component %d (%d) exceeds the maximum %d for an int64 Atlas version", v, i+1, padded[i], flywayComponentBase-1)
		}
	}
	var value int64
	for _, c := range padded {
		if value > (math.MaxInt64-int64(c))/flywayComponentBase {
			return 0, fmt.Errorf("Flyway version %s is too large to map to an int64 Atlas version", v)
		}
		value = value*flywayComponentBase + int64(c)
	}
	if value <= 0 {
		return 0, fmt.Errorf("Flyway version %s does not map to a positive Atlas version", v)
	}
	return value, nil
}

// compareFlywayVersions orders two Flyway versions numerically component by
// component, zero-padding the shorter one, matching Flyway's ordering.
func compareFlywayVersions(a, b flywayVersion) int {
	n := max(len(a.components), len(b.components))
	for i := range n {
		av, bv := 0, 0
		if i < len(a.components) {
			av = a.components[i]
		}
		if i < len(b.components) {
			bv = b.components[i]
		}
		if c := cmp.Compare(av, bv); c != 0 {
			return c
		}
	}
	return 0
}

var (
	flywayFileRe        = regexp.MustCompile(`(?i)^([vbru])([0-9][0-9._]*)?__(.+)\.sql$`)
	flywayVersionedRe   = regexp.MustCompile(`(?i)^[vbu].+__.+\.sql$`)
	golangMigrateFileRe = regexp.MustCompile(`^([0-9]+)_(.+)\.(up|down)\.sql$`)
	golangMigrateLikeRe = regexp.MustCompile(`^[0-9]+_.+\.(up|down)\.sql$`)
	numberedSQLFileRe   = regexp.MustCompile(`^[0-9]+_.+\.sql$`)
	unsafeNameRe        = regexp.MustCompile(`[^A-Za-z0-9_.-]`)
)

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
	snapshot, captureErr := migrationsnapshot.CaptureStable(root.FS())
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
		return loadDirectiveSectionEntries(fsys, files, "-- +goose Up", gooseUpSQL)
	case FormatDBMate:
		return loadDirectiveSectionEntries(fsys, files, "-- migrate:up", dbmateUpSQL)
	case FormatLiquibase:
		return loadDirectiveSectionEntries(fsys, files, "", liquibaseSQL)
	case FormatFlyway:
		return loadFlywayEntries(fsys, files)
	default:
		return nil, fmt.Errorf("unknown migration import format %q", format)
	}
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

func loadFlywayEntries(fsys fs.FS, files []fs.DirEntry) ([]Entry, error) {
	var parsed []flywayEntry
	var baseline *flywayVersion
	for _, file := range files {
		entry, ok, err := parseFlywayEntry(file)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if entry.baseline && (baseline == nil || compareFlywayVersions(entry.version, *baseline) > 0) {
			version := entry.version
			baseline = &version
		}
		parsed = append(parsed, entry)
	}

	selected := make([]flywayEntry, 0, len(parsed))
	for _, entry := range parsed {
		if !entry.baseline && baseline != nil && compareFlywayVersions(entry.version, *baseline) <= 0 {
			continue
		}
		selected = append(selected, entry)
	}

	if err := rejectDuplicateFlywayVersions(selected); err != nil {
		return nil, err
	}
	slices.SortStableFunc(selected, func(a, b flywayEntry) int {
		return compareFlywayVersions(a.version, b.version)
	})

	// Atlas migration versions are int64 ordered numerically, so a major.minor
	// Flyway version (V1.5 sits between V1 and V2) cannot be represented by
	// echoing the original number. Encode each version into a stable,
	// order-preserving int64 instead (see flywayVersion.atlasVersion). The
	// encoding depends only on the version itself, so inserting a mid-sequence
	// migration never renumbers the others and Atlas revision checksums stay
	// valid on re-apply. The original description is kept in the file name.
	entries := make([]Entry, 0, len(selected))
	for _, entry := range selected {
		version, err := entry.version.atlasVersion()
		if err != nil {
			return nil, err
		}
		data, err := readImportSQLFile(fsys, entry.source)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Name: fmt.Sprintf("%d_%s.sql", version, entry.name), Data: data})
	}
	return entries, nil
}

// rejectDuplicateFlywayVersions fails when two selected files resolve to the
// same Flyway version (for example V1.5 and V1_5, or V2 and V2.0). It runs
// before the monotonic version assignment so a genuine collision can never be
// masked by sequential numbering or surface only after the database is opened.
func rejectDuplicateFlywayVersions(entries []flywayEntry) error {
	seen := make(map[string]string, len(entries))
	for _, entry := range entries {
		key := entry.version.key()
		if prev, ok := seen[key]; ok {
			return fmt.Errorf("Flyway migrations %s and %s resolve to the same version %s", prev, entry.source, entry.version)
		}
		seen[key] = entry.source
	}
	return nil
}

func parseFlywayEntry(file fs.DirEntry) (flywayEntry, bool, error) {
	if file.IsDir() {
		return flywayEntry{}, false, nil
	}
	match := flywayFileRe.FindStringSubmatch(file.Name())
	if match == nil {
		if strings.HasSuffix(file.Name(), ".sql") && flywayVersionedRe.MatchString(file.Name()) {
			return flywayEntry{}, false, fmt.Errorf("unsupported Flyway migration file name %s", file.Name())
		}
		return flywayEntry{}, false, nil
	}

	prefix := strings.ToUpper(match[1])
	if prefix == "U" {
		return flywayEntry{}, false, nil
	}
	entry := flywayEntry{source: file.Name(), name: sanitizeName(match[3])}
	if prefix == "R" {
		return flywayEntry{}, false, fmt.Errorf("Flyway repeatable migration %s cannot be imported yet because Ptah does not execute Atlas R-suffixed migrations", file.Name())
	}
	version, err := parseFlywayVersion(match[2], file.Name())
	if err != nil {
		return flywayEntry{}, false, err
	}
	entry.version = version
	entry.baseline = prefix == "B"
	return entry, true, nil
}

// parseFlywayVersion parses a Flyway version string into numeric components. It
// splits on '.'/'_' (Flyway's interchangeable separators) so "1.5" and "1_5"
// both yield [1 5], preserving component boundaries for correct ordering rather
// than concatenating digits.
func parseFlywayVersion(raw, filename string) (flywayVersion, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == '.' || r == '_' })
	if len(parts) == 0 {
		return flywayVersion{}, fmt.Errorf("parse Flyway version in %s: missing version", filename)
	}
	components := make([]int, len(parts))
	allZero := true
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil {
			return flywayVersion{}, fmt.Errorf("parse Flyway version in %s: %w", filename, err)
		}
		if n < 0 {
			return flywayVersion{}, fmt.Errorf("parse Flyway version in %s: version must be greater than zero", filename)
		}
		if n != 0 {
			allZero = false
		}
		components[i] = n
	}
	if allZero {
		return flywayVersion{}, fmt.Errorf("parse Flyway version in %s: version must be greater than zero", filename)
	}
	return flywayVersion{components: components, raw: raw}, nil
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
