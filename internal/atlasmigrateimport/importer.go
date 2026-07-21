// Package atlasmigrateimport converts migration directories from
// Atlas-supported tools to Atlas migration directory layout.
package atlasmigrateimport

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/atlascompat"
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

type importEntry struct {
	Name string
	Data []byte
}

type flywayEntry struct {
	source   string
	name     string
	version  int
	baseline bool
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

	entries, err := loadEntries(from.Dir, format)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no importable migration files found in %s for format %q", from.Dir, format)
	}
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

func preflightTarget(fromDir, toDir string, format Format, entries []importEntry) error {
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

func loadEntries(dir string, format Format) ([]importEntry, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read source migration directory %s: %w", dir, err)
	}
	switch format {
	case FormatAtlas:
		return loadAtlasEntries(dir, files)
	case FormatGolangMigrate:
		return loadGolangMigrateEntries(dir, files)
	case FormatGoose:
		return loadDirectiveSectionEntries(dir, files, gooseUpSQL)
	case FormatDBMate:
		return loadDirectiveSectionEntries(dir, files, dbmateUpSQL)
	case FormatLiquibase:
		return loadDirectiveSectionEntries(dir, files, liquibaseSQL)
	case FormatFlyway:
		return loadFlywayEntries(dir, files)
	default:
		return nil, fmt.Errorf("unknown migration import format %q", format)
	}
}

func loadAtlasEntries(dir string, files []os.DirEntry) ([]importEntry, error) {
	var entries []importEntry
	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".sql" {
			continue
		}
		data, err := readImportSQLFile(dir, file.Name())
		if err != nil {
			return nil, err
		}
		entries = append(entries, importEntry{Name: file.Name(), Data: data})
	}
	sortEntries(entries)
	return entries, nil
}

func loadGolangMigrateEntries(dir string, files []os.DirEntry) ([]importEntry, error) {
	var entries []importEntry
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
		data, err := readImportSQLFile(dir, file.Name())
		if err != nil {
			return nil, err
		}
		entries = append(entries, importEntry{Name: match[1] + "_" + match[2] + ".sql", Data: data})
	}
	sortEntries(entries)
	return entries, nil
}

func loadDirectiveSectionEntries(
	dir string,
	files []os.DirEntry,
	extract func([]byte) []byte,
) ([]importEntry, error) {
	var entries []importEntry
	for _, file := range files {
		if file.IsDir() || !numberedSQLFileRe.MatchString(file.Name()) {
			continue
		}
		data, err := readImportSQLFile(dir, file.Name())
		if err != nil {
			return nil, err
		}
		data = extract(data)
		if len(data) == 0 {
			continue
		}
		entries = append(entries, importEntry{Name: file.Name(), Data: data})
	}
	sortEntries(entries)
	return entries, nil
}

func loadFlywayEntries(dir string, files []os.DirEntry) ([]importEntry, error) {
	var parsed []flywayEntry
	baselineVersion := 0
	for _, file := range files {
		entry, ok, err := parseFlywayEntry(file)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if entry.baseline && entry.version > baselineVersion {
			baselineVersion = entry.version
		}
		parsed = append(parsed, entry)
	}

	var entries []importEntry
	for _, entry := range parsed {
		if !entry.baseline && entry.version <= baselineVersion {
			continue
		}
		data, err := readImportSQLFile(dir, entry.source)
		if err != nil {
			return nil, err
		}
		entries = append(entries, importEntry{Name: fmt.Sprintf("%d_%s.sql", entry.version, entry.name), Data: data})
	}
	sortEntries(entries)
	return entries, nil
}

func parseFlywayEntry(file os.DirEntry) (flywayEntry, bool, error) {
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

func parseFlywayVersion(raw, filename string) (int, error) {
	digits := strings.NewReplacer(".", "", "_", "").Replace(raw)
	if digits == "" {
		return 0, fmt.Errorf("parse Flyway version in %s: missing version", filename)
	}
	version, err := strconv.Atoi(digits)
	if err != nil {
		return 0, fmt.Errorf("parse Flyway version in %s: %w", filename, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("parse Flyway version in %s: version must be greater than zero", filename)
	}
	return version, nil
}

func readImportSQLFile(dir, name string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("read source migration %s: %w", name, err)
	}
	return normalizeSQL(data), nil
}

func gooseUpSQL(data []byte) []byte {
	var out []string
	inUp := false
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		switch trimmed {
		case "-- +goose up":
			inUp = true
			continue
		case "-- +goose down":
			return normalizeSQL([]byte(strings.Join(out, "\n")))
		case "-- +goose statementbegin", "-- +goose statementend":
			continue
		}
		if inUp {
			out = append(out, line)
		}
	}
	if inUp {
		return normalizeSQL([]byte(strings.Join(out, "\n")))
	}
	return normalizeSQL(data)
}

func dbmateUpSQL(data []byte) []byte {
	sql := string(data)
	upper := strings.ToUpper(sql)
	upIdx := strings.Index(upper, "-- MIGRATE:UP")
	if upIdx < 0 {
		return normalizeSQL(data)
	}
	sql = sql[upIdx+len("-- MIGRATE:UP"):]
	upper = strings.ToUpper(sql)
	if downIdx := strings.Index(upper, "-- MIGRATE:DOWN"); downIdx >= 0 {
		sql = sql[:downIdx]
	}
	return normalizeSQL([]byte(sql))
}

func liquibaseSQL(data []byte) []byte {
	var out []string
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		if trimmed == "--liquibase formatted sql" || strings.HasPrefix(trimmed, "--rollback") {
			continue
		}
		out = append(out, line)
	}
	return normalizeSQL([]byte(strings.Join(out, "\n")))
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

func sortEntries(entries []importEntry) {
	slices.SortFunc(entries, func(a, b importEntry) int {
		return strings.Compare(a.Name, b.Name)
	})
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
