package importer

import (
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

// dbmateFileRE matches a dbmate migration name: <version>_<name>.sql.
var dbmateFileRE = regexp.MustCompile(`^(\d+)_(.+)\.sql$`)

// dbmateParser imports dbmate SQL migrations. A dbmate migration is a single
// <version>_<name>.sql file whose up and down statements are separated by
// `-- migrate:up` and `-- migrate:down` directive lines. Directive options such
// as `transaction:false` are part of the directive line, not SQL, and are
// dropped from the imported output.
type dbmateParser struct{}

func (dbmateParser) Name() string { return "dbmate" }

func (dbmateParser) Detect(fsys fs.FS) bool {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !dbmateFileRE.MatchString(entry.Name()) {
			continue
		}
		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			continue
		}
		if dbmateHasUpDirective(string(content)) {
			return true
		}
	}
	return false
}

func (dbmateParser) Parse(fsys fs.FS) ([]SourceMigration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}

	var migrations []SourceMigration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := dbmateFileRE.FindStringSubmatch(entry.Name())
		if match == nil {
			continue // ignore non-migration files
		}
		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", entry.Name(), err)
		}
		if !dbmateHasUpDirective(string(content)) {
			continue // not a dbmate migration (no -- migrate:up directive)
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid dbmate version in %q: %w", entry.Name(), err)
		}
		up, down := splitDbmateSQL(string(content))
		if strings.TrimSpace(up) == "" {
			return nil, fmt.Errorf("dbmate migration %q has an empty up section", entry.Name())
		}
		migrations = append(migrations, SourceMigration{
			Version: version,
			Name:    match[2],
			UpSQL:   up,
			DownSQL: down,
		})
	}
	if len(migrations) == 0 {
		return nil, fmt.Errorf("no dbmate migration files (<version>_<name>.sql with -- migrate:up) found")
	}
	return migrations, nil
}

// splitDbmateSQL splits a dbmate migration file into its up and down SQL.
// Directive lines are matched whole and dropped entirely, so trailing options
// such as "-- migrate:up transaction:false" never leak into the executable
// SQL. Content before the first directive and content under directives other
// than up/down is ignored.
func splitDbmateSQL(content string) (up, down string) {
	var upBuilder, downBuilder strings.Builder
	section := ""
	for line := range strings.SplitSeq(content, "\n") {
		if name, ok := dbmateDirective(line); ok {
			section = name
			continue
		}
		switch section {
		case "up":
			upBuilder.WriteString(line)
			upBuilder.WriteByte('\n')
		case "down":
			downBuilder.WriteString(line)
			downBuilder.WriteByte('\n')
		}
	}
	return strings.TrimSpace(upBuilder.String()), strings.TrimSpace(downBuilder.String())
}

// dbmateHasUpDirective reports whether content contains a `-- migrate:up`
// directive, marking the file as a dbmate migration.
func dbmateHasUpDirective(content string) bool {
	for line := range strings.SplitSeq(content, "\n") {
		if name, ok := dbmateDirective(line); ok && name == "up" {
			return true
		}
	}
	return false
}

// dbmateDirective reports whether line is a dbmate "-- migrate:<name>"
// directive and returns the lowercased directive name. Any options after the
// name (such as "transaction:false") are part of the directive line, not
// executable SQL.
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
