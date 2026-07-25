package importer

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strings"
)

// Liquibase formatted-SQL changelog markers. A formatted-SQL changelog begins
// with a `--liquibase formatted sql` header line and splits its migrations into
// changesets marked by `--changeset <author>:<id>`; rollback SQL for a changeset
// is given by `--rollback <sql>` lines.
var (
	liquibaseHeaderRE = regexp.MustCompile(`(?i)^--\s*liquibase\s+formatted\s+sql\b`)
	// liquibaseChangesetRE matches on the `--changeset` keyword alone (not the
	// argument) so that a marker missing its author:id still routes through
	// validation and errors, rather than being absorbed as the previous
	// changeset's SQL. `\b` keeps `--changesetlike` from matching.
	liquibaseChangesetRE = regexp.MustCompile(`(?i)^\s*--\s*changeset\b(.*)$`)
	liquibaseRollbackRE  = regexp.MustCompile(`(?i)^\s*--\s*rollback\b(.*)$`)
	// liquibaseNameSepRE collapses runs of non-alphanumerics (e.g. the ':' and
	// '-' common in changeset ids) to a single '_' when building a Ptah name.
	liquibaseNameSepRE = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	// liquibaseChangelogRootRE recognizes an XML/YAML/JSON changelog by its
	// `databaseChangeLog` root in a structural position, avoiding a false match on
	// a stray textual mention.
	liquibaseChangelogRootRE = regexp.MustCompile(`(?m)<databaseChangeLog|"databaseChangeLog"|^\s*databaseChangeLog\s*:`)
)

// liquibaseChangelogExts are the changelog file extensions whose XML/YAML/JSON
// formats this importer does not yet parse (only formatted SQL is supported).
var liquibaseChangelogExts = map[string]bool{".xml": true, ".yaml": true, ".yml": true, ".json": true}

// liquibaseParser imports Liquibase formatted-SQL changelogs: a `.sql` file that
// opens with `--liquibase formatted sql` and groups statements into changesets.
// Changesets have no numeric version — they are identified by `author:id` and
// applied in file order — so they are assigned sequential Ptah versions in that
// order, with the `author:id` carried into the name. XML, YAML, and JSON
// changelogs are detected and rejected with an actionable message.
type liquibaseParser struct{}

func (liquibaseParser) Name() string { return "liquibase" }

func (liquibaseParser) Detect(fsys fs.FS) bool {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if liquibaseFormattedSQLFile(fsys, entry.Name()) || liquibaseChangelogFile(fsys, entry.Name()) {
			return true
		}
	}
	return false
}

func (liquibaseParser) Parse(fsys fs.FS) ([]SourceMigration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}

	var sqlFiles, changelogFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		switch {
		case liquibaseFormattedSQLFile(fsys, entry.Name()):
			sqlFiles = append(sqlFiles, entry.Name())
		case liquibaseChangelogFile(fsys, entry.Name()):
			changelogFiles = append(changelogFiles, entry.Name())
		}
	}

	// A changelog (XML/YAML/JSON) is unsupported and, when present, likely defines
	// the real apply order or includes the SQL files. Importing only the SQL files
	// (ordered by name) could silently reorder or omit history, so reject rather
	// than partially import.
	if len(changelogFiles) > 0 {
		return nil, fmt.Errorf("liquibase XML/YAML/JSON changelogs are not yet supported (only formatted-SQL changelogs beginning with %q); found %s", "--liquibase formatted sql", strings.Join(changelogFiles, ", "))
	}

	// No numeric version orders formatted-SQL changesets: Liquibase applies them
	// in changelog order. Absent a master changelog, order the files by name and
	// the changesets within each by appearance, then assign sequential Ptah
	// versions across the whole set.
	slices.Sort(sqlFiles)

	var migrations []SourceMigration
	for _, name := range sqlFiles {
		content, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", name, err)
		}
		changesets, err := parseLiquibaseFormattedSQL(name, string(content))
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, changesets...)
	}

	if len(migrations) == 0 {
		if len(sqlFiles) > 0 {
			return nil, fmt.Errorf("liquibase formatted-SQL changelog(s) %s contain no --changeset markers", strings.Join(sqlFiles, ", "))
		}
		return nil, fmt.Errorf("no liquibase formatted-SQL changelogs (files beginning with %q) found", "--liquibase formatted sql")
	}

	// Assign sequential versions in the established order.
	for i := range migrations {
		migrations[i].Version = int64(i + 1)
	}
	return migrations, nil
}

// parseLiquibaseFormattedSQL splits one formatted-SQL changelog into changesets.
// Lines before the first `--changeset` (the header and any preamble) are ignored;
// within a changeset, `--rollback` lines contribute the down SQL and every other
// line is up SQL. Version is left zero here — the caller assigns sequential
// versions across all files.
func parseLiquibaseFormattedSQL(fileName, content string) ([]SourceMigration, error) {
	var changesets []SourceMigration
	seen := make(map[string]bool) // author:id within this file

	var up, down strings.Builder
	author, id := "", ""
	inChangeset := false

	flush := func() error {
		if !inChangeset {
			return nil
		}
		upSQL := strings.TrimSpace(up.String())
		if upSQL == "" {
			return fmt.Errorf("liquibase changeset %s:%s in %q has no SQL", author, id, fileName)
		}
		changesets = append(changesets, SourceMigration{
			Name:    liquibaseChangesetName(author, id),
			UpSQL:   upSQL,
			DownSQL: strings.TrimSpace(down.String()),
		})
		return nil
	}

	for line := range strings.SplitSeq(content, "\n") {
		if match := liquibaseChangesetRE.FindStringSubmatch(line); match != nil {
			if err := flush(); err != nil {
				return nil, err
			}
			up.Reset()
			down.Reset()
			var err error
			author, id, err = parseLiquibaseChangesetID(match[1], fileName)
			if err != nil {
				return nil, err
			}
			key := author + ":" + id
			if seen[key] {
				return nil, fmt.Errorf("duplicate liquibase changeset %s in %q", key, fileName)
			}
			seen[key] = true
			inChangeset = true
			continue
		}
		if !inChangeset {
			// Only the `--liquibase formatted sql` header, comments, and blank
			// lines may precede the first changeset; real SQL there would be lost,
			// so reject it rather than drop it.
			if trimmed := strings.TrimSpace(line); trimmed != "" && !strings.HasPrefix(trimmed, "--") {
				return nil, fmt.Errorf("liquibase changelog %q has SQL before the first --changeset: %q", fileName, trimmed)
			}
			continue // header / preamble before the first changeset
		}
		if match := liquibaseRollbackRE.FindStringSubmatch(line); match != nil {
			if payload := strings.TrimSpace(match[1]); payload != "" {
				down.WriteString(payload)
				down.WriteByte('\n')
			}
			continue
		}
		up.WriteString(line)
		up.WriteByte('\n')
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return changesets, nil
}

// parseLiquibaseChangesetID extracts the author and id from a `--changeset`
// marker's argument ("author:id [attr:value ...]"). Liquibase requires both.
func parseLiquibaseChangesetID(args, fileName string) (author, id string, err error) {
	fields := strings.Fields(args) // first token is author:id; trailing tokens are attributes
	if len(fields) == 0 {
		return "", "", fmt.Errorf("liquibase changeset marker in %q is missing author:id", fileName)
	}
	author, id, ok := strings.Cut(fields[0], ":")
	if !ok || author == "" || id == "" {
		return "", "", fmt.Errorf("liquibase changeset marker %q in %q is missing author:id", strings.TrimSpace(args), fileName)
	}
	return author, id, nil
}

// liquibaseChangesetName builds a Ptah description from a changeset's author and
// id, so the imported file name stays traceable to the source changeset.
func liquibaseChangesetName(author, id string) string {
	clean := func(s string) string {
		return strings.Trim(liquibaseNameSepRE.ReplaceAllString(s, "_"), "_")
	}
	return clean(author) + "_" + clean(id)
}

// liquibaseFormattedSQLFile reports whether name is a `.sql` file whose first
// non-blank line is the `--liquibase formatted sql` header.
func liquibaseFormattedSQLFile(fsys fs.FS, name string) bool {
	if !strings.EqualFold(path.Ext(name), ".sql") {
		return false
	}
	content, err := fs.ReadFile(fsys, name)
	if err != nil {
		return false
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		return liquibaseHeaderRE.MatchString(strings.TrimSpace(line))
	}
	return false
}

// liquibaseChangelogFile reports whether name is an XML/YAML/JSON Liquibase
// changelog (a `databaseChangeLog` root), the formats this importer rejects.
func liquibaseChangelogFile(fsys fs.FS, name string) bool {
	if !liquibaseChangelogExts[strings.ToLower(path.Ext(name))] {
		return false
	}
	content, err := fs.ReadFile(fsys, name)
	if err != nil {
		return false
	}
	return liquibaseChangelogRootRE.Match(content)
}
