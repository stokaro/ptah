package importer

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/liquibaserun"
)

// Liquibase formatted-SQL changelog markers. A formatted-SQL changelog begins
// with a `--liquibase formatted sql` header line and splits its migrations into
// changesets marked by `--changeset <author>:<id>`; rollback SQL for a changeset
// is given by `--rollback <sql>` lines.
var (
	liquibaseHeaderRE   = regexp.MustCompile(`(?i)^--\s*liquibase\s+formatted\s+sql\b`)
	liquibaseRollbackRE = regexp.MustCompile(`(?i)^\s*--\s*rollback\b(.*)$`)
	// liquibaseNameSepRE collapses runs of non-alphanumerics (e.g. the ':' and
	// '-' common in changeset ids) to a single '_' when building a Ptah name.
	liquibaseNameSepRE = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	// liquibaseChangelogRootRE recognizes an XML/YAML/JSON changelog by its
	// `databaseChangeLog` root in a structural position, avoiding a false match on
	// a stray textual mention.
	liquibaseChangelogRootRE = regexp.MustCompile(`(?m)<databaseChangeLog|"databaseChangeLog"|^\s*databaseChangeLog\s*:`)
)

// liquibaseChangelogExts are the extensions of the XML, YAML and JSON
// changelogs, which liquibase_changelog.go reads.
var liquibaseChangelogExts = map[string]bool{".xml": true, ".yaml": true, ".yml": true, ".json": true}

// liquibaseParser imports Liquibase changelogs: formatted SQL -- a `.sql` file
// that opens with `--liquibase formatted sql` and groups statements into
// changesets -- and the XML, YAML and JSON changelogs. Changesets have no
// numeric version -- they are identified by `author:id` and applied in file
// order -- so they are assigned sequential Ptah versions in that order, with
// the `author:id` carried into the name.
//
// dialect is the target a typed change is rendered for, and caps the preset it
// is rendered against. Both are empty until [WithDialectCapabilities] sets
// them; without a dialect a typed change is refused.
type liquibaseParser struct {
	dialect string
	caps    capability.Capabilities
}

// withDialect returns the parser set to render typed changes for dialect.
func (p liquibaseParser) withDialect(dialect string, caps capability.Capabilities) Parser {
	p.dialect = dialect
	p.caps = caps
	return p
}

func (liquibaseParser) Name() string { return "liquibase" }

func (liquibaseParser) NamePattern() string {
	return "a formatted-SQL changelog beginning with --liquibase formatted sql, or an XML/YAML/JSON changelog"
}

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

func (p liquibaseParser) Parse(fsys fs.FS) (*ParseResult, error) {
	result := &ParseResult{}
	entries, err := topLevelOnly(fsys, p.Name(), result)
	if err != nil {
		return nil, err
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

	// A changelog (XML/YAML/JSON) defines the real apply order and may include
	// the SQL files, so mixing the two sources would reorder or duplicate
	// history. They are read INSTEAD of the formatted-SQL files when present,
	// and a directory holding both is refused rather than half-read
	// (stokaro/ptah#1629).
	if len(changelogFiles) > 0 {
		if len(sqlFiles) > 0 {
			return nil, fmt.Errorf(
				"liquibase source holds both changelog files (%s) and formatted-SQL changelogs (%s); "+
					"a changelog defines the apply order and may include the SQL files, so importing both "+
					"would reorder or duplicate history -- import them separately",
				strings.Join(changelogFiles, ", "), strings.Join(sqlFiles, ", "))
		}
		migrations, read, err := parseLiquibaseChangelogFiles(fsys, changelogFiles, p.dialect, p.caps)
		if err != nil {
			return nil, err
		}
		for _, name := range changelogFiles {
			result.consume(name)
		}
		// A file a sqlFile change read became part of a migration, so it is
		// accounted for as consumed rather than reported as left behind.
		for _, name := range read {
			result.consume(name)
		}
		if len(migrations) == 0 {
			return nil, fmt.Errorf("liquibase changelog(s) %s contain no changesets",
				strings.Join(changelogFiles, ", "))
		}
		for i := range migrations {
			migrations[i].Version = int64(i + 1)
		}
		result.Migrations = migrations
		return result, nil
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
		result.consume(name)
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
	result.Migrations = migrations
	return result, nil
}

// parseLiquibaseFormattedSQL splits one formatted-SQL changelog into changesets.
// Lines before the first `--changeset` (the header and any preamble) are ignored;
// within a changeset, `--rollback` lines contribute the down SQL and every other
// line is up SQL. Version is left zero here — the caller assigns sequential
// versions across all files.
//
// The attributes of a `--changeset` line and its `--preconditions` lines go to
// [liquibaserun.Condition], the same recognizer the XML, YAML and JSON readers
// use, so a changeset that runs conditionally or repeatedly is refused here as
// it is there.
func parseLiquibaseFormattedSQL(fileName, content string) ([]SourceMigration, error) {
	var changesets []SourceMigration
	seen := make(map[string]bool) // author:id within this file
	var current *liquibaseFormattedChangeSet

	flush := func() error {
		if current == nil {
			return nil
		}
		migration, err := current.migration(fileName)
		if err != nil {
			return err
		}
		changesets = append(changesets, migration)
		return nil
	}

	for line := range strings.SplitSeq(content, "\n") {
		// A marker missing its author:id is still a marker, so it is refused
		// rather than absorbed as the previous changeset's SQL.
		if args, ok := liquibaserun.ChangesetArgs(line); ok {
			if err := flush(); err != nil {
				return nil, err
			}
			author, id, err := parseLiquibaseChangesetID(args, fileName)
			if err != nil {
				return nil, err
			}
			key := author + ":" + id
			if seen[key] {
				return nil, fmt.Errorf("duplicate liquibase changeset %s in %q", key, fileName)
			}
			seen[key] = true
			current = &liquibaseFormattedChangeSet{liquibaseChangeSet: liquibaseChangeSet{author: author, id: id}}
			for _, attribute := range liquibaserun.Attributes(args) {
				current.run.Note(attribute[0], attribute[1])
			}
			continue
		}
		if current == nil {
			// Only the `--liquibase formatted sql` header, comments, and blank
			// lines may precede the first changeset; real SQL there would be lost,
			// so reject it rather than drop it.
			if trimmed := strings.TrimSpace(line); trimmed != "" && !strings.HasPrefix(trimmed, "--") {
				return nil, fmt.Errorf("liquibase changelog %q has SQL before the first --changeset: %q", fileName, trimmed)
			}
			continue // header / preamble before the first changeset
		}
		current.addLine(line)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return changesets, nil
}

// liquibaseFormattedChangeSet collects one formatted-SQL changeset while its
// lines are read.
type liquibaseFormattedChangeSet struct {
	liquibaseChangeSet
	up, down strings.Builder
}

// addLine files one line of the changeset: a `--rollback` line into the down, a
// `--preconditions` line into the run conditions, and any other line into the
// up.
func (cs *liquibaseFormattedChangeSet) addLine(line string) {
	if match := liquibaseRollbackRE.FindStringSubmatch(line); match != nil {
		if payload := strings.TrimSpace(match[1]); payload != "" {
			cs.down.WriteString(payload)
			cs.down.WriteByte('\n')
		}
		return
	}
	if liquibaserun.IsPreconditions(line) {
		// A comment to the database, and a condition to Liquibase: kept in the
		// up, it would run unconditionally what ran conditionally.
		cs.run.Note("preconditions", "")
		return
	}
	cs.up.WriteString(line)
	cs.up.WriteByte('\n')
}

// migration is the changeset as a migration, or the refusal that stops it.
func (cs *liquibaseFormattedChangeSet) migration(fileName string) (SourceMigration, error) {
	if err := cs.run.Err(cs.author+":"+cs.id, fileName); err != nil {
		return SourceMigration{}, err
	}
	upSQL := strings.TrimSpace(cs.up.String())
	if upSQL == "" {
		return SourceMigration{}, fmt.Errorf("liquibase changeset %s:%s in %q has no SQL", cs.author, cs.id, fileName)
	}
	return SourceMigration{
		Name:    liquibaseChangesetName(cs.author, cs.id),
		UpSQL:   upSQL,
		DownSQL: strings.TrimSpace(cs.down.String()),
	}, nil
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
// changelog (a `databaseChangeLog` root), the serializations read by
// liquibase_changelog.go.
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
