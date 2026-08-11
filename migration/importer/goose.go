package importer

import (
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

// gooseFileRE matches a Goose single-file migration name: <version>_<name>.sql.
var gooseFileRE = regexp.MustCompile(`^(\d+)_(.+)\.sql$`)

// gooseGoFileRE matches a Goose Go-based migration file, which this importer
// cannot convert (SQL only).
var gooseGoFileRE = regexp.MustCompile(`^(\d+)_(.+)\.go$`)

// gooseUpMarker and gooseDownMarker start the up and down sections of a Goose
// migration file. Goose also emits StatementBegin/End and NO TRANSACTION
// annotations, which are directives to Goose's splitter, not SQL. The section
// directives are dropped; NO TRANSACTION becomes typed whole-migration metadata
// that Emit translates to Ptah's directive on both directions.
const (
	gooseUpMarker   = "up"
	gooseDownMarker = "down"
)

// gooseParser imports Goose SQL migrations. A Goose migration is a single
// <version>_<name>.sql file whose up and down statements are separated by
// `-- +goose Up` and `-- +goose Down` annotations. Go-based Goose migrations are
// out of scope and reported as an error.
type gooseParser struct{}

func (gooseParser) Name() string { return "goose" }

func (gooseParser) Detect(fsys fs.FS) bool {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !gooseFileRE.MatchString(entry.Name()) {
			continue
		}
		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			continue
		}
		if gooseSection(string(content)) != "" {
			return true
		}
	}
	return false
}

func (gooseParser) Parse(fsys fs.FS) ([]SourceMigration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}

	var migrations []SourceMigration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if gooseGoFileRE.MatchString(entry.Name()) {
			return nil, fmt.Errorf("Go-based Goose migration %q is not supported (SQL migrations only)", entry.Name())
		}
		match := gooseFileRE.FindStringSubmatch(entry.Name())
		if match == nil {
			continue // ignore non-migration files
		}
		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", entry.Name(), err)
		}
		if gooseSection(string(content)) == "" {
			continue // not a Goose migration (no -- +goose Up marker)
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid goose version in %q: %w", entry.Name(), err)
		}
		up, down, noTransaction := splitGooseSQL(string(content))
		if strings.TrimSpace(up) == "" {
			return nil, fmt.Errorf("goose migration %q has an empty up section", entry.Name())
		}
		migrations = append(migrations, SourceMigration{
			Version:       version,
			Name:          match[2],
			UpSQL:         up,
			DownSQL:       down,
			NoTransaction: noTransaction,
		})
	}
	if len(migrations) == 0 {
		return nil, fmt.Errorf("no goose migration files (<version>_<name>.sql with -- +goose Up) found")
	}
	return migrations, nil
}

// splitGooseSQL splits a Goose migration file into its up and down SQL, dropping
// the goose annotation lines. Content before the first `-- +goose Up` is ignored.
//
// Statements wrapped in `-- +goose StatementBegin` / `StatementEnd` are copied
// verbatim except for the whole-file `-- +goose NO TRANSACTION` annotation,
// which Goose recognizes even inside a statement block. Other annotations in a
// block remain statement text: the body may legitimately contain semicolons or
// annotation lookalikes, for example a `-- +goose Down` comment inside a
// function body. Honoring only `NO TRANSACTION` and `StatementEnd` inside a
// block prevents a marker in a statement body from silently flipping sections.
func splitGooseSQL(content string) (up, down string, noTransaction bool) {
	var upBuilder, downBuilder strings.Builder
	section := ""
	inStatement := false
	writeLine := func(line string) {
		switch section {
		case gooseUpMarker:
			upBuilder.WriteString(line)
			upBuilder.WriteByte('\n')
		case gooseDownMarker:
			downBuilder.WriteString(line)
			downBuilder.WriteByte('\n')
		}
	}
	for line := range strings.SplitSeq(content, "\n") {
		marker := gooseMarker(line)
		if marker == gooseNoTxMarker {
			noTransaction = true
			continue // whole-file annotation, even inside StatementBegin/End
		}
		if inStatement {
			if marker == gooseStatementEndMarker {
				inStatement = false
				continue // strip the StatementEnd annotation
			}
			writeLine(line) // verbatim body, annotations included
			continue
		}
		switch marker {
		case gooseUpMarker:
			section = gooseUpMarker
		case gooseDownMarker:
			section = gooseDownMarker
		case gooseStatementBeginMarker:
			inStatement = true // strip the StatementBegin annotation
		case gooseStatementEndMarker, gooseOtherMarker:
			// directive, not SQL — drop it
		default:
			writeLine(line) // ordinary SQL line
		}
	}
	return strings.TrimSpace(upBuilder.String()), strings.TrimSpace(downBuilder.String()), noTransaction
}

// gooseSection reports the first section marker (up/down) in content, or "" when
// the file is not a Goose migration.
func gooseSection(content string) string {
	for line := range strings.SplitSeq(content, "\n") {
		switch gooseMarker(line) {
		case gooseUpMarker:
			return gooseUpMarker
		case gooseDownMarker:
			return gooseDownMarker
		}
	}
	return ""
}

const (
	gooseStatementBeginMarker = "statement_begin"
	gooseStatementEndMarker   = "statement_end"
	gooseNoTxMarker           = "notx"
	gooseOtherMarker          = "other"
)

// gooseMarker classifies a line as a goose annotation: the up/down section
// markers, the StatementBegin/End and NO TRANSACTION directives (not SQL), or ""
// for an ordinary SQL line.
func gooseMarker(line string) string {
	trimmed := strings.TrimSpace(line)
	directive, ok := strings.CutPrefix(trimmed, "-- +goose")
	if !ok {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(directive)) {
	case gooseUpMarker:
		return gooseUpMarker
	case gooseDownMarker:
		return gooseDownMarker
	case "statementbegin":
		return gooseStatementBeginMarker
	case "statementend":
		return gooseStatementEndMarker
	case "no transaction":
		return gooseNoTxMarker
	default:
		return gooseOtherMarker
	}
}
