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

func (dbmateParser) NamePattern() string {
	return "<version>_<name>.sql carrying a -- migrate:up directive"
}

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

func (p dbmateParser) Parse(fsys fs.FS) (*ParseResult, error) {
	result := &ParseResult{}
	entries, err := topLevelOnly(fsys, p.Name(), result)
	if err != nil {
		return nil, err
	}

	var migrations []SourceMigration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := dbmateFileRE.FindStringSubmatch(entry.Name())
		if match == nil {
			continue // AccountForSource reports it by name
		}
		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", entry.Name(), err)
		}
		if !dbmateHasUpDirective(string(content)) {
			// The name is a dbmate migration's and the content is not, which is
			// far more likely a migration missing its directive than a file
			// that was never one.
			result.decline(entry.Name(), "it has a dbmate migration's name but no -- migrate:up directive")
			continue
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid dbmate version in %q: %w", entry.Name(), err)
		}
		result.consume(entry.Name())
		up, down, upNoTransaction, downNoTransaction := splitDbmateSQL(string(content))
		if strings.TrimSpace(up) == "" {
			return nil, fmt.Errorf("dbmate migration %q has an empty up section", entry.Name())
		}
		migrations = append(migrations, SourceMigration{
			Version: version,
			Name:    match[2],
			UpSQL:   up,
			DownSQL: down,
			// dbmate scopes transaction:false to one direction and so does
			// Ptah, so each side carries its own. Widening one direction's
			// option to both would take the transaction away from a
			// multi-statement direction that never asked, and a failure
			// partway through it would leave half the change behind.
			UpNoTransaction:   upNoTransaction,
			DownNoTransaction: downNoTransaction,
		})
	}
	if len(migrations) == 0 {
		return nil, fmt.Errorf("no dbmate migration files (<version>_<name>.sql with -- migrate:up) found")
	}
	result.Migrations = migrations
	return result, nil
}

// splitDbmateSQL splits a dbmate migration file into its up and down SQL, and
// reports for each direction whether it asked not to run inside a transaction.
//
// Directive lines are matched whole and dropped entirely, so trailing options
// such as "-- migrate:up transaction:false" never leak into the executable
// SQL. Dropping the option from the SQL is right, and dropping what it meant
// is the part worth guarding: `transaction:false` is how a dbmate author says
// a statement cannot run inside a transaction, which is what CREATE INDEX
// CONCURRENTLY and its relatives require. Without the flag this returns, a
// converted migration runs inside a transaction and fails on a server that
// refuses it there.
//
// Content before the first directive and content under directives other than
// up/down is ignored.
func splitDbmateSQL(content string) (up, down string, upNoTransaction, downNoTransaction bool) {
	var upBuilder, downBuilder strings.Builder
	section := ""
	for line := range strings.SplitSeq(content, "\n") {
		if name, options, ok := dbmateDirective(line); ok {
			section = name
			switch {
			case name == "up" && dbmateDisablesTransaction(options):
				upNoTransaction = true
			case name == "down" && dbmateDisablesTransaction(options):
				downNoTransaction = true
			}
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
	return strings.TrimSpace(upBuilder.String()), strings.TrimSpace(downBuilder.String()), upNoTransaction, downNoTransaction
}

// dbmateDisablesTransaction reports whether a directive's options carry
// dbmate's transaction:false. The options are whitespace-separated key:value
// pairs; only this one has a destination in Ptah's format today, and an option
// nobody recognizes is left alone rather than guessed at.
func dbmateDisablesTransaction(options string) bool {
	for field := range strings.FieldsSeq(strings.ToLower(options)) {
		if field == "transaction:false" {
			return true
		}
	}
	return false
}

// dbmateHasUpDirective reports whether content contains a `-- migrate:up`
// directive, marking the file as a dbmate migration.
func dbmateHasUpDirective(content string) bool {
	for line := range strings.SplitSeq(content, "\n") {
		if name, _, ok := dbmateDirective(line); ok && name == "up" {
			return true
		}
	}
	return false
}

// dbmateDirective reports whether line is a dbmate "-- migrate:<name>"
// directive and returns the lowercased directive name with the options that
// followed it. Any options after the name (such as "transaction:false") are
// part of the directive line, not executable SQL, and the caller decides what
// they mean.
func dbmateDirective(line string) (name, options string, ok bool) {
	trimmed := strings.TrimSpace(line)
	const prefix = "-- migrate:"
	if !strings.HasPrefix(strings.ToLower(trimmed), prefix) {
		return "", "", false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	name = rest
	if idx := strings.IndexAny(rest, " \t"); idx >= 0 {
		name, options = rest[:idx], strings.TrimSpace(rest[idx:])
	}
	return strings.ToLower(name), options, true
}
