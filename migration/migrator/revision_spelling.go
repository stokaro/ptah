package migrator

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
)

// RevisionSpellingError reports Atlas-format revision rows whose version spells
// a migration file's version another way, such as a row 1 for the file
// 001_init.sql.
//
// Atlas compares versions as text, so the row and the file name two different
// revisions to it, while the number they share makes them one migration to a
// reader that compares numbers. The readers of one history then disagree
// about what is applied, and a write that addresses the row by the file's
// spelling misses it: a rollback runs the down migration and leaves the row
// behind. Every command that reads the revision table refuses such a history
// until each row spells its file's version. The error carries the statements
// that respell the rows, and nothing here runs them.
type RevisionSpellingError struct {
	// Table is the revision table, qualified and quoted for its dialect.
	Table string
	// Rows are the rows that spell a file's version another way, in the order
	// the revision table returned them.
	Rows []RevisionSpelling
	// Statements respell every row in Rows. They are complete SQL statements
	// without a terminating semicolon: one UPDATE per row, and on ClickHouse,
	// which keeps the version in the primary key, an INSERT of the respelled row
	// followed by a DELETE of the old one.
	Statements []string
}

// RevisionSpelling is one revision row whose version spells its migration
// file's version another way.
type RevisionSpelling struct {
	// Version is the number the row and the file share.
	Version int64
	// Recorded is the version the row holds, and File the version the migration
	// file's name spells.
	Recorded string
	File     string
}

func (e *RevisionSpellingError) Error() string {
	first := e.Rows[0]
	var b strings.Builder
	fmt.Fprintf(&b, "revision table %s records %d %s under another spelling than %s: %s for %s",
		e.Table,
		len(e.Rows),
		plural(len(e.Rows), "version", "versions"),
		plural(len(e.Rows), "its migration file", "their migration files"),
		first.Recorded,
		first.File,
	)
	if len(e.Rows) > 1 {
		fmt.Fprintf(&b, " and %d more", len(e.Rows)-1)
	}
	b.WriteString("; Atlas compares versions as text, so each of these rows names a different revision " +
		"than its file does. Respell the rows, then run the command again:")
	for _, statement := range e.Statements {
		b.WriteString("\n")
		b.WriteString(statement)
		b.WriteString(";")
	}
	return b.String()
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// respelledRevisions returns the Atlas-format rows whose version is a decimal
// spelling of a number an ordinary Atlas file in the directory spells another
// way.
//
// This is the one reading of that shape. The directory's spelling comes from
// [Migrator.atlasVersionSpelling], the same answer that gives a native row the
// key of its file, so a row and a file are one revision here exactly when the
// provider says the file spells the row's number the way the row does. A
// repeatable or mapped identity is not a spelling of its number and is never
// reported, and neither is a row the directory has no file for: that is a
// missing migration, which [Migrator.verifyAppliedMigrationChecksums] reports.
func (m *Migrator) respelledRevisions(revisions []MigrationRevision) []RevisionSpelling {
	var respelled []RevisionSpelling
	for _, revision := range revisions {
		// A native row holds no version token, so it never reaches the lookup:
		// its number already takes its file's spelling.
		if !allDigitsToken(revision.AtlasVersion) {
			continue
		}
		file := m.atlasVersionSpelling(revision.Version)
		if file == "" || file == revision.AtlasVersion {
			continue
		}
		respelled = append(respelled, RevisionSpelling{
			Version:  revision.Version,
			Recorded: revision.AtlasVersion,
			File:     file,
		})
	}
	return respelled
}

// refuseRespelledRevisions is the refusal both readers of the whole revision
// table apply before any caller acts on the rows.
func (m *Migrator) refuseRespelledRevisions(revisions []MigrationRevision) error {
	respelled := m.respelledRevisions(revisions)
	if len(respelled) == 0 {
		return nil
	}
	table := m.qualifiedMigrationsTable()
	statements := make([]string, 0, len(respelled))
	for _, row := range respelled {
		statements = append(statements, respellRevisionStatements(m.connectionDialect(), table, row)...)
	}
	return &RevisionSpellingError{Table: table, Rows: respelled, Statements: statements}
}

// respellRevisionStatements renders the statements that give one row its
// file's spelling. Both versions are decimal digits, which is what makes
// quoting them as literals safe.
func respellRevisionStatements(dialect, table string, row RevisionSpelling) []string {
	if platform.NormalizeDialect(dialect) == platform.ClickHouse {
		return []string{
			fmt.Sprintf("INSERT INTO %s SELECT '%s', * EXCEPT (version) FROM %s WHERE version = '%s'",
				table, row.File, table, row.Recorded),
			fmt.Sprintf("ALTER TABLE %s DELETE WHERE version = '%s' SETTINGS mutations_sync = 1",
				table, row.Recorded),
		}
	}
	return []string{
		fmt.Sprintf("UPDATE %s SET version = '%s' WHERE version = '%s'", table, row.File, row.Recorded),
	}
}
