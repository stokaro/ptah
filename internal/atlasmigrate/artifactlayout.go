package atlasmigrate

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// This file is the write half of `migrate diff`'s layout rules: given the
// planned migrations of one run, the FILES they become in each directory
// convention the verb can be pointed at.
//
// It is deliberately the naming [atlasmigrateimport.SkeletonFiles] already
// emits for `migrate new`, and for the same reason that file states: a name
// this composes has to be a name [atlasmigrateimport.SumFileNames] covers and
// the loader accepts, or the run would hand the operator a directory its own
// `migrate hash` does not cover and its own `migrate apply` cannot read.
// TestComposeMigrationArtifactsAreCoveredAndLoadable states that over both.
//
// Every shape below was measured against the pinned community binary v1.3.0 on
// 2026-08-08, by running `migrate diff demo --dir 'file://<empty>?format=<f>'`
// against a PostgreSQL dev database with one CREATE TABLE as the desired state:
//
//	golang-migrate  20260808232952_demo.up.sql    "-- create \"orders\" table\nCREATE TABLE …"
//	                20260808232952_demo.down.sql  "-- reverse: create \"orders\" table\nDROP TABLE …"
//	flyway          V20260808233006__demo.sql     forward
//	                U20260808233006__demo.sql     reverse
//	goose           20260808233014_demo.sql       "-- +goose Up\n…\n\n-- +goose Down\n…"
//	dbmate          20260808233023_demo.sql       "-- migrate:up\n…\n\n-- migrate:down\n…"
//	liquibase       20260808233032_demo.sql       "--liquibase formatted sql\n--changeset atlas:<v>-1\n…\n--rollback: …"
//
// In every case that binary's atlas.sum covered only the forward half — the
// `.up.sql`, the `V…` file, or the single file — which is what
// [atlasmigrateimport.SumFileNames] already computes per layout.
//
// The SQL text itself is Ptah's own renderer's, not that binary's, on every
// layout including the native one. Matching the layout is what this closes;
// matching the generated DDL byte for byte never was, and is not, the contract.
const (
	// gooseUpDirective and gooseDownDirective are the annotations Goose needs
	// to recognize the two halves of a migration. A Goose file without them is
	// not an empty migration, it is an unparseable one.
	gooseUpDirective   = "-- +goose Up"
	gooseDownDirective = "-- +goose Down"
	// gooseNoTransactionDirective opts the whole Goose migration file out of
	// transaction wrapping. Because Goose keeps both directions in that file,
	// one leading directive represents a requirement from either half.
	gooseNoTransactionDirective = "-- +goose NO TRANSACTION"
	// dbmateUpDirective and dbmateDownDirective are dbmate's equivalents.
	dbmateUpDirective   = "-- migrate:up"
	dbmateDownDirective = "-- migrate:down"
	// dbmateNoTransactionOption is dbmate's own spelling for a migration half
	// that must not be wrapped in a transaction. dbmate documents it as an
	// option on the directive line, which is why this layout can express the
	// requirement where golang-migrate cannot (stokaro/ptah#1630).
	dbmateNoTransactionOption = " transaction:false"
	// liquibaseHeader is the first line of a Liquibase formatted-SQL changelog.
	liquibaseHeader = "--liquibase formatted sql"
	// liquibaseChangesetAuthor is the author half of the `author:id` pair on
	// the changeset line.
	//
	// It is `atlas` rather than `ptah` because the value identifies the FORMAT
	// convention a reader of this directory has to recognize, not the program
	// that wrote the line: a directory this verb writes is meant to be one the
	// community binary's own `migrate hash`, `validate` and `apply` read back,
	// and that binary writes `--changeset atlas:<version>-<n>`.
	liquibaseChangesetAuthor = "atlas"
	// liquibaseRollbackPrefix introduces one line of a changeset's rollback.
	// Liquibase concatenates consecutive rollback lines into one rollback
	// statement, which is why a multi-line statement is emitted as several of
	// these rather than as one line carrying newlines.
	liquibaseRollbackPrefix = "--rollback: "
)

// composeMigrationArtifacts turns the planned migrations of one `migrate diff`
// run into the files a directory read as format consists of, in creation order.
//
// version is the first version of the run; a plan split into several files
// takes version, version+1, ... exactly as the native Atlas layout always has,
// so a caller's collision check over `len(contents)` consecutive versions
// stays correct for every layout.
//
// The forward file is emitted before the rollback file of the same migration.
// The order is the caller's, not the layout's: staging writes them in this
// order, so an interrupted run leaves behind the file atlas.sum would have
// covered rather than an orphaned rollback half the integrity file cannot see.
func composeMigrationArtifacts(
	format atlasmigrateimport.Format,
	name string,
	version int64,
	contents []MigrationFileContent,
) ([]PublicationArtifact, error) {
	if len(contents) == 0 {
		return nil, fmt.Errorf("migration SQL is empty")
	}
	artifacts := make([]PublicationArtifact, 0, 2*len(contents))
	for i, content := range contents {
		if err := validateForeignTransactionMode(format, content); err != nil {
			return nil, err
		}
		fileVersion := version + int64(i)
		slug := migrationSlug(name + content.NameSuffix)
		composed, err := composeMigrationArtifact(format, slug, fileVersion, content)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, composed...)
	}
	return artifacts, nil
}

func composeMigrationArtifact(
	format atlasmigrateimport.Format,
	slug string,
	version int64,
	content MigrationFileContent,
) ([]PublicationArtifact, error) {
	switch format {
	case atlasmigrateimport.FormatAtlas, "":
		return []PublicationArtifact{{
			Name:     fmt.Sprintf("%d_%s.sql", version, slug),
			Contents: []byte(content.SQL),
		}}, nil
	case atlasmigrateimport.FormatGolangMigrate:
		stem := fmt.Sprintf("%d_%s", version, slug)
		return pairedArtifacts(stem+".up.sql", stem+".down.sql", content), nil
	case atlasmigrateimport.FormatFlyway:
		stem := fmt.Sprintf("%d__%s", version, slug)
		return pairedArtifacts("V"+stem+".sql", "U"+stem+".sql", content), nil
	case atlasmigrateimport.FormatGoose:
		return gooseArtifact(
			fmt.Sprintf("%d_%s.sql", version, slug),
			content,
		), nil
	case atlasmigrateimport.FormatDBMate:
		return dbmateArtifact(fmt.Sprintf("%d_%s.sql", version, slug), content), nil
	case atlasmigrateimport.FormatLiquibase:
		return composeLiquibaseArtifact(fmt.Sprintf("%d_%s.sql", version, slug), version, content), nil
	default:
		return nil, fmt.Errorf("unknown migration import format %q", format)
	}
}

// dbmateArtifact composes the layout that marks each direction separately.
//
// dbmate keeps both directions in one file under a directive each and takes
// `transaction:false` as an option on that line, so a forward requirement and a
// rollback requirement are independent. Goose, below, has only a whole-file
// directive and has to opt the entire file out when either half needs it.
//
// Measured against dbmate v2.35.0 on PostgreSQL 17: a migration carrying the
// option applies a CREATE INDEX CONCURRENTLY that fails without it. Note the
// statement still has to be the only one in its send, but that is PostgreSQL's
// rule and not dbmate's -- a multi-statement send is an implicit transaction
// block on every layout, the native one included (stokaro/ptah#1630).
func dbmateArtifact(name string, content MigrationFileContent) []PublicationArtifact {
	up, down := dbmateUpDirective, dbmateDownDirective
	if content.NoTransaction {
		up += dbmateNoTransactionOption
	}
	if content.ReverseNoTransaction {
		down += dbmateNoTransactionOption
	}
	return directiveArtifact(name, up, down, content)
}

func gooseArtifact(name string, content MigrationFileContent) []PublicationArtifact {
	// BuildMigrationFileContents carries Atlas's directive in SQL because the
	// native layout writes that string directly. Goose needs its own whole-file
	// directive instead, so do not leak Atlas metadata into the source format.
	content.SQL = strings.TrimPrefix(content.SQL, AtlasTxModeNoneDirective+"\n\n")
	artifacts := directiveArtifact(name, gooseUpDirective, gooseDownDirective, content)
	if !content.NoTransaction && !content.ReverseNoTransaction {
		return artifacts
	}
	artifacts[0].Contents = append(
		[]byte(gooseNoTransactionDirective+"\n"),
		artifacts[0].Contents...,
	)
	return artifacts
}

// pairedArtifacts composes the two layouts that keep the rollback in a file of
// its own.
//
// The rollback file is written even when the run planned no reverse, and it is
// then empty. That is what the community binary's `migrate new` writes on these
// two layouts, and it is what keeps the pair complete: a golang-migrate
// migration whose `.down.sql` is missing is a migration the source tool cannot
// roll back past, while an empty one is a no-op it can.
func pairedArtifacts(upName, downName string, content MigrationFileContent) []PublicationArtifact {
	return []PublicationArtifact{
		{Name: upName, Contents: []byte(content.SQL)},
		{Name: downName, Contents: []byte(content.DownSQL)},
	}
}

// directiveArtifact composes the two layouts that keep both halves in one file
// under a directive each.
//
// The down section is emitted even when it is empty, because the directive is
// what makes the file parseable rather than what makes it useful: a Goose file
// carrying only `-- +goose Up` is a migration the source tool cannot roll back
// past, and both readers expect the pair.
func directiveArtifact(
	name, upDirective, downDirective string,
	content MigrationFileContent,
) []PublicationArtifact {
	var body strings.Builder
	body.WriteString(upDirective)
	body.WriteString("\n")
	body.WriteString(ensureTrailingNewline(content.SQL))
	body.WriteString("\n")
	body.WriteString(downDirective)
	body.WriteString("\n")
	body.WriteString(ensureTrailingNewline(content.DownSQL))
	return []PublicationArtifact{{Name: name, Contents: []byte(body.String())}}
}

// composeLiquibaseArtifact composes the one layout whose rollback is attached
// to a changeset rather than appended as a block.
//
// # One changeset, where the community binary writes one per statement
//
// Measured on the pinned v1.3.0, a two-table diff produces two changesets there
// — `atlas:<version>-1` and `atlas:<version>-2` — each carrying its own
// forward statement and its own `--rollback:` line. This writes one changeset
// carrying the whole migration and every rollback line for it, and the
// difference is deliberate.
//
// Pairing forward statement i with reverse statement i would be a guess. Ptah
// plans the reverse of the RUN — [DiffOptions.PlanBidirectional] answers the
// whole forward and reverse plan, ordered by reverse dependency
// — so the two lists are not index-aligned and need not even be the same
// length: one added table with two indexes reverses into one DROP TABLE.
// Emitting a changeset per forward statement would therefore attach some
// statement's rollback to a different statement, and rolling back a single
// changeset would run SQL that does not undo it. One changeset per migration is
// the granularity Ptah can state truthfully: rolling it back runs the reverse
// of exactly what applying it ran.
//
// The cost is per-changeset rollback granularity, which is the ability to undo
// part of one migration. Nothing else moves: the file name, the covered set,
// the header and the changeset syntax are the layout's, and the rollback is
// complete.
//
// A statement spanning several lines becomes several `--rollback:` lines, which
// Liquibase concatenates into one rollback statement. Emitting it as a single
// line carrying newlines would end the rollback at the first one and silently
// drop the rest.
func composeLiquibaseArtifact(
	name string,
	version int64,
	content MigrationFileContent,
) []PublicationArtifact {
	var body strings.Builder
	body.WriteString(liquibaseHeader)
	body.WriteString("\n")
	fmt.Fprintf(&body, "--changeset %s:%d-1\n", liquibaseChangesetAuthor, version)
	for _, statement := range content.Statements {
		body.WriteString(strings.TrimRight(statement, "\n"))
		body.WriteString(";\n")
	}
	for _, statement := range content.ReverseStatements {
		for line := range strings.SplitSeq(strings.TrimRight(statement, "\n")+";", "\n") {
			body.WriteString(liquibaseRollbackPrefix)
			body.WriteString(line)
			body.WriteString("\n")
		}
	}
	return []PublicationArtifact{{Name: name, Contents: []byte(body.String())}}
}

func ensureTrailingNewline(sql string) string {
	if sql == "" || strings.HasSuffix(sql, "\n") {
		return sql
	}
	return sql + "\n"
}
