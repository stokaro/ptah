package atlasmigrateimport

import (
	"errors"
	"fmt"
	"strings"
)

// This file is the write half of the per-format directory rules the rest of the
// package reads with: given a version and a name, the files an EMPTY migration
// consists of when it is written in a source tool's own convention.
//
// It lives beside [SumFileNames] and the loader regexes on purpose. The names
// emitted here have to be names those two accept — a `migrate new` that wrote a
// file the directory's own `migrate hash` does not cover, or that
// `migrate apply` cannot load, would hand the operator a directory this binary
// refuses to read back. Keeping the emitter in the package that owns the
// reading rules makes that a property one test can state over both
// (TestSkeletonFilesAreCoveredAndLoadable) rather than an agreement between two
// packages that has to be re-checked whenever either moves.
//
// Every name and every byte below was measured against the pinned Atlas CE
// v1.3.0 executable on 2026-08-06, by running `atlas migrate new addcol --dir
// file://<empty> --dir-format <format>` and dumping the result with `od -c`:
//
//	golang-migrate  20260806071434_addcol.up.sql      (empty)
//	                20260806071434_addcol.down.sql    (empty)
//	flyway          V20260806071435__addcol.sql       (empty)
//	                U20260806071435__addcol.sql       (empty)
//	goose           20260806071435_addcol.sql         "-- +goose Up\n\n-- +goose Down\n"
//	dbmate          20260806071435_addcol.sql         "-- migrate:up\n\n-- migrate:down\n"
//	liquibase       20260806071435_addcol.sql         "--liquibase formatted sql" (no newline)
//
// The atlas layout is deliberately absent. A native Atlas directory is written
// by `ptah migrations create`, which the compat surface forwards to unchanged,
// so adding a row for it here would be a second definition of a file that
// already has one.
const (
	// gooseSkeletonBody is the directive pair Goose needs to recognize a file
	// as a migration at all. An empty Goose file is not an empty migration, it
	// is an unparseable one.
	gooseSkeletonBody = "-- +goose Up\n\n-- +goose Down\n"
	// dbmateSkeletonBody is dbmate's equivalent directive pair.
	dbmateSkeletonBody = "-- migrate:up\n\n-- migrate:down\n"
	// liquibaseSkeletonBody is the header Liquibase requires on a formatted-SQL
	// changelog. It carries no trailing newline, which is measured rather than
	// tidied: the byte is inside the file the integrity hash chains, so adding
	// one would produce an atlas.sum entry the community binary never writes.
	liquibaseSkeletonBody = "--liquibase formatted sql"
)

// SkeletonFile is one file of the empty migration `migrate new` creates.
type SkeletonFile struct {
	// Name is the file name relative to the migration directory. It is always a
	// single path element.
	Name string
	// Content is the file's exact bytes.
	Content string
}

// SkeletonFiles returns the files an empty migration named name at version
// consists of when written for format, in the order they should be created:
// the file the integrity hash covers first, then the rollback file for the two
// layouts that keep one.
//
// The order matters to the caller rather than to the layout. Writing the
// covered file first means an interrupted `migrate new` leaves behind the file
// `migrate hash` would have covered, never an orphaned rollback half that the
// integrity file cannot see.
//
// format must name an external layout. The native Atlas layout is written by
// the forwarded `ptah migrations create`, and asking for it here is a caller
// bug rather than an unsupported combination.
//
// name is required and must be a single, plain path element. Both refusals are
// narrower than the community binary, which writes `20260806071553.up.sql` for
// an omitted name and reports `open .../20260806071901_add/col.up.sql: no such
// file or directory` for a name holding a separator; see
// [ErrSkeletonNameRequired] and [ErrSkeletonNameNotAnElement] for the measured
// reason each one is drawn here instead.
func SkeletonFiles(format Format, version int64, name string) ([]SkeletonFile, error) {
	if err := validateExternalFormat(format); err != nil {
		return nil, err
	}
	// The atlas refusal precedes the name check, so asking this package for a
	// layout it does not own reports that rather than a complaint about the
	// name it was going to reject next.
	if format == FormatAtlas {
		return nil, errNoAtlasSkeleton
	}
	if err := validateSkeletonName(name); err != nil {
		return nil, err
	}

	switch format {
	case FormatGolangMigrate:
		stem := fmt.Sprintf("%d_%s", version, name)
		return []SkeletonFile{
			{Name: stem + ".up.sql"},
			{Name: stem + ".down.sql"},
		}, nil
	case FormatFlyway:
		stem := fmt.Sprintf("%d__%s", version, name)
		return []SkeletonFile{
			{Name: "V" + stem + ".sql"},
			{Name: "U" + stem + ".sql"},
		}, nil
	case FormatGoose:
		return []SkeletonFile{{Name: numberedSkeletonName(version, name), Content: gooseSkeletonBody}}, nil
	case FormatDBMate:
		return []SkeletonFile{{Name: numberedSkeletonName(version, name), Content: dbmateSkeletonBody}}, nil
	case FormatLiquibase:
		return []SkeletonFile{{Name: numberedSkeletonName(version, name), Content: liquibaseSkeletonBody}}, nil
	case FormatAtlas:
		// Unreachable: rejected above. Listed so adding a Format constant
		// without deciding what it writes fails the exhaustiveness linter here.
		return nil, errNoAtlasSkeleton
	default:
		return nil, fmt.Errorf("unknown migration import format %q", format)
	}
}

// errNoAtlasSkeleton reports that the native Atlas layout has no skeleton to
// emit here. `ptah migrations create` owns that file name, and the compat
// surface forwards to it; a second definition in this package would be a rule
// with two homes.
var errNoAtlasSkeleton = errors.New("the atlas migration directory format writes no skeleton files")

func numberedSkeletonName(version int64, name string) string {
	return fmt.Sprintf("%d_%s.sql", version, name)
}

// skeletonNameError is the error kind [SkeletonFiles] rejects a migration name
// with, so a caller can turn it into the diagnostic its surface prints without
// matching on message text.
type skeletonNameError string

func (e skeletonNameError) Error() string { return string(e) }

const (
	// ErrSkeletonNameRequired reports that no migration name was supplied.
	//
	// The community binary accepts an omitted name and writes the version
	// alone: `migrate new --dir-format golang-migrate` produces
	// `20260806071553.up.sql`. Ptah refuses instead, because it cannot read that
	// file back. Measured on 2026-08-06 over a directory holding exactly the
	// nameless file each layout's own `migrate new` writes, hashed by the
	// community binary and applied by both:
	//
	//	golang-migrate  CE 0, version "20260806071553.up"  ptah 1, "no importable migration files found"
	//	goose           CE 0, version "20260806071553"     ptah 1, "no importable migration files found"
	//	liquibase       CE 0, version "20260806071553"     ptah 1, "no importable migration files found"
	//	dbmate          CE 0, version "20260806071553"     ptah 1, "no importable migration files found"
	//	flyway          CE 0, version "20260806071553"     ptah 0, version 5438407949371077319
	//
	// Writing the file would therefore hand the operator a directory that this
	// binary's own `migrate apply` refuses on four layouts out of five, and
	// records under a different version on the fifth. Refusing to create it is
	// exit 1 where the community binary exits 0 — the strict direction — and it
	// is the direction to take while the readers disagree.
	ErrSkeletonNameRequired = skeletonNameError("migration name is required")
	// ErrSkeletonNameNotAnElement reports that the migration name is not usable
	// as a single file-name element.
	//
	// The community binary interpolates the name into a path and lets the write
	// fail: `migrate new 'add/col' --dir-format golang-migrate` exits 1 with
	// `open .../20260806071901_add/col.up.sql: no such file or directory`.
	// Rejecting the name up front keeps the same exit code without ever
	// resolving an operator-supplied path fragment against the migration
	// directory.
	ErrSkeletonNameNotAnElement = skeletonNameError(
		"migration name must be a single file name element, without a path separator or control character",
	)
)

// validateSkeletonName refuses a migration name that cannot become one file
// name in the migration directory.
//
// The name is otherwise passed through verbatim, which is what the community
// binary does: `add col`, `add.col`, `ADD-col` and `añb` all reach the file
// name unchanged there. It differs from the sanitizing rule the native
// atlas-format path applies (spaces become `-`, everything outside
// [A-Za-z0-9_.-] is dropped, so `añb` becomes `ab`); that rule predates this
// path and changing it would move the native layout's file names, which is not
// what this is for.
func validateSkeletonName(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrSkeletonNameRequired
	}
	if strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return ErrSkeletonNameNotAnElement
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return ErrSkeletonNameNotAnElement
		}
	}
	return nil
}
