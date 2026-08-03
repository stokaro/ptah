package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file holds the atlas.sum integrity gate for NATIVE Atlas migration
// directories, shared by every compat verb that reads one.
//
// It is deliberately verb-neutral. The gate started life inside
// `migrate apply` (#955, #970, #972), and keeping it there is what produced
// #974: `migrate status` and `migrate set` read the same directories through
// the same capture step and reported normally on a directory Atlas refuses,
// because the rule was a private detail of one call site rather than one rule
// with several call sites. Measured against the pinned community binary
// v1.3.0, an unhashed one-migration directory is refused by `migrate status`,
// `migrate set`, `migrate apply` and `migrate validate` alike, so there is one
// predicate to express, not four.
//
// A verb that can also read a foreign layout keeps its own dispatcher, because
// the covered file set differs per layout; see verifyAtlasApplyChecksum in
// migrate_apply.go. `migrate status` and `migrate set` reject both spellings
// of a non-Atlas layout (`?format=` on --dir and a non-atlas --dir-format)
// before they reach the gate, so only the native branch is reachable there.

// verifyNativeAtlasDirChecksum enforces the atlas.sum integrity gate on a
// captured NATIVE Atlas migration filesystem: a missing atlas.sum and a failed
// verification both refuse the command, with output byte-identical to
// `migrate validate` on the same directory.
//
// The refusal is returned as-is, never wrapped through cmdutil.Fail or
// failAtlasCommand: the migratevalidate helpers already wrote the guidance
// block to stdout and return an exitcode.New(1, ...) error whose message the
// root command prints as `Error: checksum file not found`. Re-wrapping would
// prepend `error: ` and move the text to the other stream, losing the byte
// parity that is the point of routing through those helpers.
//
// Call it immediately after the directory snapshot and before connecting to
// the database. That ordering is measured, not stylistic: the community binary
// prints the checksum refusal even when --url is unreachable, and on
// `migrate set` even when the positional arity is wrong.
func verifyNativeAtlasDirChecksum(cmd *cobra.Command, fsys fs.FS) error {
	result, hashed, err := migratesum.VerifyHashed(fsys, migrator.MigrationDirFormatAtlas)
	switch {
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		// A malformed atlas.sum has no entry-level mismatch to point at; the
		// validate surface reports it as a plain checksum mismatch.
		return migratevalidate.FailAtlasChecksumMismatch(cmd, nil)
	case errors.Is(err, migratesum.ErrCoveredEntryUnreadable):
		// A covered entry that is a directory (#991). The community binary
		// prints the checksum preamble here, not a bare error, so routing it
		// anywhere else would lose the stream layout this gate exists to match.
		return migratevalidate.FailAtlasChecksumUnreadableEntry(cmd, err)
	case err != nil:
		return err
	case !hashed:
		return failUnhashedAtlasDir(cmd, fsys)
	case !result.OK():
		return migratevalidate.FailAtlasChecksumMismatch(cmd, result.FirstMismatch())
	}
	return nil
}

// failUnhashedAtlasDir refuses a NATIVE Atlas directory that carries no
// atlas.sum, unless it holds no SQL file anywhere in its tree.
//
// The exemption exists because a directory with nothing to execute is not a
// checksum error: the community binary reports success and exits 0 on an empty
// directory and on one holding only non-SQL files, so a CI bootstrap that
// creates an empty migrations directory keeps working (#970). The gate fires on
// the presence of a SQL file, not on parseable versioned migrations — an
// unhashed directory holding only `foo.sql` is refused there, and so it is
// here.
//
// The scan is recursive even though the community binary's is not. That binary
// ignores subdirectories entirely, so a migration one level down is
// nothing-to-execute for it but is executed by Ptah's registrar, which
// recurses. Keying the exemption on the shallower view would let exactly the
// unhashed migrations this gate exists to stop run unverified. The result is
// exit 1 where the community binary exits 0 for that layout — the safe side of
// a pre-existing divergence in what the two tools consider a migration (#976),
// and now visible on `migrate status` and `migrate set` too, where both tools
// previously exited 0 (with different pending counts). Pinned as a named
// known-divergence test rather than left to be discovered.
//
// That asymmetry is why a converted directory uses a different predicate
// instead of reusing this one. It is the same asymmetry that produced the #972
// commit-2 regression tracked as #976, so it is worth stating in both places:
// on the converted path Ptah's own loader reads only top-level files, so the
// shallow per-format covered set is both correct and precise there, and
// recursing would refuse layouts both tools agree have nothing to execute.
// Flyway is the exception that proves it is about the covered set rather than
// about depth — sub/V2__nested.sql is hashed, so an unhashed Flyway directory
// whose only migration sits one level down is refused on both tools.
func failUnhashedAtlasDir(cmd *cobra.Command, fsys fs.FS) error {
	var foundSQL bool
	err := fs.WalkDir(fsys, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".sql") {
			return nil
		}
		foundSQL = true
		return fs.SkipAll
	})
	if err != nil {
		return fmt.Errorf("scan migration directory for SQL files: %w", err)
	}
	if !foundSQL {
		return nil
	}
	return migratevalidate.FailAtlasChecksumFileNotFound(cmd)
}
