// Package dbverify implements "ptah db verify", which evaluates release-state
// assertions against an existing database and changes nothing in it.
//
// `migrations status` answers whether the recorded history matches the
// directory, and `schema drift` whether the structure matches the declaration.
// Neither answers what a release rests on: did the change produce the result it
// was for. A migration that adds a column and backfills it can succeed
// statement by statement, match the declaration, and report no drift, while the
// backfill predicate was wrong and a subset of rows kept a null. Drift compares
// structure; this verb evaluates requirements about the data
// (stokaro/ptah#3404).
//
// The requirements are written as the `-- +ptah check` directives a migration
// already uses, so versioned migrations and direct schema changes express a
// requirement the same way rather than growing two dialects for one idea.
package dbverify

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
	"ptah.run/migration/migrator"
)

// checksFileExtension is the only extension a checks directory contributes.
// The directory holds SQL files carrying directives, and a README or a stray
// editor backup beside them must not be parsed as one.
const checksFileExtension = ".sql"

// sourcedCheck is a check together with the file it was read from, so a report
// can name where a requirement is written rather than only what it asserts.
type sourcedCheck struct {
	Check  migrator.Check
	Source string
}

// loadChecks reads every check from path in a deterministic order.
//
// path is a file, a directory of `.sql` files, or an `oci://` reference to a
// published schema artifact. A directory contributes its files sorted by name,
// and each file contributes its checks in the order written. The order is
// fixed rather than incidental because the report is read as a list, and a
// list whose order changes between runs cannot be diffed.
//
// The artifact form is what closes the gap between approval and evaluation: a
// reference pinned by digest names one immutable set of bytes, so the checks a
// reviewer approved are the checks that run. A path names whatever is on disk
// at the time of the run (stokaro/ptah#3458).
//
// dialect selects the lexer rules the directive scanner needs; it comes from
// the connection, so a checks source is parsed by the rules of the engine it
// will be evaluated against.
func loadChecks(ctx context.Context, path, dialect string, plainHTTP bool) ([]sourcedCheck, error) {
	if strings.HasPrefix(path, ociartifact.Scheme) {
		return loadChecksFromArtifact(ctx, path, dialect, plainHTTP)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read checks from %s: %w", path, err)
	}
	if !info.IsDir() {
		return loadChecksFile(path, dialect)
	}
	files, err := checksDirectoryFiles(path)
	if err != nil {
		return nil, err
	}
	var checks []sourcedCheck
	for _, file := range files {
		parsed, err := loadChecksFile(file, dialect)
		if err != nil {
			return nil, err
		}
		checks = append(checks, parsed...)
	}
	return checks, nil
}

func checksDirectoryFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read checks directory %s: %w", directory, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.EqualFold(filepath.Ext(entry.Name()), checksFileExtension) {
			continue
		}
		files = append(files, filepath.Join(directory, entry.Name()))
	}
	slices.Sort(files)
	return files, nil
}

func loadChecksFile(path, dialect string) ([]sourcedCheck, error) {
	source, err := os.ReadFile(path) // #nosec G304 -- the operator named this file
	if err != nil {
		return nil, fmt.Errorf("read checks file %s: %w", path, err)
	}
	return parseChecks(source, path, dialect)
}

// parseChecks turns one source into checks that name where they came from,
// whether that is a path or a pinned artifact reference.
func parseChecks(source []byte, origin, dialect string) ([]sourcedCheck, error) {
	parsed, err := migrator.ParseChecks(string(source), dialect)
	if err != nil {
		return nil, fmt.Errorf("checks file %s: %w", origin, err)
	}
	checks := make([]sourcedCheck, 0, len(parsed))
	for _, check := range parsed {
		checks = append(checks, sourcedCheck{Check: check, Source: origin})
	}
	return checks, nil
}

// loadChecksFromArtifact reads the checks layer of a published schema
// artifact.
//
// An artifact that carries no checks layer is refused rather than reported as
// a run with nothing to verify: the operator named an artifact because they
// expected its checks, and "no assertions" and "this artifact publishes none"
// are different answers.
func loadChecksFromArtifact(
	ctx context.Context,
	reference, dialect string,
	plainHTTP bool,
) ([]sourcedCheck, error) {
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return nil, err
	}
	artifact, err := schemaartifact.Pull(ctx, client, reference)
	if err != nil {
		return nil, fmt.Errorf("read checks from %s: %w", reference, err)
	}
	if len(artifact.Checks) == 0 {
		return nil, fmt.Errorf(
			"schema artifact %s carries no %s layer, so it publishes no release assertions",
			artifact.Reference.PinnedString(artifact.Descriptor.Digest.String()),
			schemaartifact.ChecksFileName,
		)
	}
	return parseChecks(
		artifact.Checks,
		artifact.Reference.PinnedString(artifact.Descriptor.Digest.String()),
		dialect,
	)
}
