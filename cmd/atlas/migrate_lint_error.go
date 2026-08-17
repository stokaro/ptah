package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/migrationlintreport"
)

// atlasMigrateLintGitDisplayError adapts a failed `git diff` performed for
// --git-base to the Atlas-compatible surface. Unwrap keeps the full invocation,
// git's own output and the process failure available to callers that inspect
// the chain, so only the printed bytes change.
type atlasMigrateLintGitDisplayError struct {
	subcommand string
	gitErr     *migrationlintreport.GitCommandError
	cause      error
}

func (e *atlasMigrateLintGitDisplayError) Error() string {
	return fmt.Sprintf("git %s: %v", e.subcommand, e.gitErr.Err)
}

func (e *atlasMigrateLintGitDisplayError) Unwrap() error { return e.cause }

// atlasMigrateLintGitError renders a failed changeset selection the way the
// pinned community binary v1.3.0 renders it: the git verb and the process
// status, with neither the argument vector nor git's own stderr (#1235 cell
// 9.12).
//
// Only the `diff` invocation is adapted, because only that one is measured. A
// run started outside a git repository fails this package's `rev-parse`
// preflight, where the pinned binary instead reaches its own `git diff` and
// reports a different status entirely -- 129 against this preflight's 128. The
// two are not the same event, so rendering one as the other would report a
// status no process returned. That case keeps the native diagnostic, which at
// least names what actually failed.
//
// Native `ptah migrations lint` never reaches this adapter and keeps the full
// invocation, which is the reproducible form.
func atlasMigrateLintGitError(err error) error {
	var gitErr *migrationlintreport.GitCommandError
	if !errors.As(err, &gitErr) || gitErr.Subcommand != "diff" {
		return err
	}
	return &atlasMigrateLintGitDisplayError{
		subcommand: gitErr.Subcommand,
		gitErr:     gitErr,
		cause:      err,
	}
}

type atlasMigrateLintMissingDirectoryError struct {
	path    string
	pathErr *os.PathError
	cause   error
}

func (e *atlasMigrateLintMissingDirectoryError) Error() string {
	return fmt.Sprintf("sql/migrate: stat %s: %v", e.path, e.pathErr.Err)
}

func (e *atlasMigrateLintMissingDirectoryError) Unwrap() error {
	return e.cause
}

// atlasMigrateLintDirCaptureError adapts only the missing-directory error from
// the rooted open performed by migrationsource.CaptureLocal. Its displayed
// diagnostic matches the pinned Atlas binary while the complete prior error
// chain remains available to errors.Is and errors.As.
func atlasMigrateLintDirCaptureError(path, allowedRoot string, captureErr error) error {
	prior := fmt.Errorf("atlas migrate lint --dir: %w", captureErr)
	pathErr, ok := atlasMigrateLintMissingDirectoryPathError(captureErr)
	if !ok || !atlasMigrateLintPathErrorMatches(path, allowedRoot, pathErr.Path) {
		return prior
	}
	return &atlasMigrateLintMissingDirectoryError{
		path:    atlasMigrateLintDisplayPath(path, pathErr.Path),
		pathErr: pathErr,
		cause:   prior,
	}
}

// atlasMigrateLintDisplayPath picks which of the two spellings of one directory
// the diagnostic prints: the relative one, because that is the one an operator
// recognizes, and the community binary prints `stat migrations` rather than
// `stat /tmp/x/001/migrations`.
//
// Which of the two IS relative depends on how the directory was opened. Opened
// through an explicit root, the os.PathError carries the path relative to that
// root while the caller holds the absolute one. Opened as a CLI path, it is the
// other way round since stokaro/ptah#1622 removed the working-directory root
// that used to keep the error's own path relative. Neither caller knows which
// case it is in, so the choice is made here from the paths themselves.
func atlasMigrateLintDisplayPath(typed, observed string) string {
	if !filepath.IsAbs(observed) {
		return filepath.Clean(observed)
	}
	return filepath.Clean(typed)
}

func atlasMigrateLintMissingDirectoryPathError(err error) (*os.PathError, bool) {
	direct, ok := err.(interface{ Unwrap() error })
	if !ok {
		return nil, false
	}
	pathErr, ok := direct.Unwrap().(*os.PathError)
	if !ok || err.Error() != "open migrations directory: "+pathErr.Error() {
		return nil, false
	}
	if pathErr.Op != "open" && pathErr.Op != "openat" {
		return nil, false
	}
	if !errors.Is(pathErr.Err, fs.ErrNotExist) {
		return nil, false
	}
	return pathErr, true
}

// atlasMigrateLintPathErrorMatches reports whether the failing open was of the
// directory the operator named, rather than of something reached from inside
// it: only the first is the community binary's `stat` diagnostic.
//
// Both sides are made absolute before comparing. The two used to be compared as
// written, which worked while a relative CLI path was opened through a
// working-directory root and the os.PathError echoed it back relative. With
// that root gone (stokaro/ptah#1622) the error carries an absolute path, and
// comparing "nope" to "/tmp/x/nope" as strings answers "different directory"
// for the same directory.
func atlasMigrateLintPathErrorMatches(path, allowedRoot, errorPath string) bool {
	return atlasMigrateLintRootedCleanPath(path, allowedRoot) ==
		atlasMigrateLintRootedCleanPath(errorPath, allowedRoot)
}

func atlasMigrateLintRootedCleanPath(path, allowedRoot string) string {
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) {
		return clean
	}
	root := allowedRoot
	if root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return clean
		}
		root = cwd
	}
	return filepath.Clean(filepath.Join(root, clean))
}
