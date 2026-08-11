package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

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
		path:    filepath.Clean(pathErr.Path),
		pathErr: pathErr,
		cause:   prior,
	}
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

func atlasMigrateLintPathErrorMatches(path, allowedRoot, errorPath string) bool {
	return atlasMigrateLintRootedCleanPath(path, allowedRoot) ==
		atlasMigrateLintRootedCleanPath(errorPath, allowedRoot)
}

func atlasMigrateLintRootedCleanPath(path, allowedRoot string) string {
	clean := filepath.Clean(path)
	if allowedRoot != "" && !filepath.IsAbs(clean) {
		return filepath.Clean(filepath.Join(allowedRoot, clean))
	}
	return clean
}
