// Package sqlite decides whether two SQLite URLs address one database: the
// database is a file, so two URLs address one database when they name one
// file, or both name a database in memory.
package sqlite

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
)

// Services returns SQLite's services.
func Services() dburlext.Services {
	return dburlext.Services{Dialects: []string{platform.SQLite}, Databases: databases{}}
}

type databases struct{}

// SameDatabase compares the files the URLs name: by identity on disk when
// both exist, so a hard link or a symlink names the file it reaches, and
// otherwise by the absolute path with every symlink of it resolved. A
// database in memory is the same as another one in memory and as no file.
func (databases) SameDatabase(left, right *url.URL) (bool, error) {
	leftPath, leftMemory, err := databasePath(left)
	if err != nil {
		return false, err
	}
	rightPath, rightMemory, err := databasePath(right)
	if err != nil {
		return false, err
	}
	if leftMemory || rightMemory {
		return leftMemory && rightMemory, nil
	}
	leftInfo, leftErr := os.Stat(leftPath)    // #nosec G703 -- the database file the caller's URL names
	rightInfo, rightErr := os.Stat(rightPath) // #nosec G703 -- the database file the caller's URL names
	if leftErr == nil && rightErr == nil {
		return os.SameFile(leftInfo, rightInfo), nil
	}
	leftIdentity, err := identity(left)
	if err != nil {
		return false, err
	}
	rightIdentity, err := identity(right)
	if err != nil {
		return false, err
	}
	return leftIdentity == rightIdentity, nil
}

func identity(parsed *url.URL) (string, error) {
	path, memory, err := databasePath(parsed)
	if err != nil {
		return "", err
	}
	if memory {
		return "sqlite\x00:memory:", nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("resolve SQLite database path")
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		absolute = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("resolve SQLite database path")
	}
	return "sqlite\x00" + filepath.Clean(absolute), nil
}

// databasePath is the file a URL names, and whether it names a database in
// memory instead.
func databasePath(parsed *url.URL) (string, bool, error) {
	path := parsed.Opaque
	fileURI := strings.HasPrefix(path, "file:")
	switch {
	case path != "":
		// Opaque SQLite URLs already contain the driver path verbatim.
	case parsed.Host != "" && parsed.Path != "":
		path = parsed.Host + parsed.Path
	case parsed.Host != "":
		path = parsed.Host
	default:
		path = parsed.Path
	}
	path = strings.TrimPrefix(path, "file:")
	if fileURI {
		decoded, err := url.PathUnescape(path)
		if err != nil {
			return "", false, errors.New("invalid SQLite database URL")
		}
		path = decoded
	}
	if path == "" || path == "/:memory:" || path == ":memory:" ||
		parsed.Query().Get("mode") == "memory" {
		return "", true, nil
	}
	return filepath.Clean(path), false, nil
}
