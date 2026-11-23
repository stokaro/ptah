// Package sqlite decides whether two SQLite URLs address one database: the
// database is a file, so two URLs address one database when they name one
// file, or both name one shared-cache database in memory.
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
// otherwise by the absolute path with every symlink of it resolved.
//
// A database in memory is no file, and is the same as another one only when
// both are one shared-cache database. SQLite gives the connections of a
// process one database for one name opened with `cache=shared`, as in
// `sqlite:file:memdb1?mode=memory&cache=shared` or
// `sqlite://file::memory:?cache=shared`, and names are compared as SQLite
// compares them, exactly. Every other database in memory is private to the
// connection that opened it, so two of them are two databases, even under one
// URL.
func (databases) SameDatabase(left, right *url.URL) (bool, error) {
	leftLocation, err := locate(left)
	if err != nil {
		return false, err
	}
	rightLocation, err := locate(right)
	if err != nil {
		return false, err
	}
	if leftLocation.memory || rightLocation.memory {
		return leftLocation.memory && rightLocation.memory &&
			leftLocation.shared != "" && leftLocation.shared == rightLocation.shared, nil
	}
	leftInfo, leftErr := os.Stat(leftLocation.path)    // #nosec G703 -- the database file the caller's URL names
	rightInfo, rightErr := os.Stat(rightLocation.path) // #nosec G703 -- the database file the caller's URL names
	if leftErr == nil && rightErr == nil {
		return os.SameFile(leftInfo, rightInfo), nil
	}
	leftIdentity, err := fileIdentity(leftLocation.path)
	if err != nil {
		return false, err
	}
	rightIdentity, err := fileIdentity(rightLocation.path)
	if err != nil {
		return false, err
	}
	return leftIdentity == rightIdentity, nil
}

// fileIdentity is the absolute path of a database file with every symlink of
// it resolved.
func fileIdentity(path string) (string, error) {
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
	return filepath.Clean(absolute), nil
}

// location is what a SQLite URL names: a database file by its path, or a
// database in memory.
type location struct {
	// path is the database file. It is empty for a database in memory.
	path string
	// memory reports a database in memory.
	memory bool
	// shared is the name the connections of a process share a database in
	// memory by, and is empty for a private one.
	shared string
}

// locate reads what parsed names the way the connector opens it
// (internal/connector/sqliteconnect). The connector hands the driver `:memory:`
// for a URL naming no file or `:memory:`, and a `file:` URI for a URL that
// spells one or says `mode=memory`. SQLite reads `cache=shared` only in a URI,
// decodes a percent escape in the URI's name, and shares a database in memory
// among the connections that open its name with `cache=shared`; an empty name
// is a private temporary database.
func locate(parsed *url.URL) (location, error) {
	path := parsed.Opaque
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
	name, fileURI := strings.CutPrefix(path, "file:")
	if fileURI {
		decoded, err := unescapeURIName(name)
		if err != nil {
			return location{}, err
		}
		name = decoded
	}
	query := parsed.Query()
	unnamed := name == "" || name == "/:memory:" || name == ":memory:"
	memoryMode := query.Get("mode") == "memory"
	if !unnamed && !memoryMode {
		return location{path: filepath.Clean(name)}, nil
	}
	uri := fileURI
	if memoryMode && !unnamed && !fileURI {
		// The connector spells this name as a URI.
		decoded, err := unescapeURIName(name)
		if err != nil {
			return location{}, err
		}
		name, uri = decoded, true
	}
	if uri && name != "" && query.Get("cache") == "shared" {
		return location{memory: true, shared: name}, nil
	}
	return location{memory: true}, nil
}

// unescapeURIName decodes the percent escapes in the name of a `file:` URI, as
// SQLite does.
func unescapeURIName(name string) (string, error) {
	decoded, err := url.PathUnescape(name)
	if err != nil {
		return "", errors.New("invalid SQLite database URL")
	}
	return decoded, nil
}
