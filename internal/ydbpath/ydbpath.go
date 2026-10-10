// Package ydbpath reads the path of a YDB scheme object as its directory and
// its name. Every owner of a standalone YDB object -- a secret, a topic --
// reads the spellings of its object's path through it, so they agree on what
// a slash means and on which spellings are refused.
package ydbpath

import (
	"errors"
	pathpkg "path"
	"strings"
)

// ErrAbsolute is what [Split] returns for a path that starts with a slash:
// such a path names the database it lies in, which nothing that spells an
// object this way knows, so it is refused rather than read some other way.
var ErrAbsolute = errors.New("the path starts with a slash; write the path relative to the database root, " +
	"without the database's own path")

// ErrOutsideDatabase is what [Relative] returns for an absolute path outside
// the database it is read against.
var ErrOutsideDatabase = errors.New("the path lies outside the database")

// Split reads path, relative to the database root, as YDB writes it: the
// segments before the last slash are the directory, and the last one is the
// name, in which a dot is part of the name. `ext/pg` is pg in the directory
// ext, and `pg.pw` is pg.pw at the root. Surrounding space is ignored. A path
// that starts with a slash is refused with [ErrAbsolute]. Split validates no
// segment: a trailing slash leaves the name empty, and the owner's identity
// check refuses that, an empty segment and a `.` or `..` one.
func Split(path string) (directory, name string, err error) {
	trimmed := strings.TrimSpace(path)
	if strings.HasPrefix(trimmed, "/") {
		return "", "", ErrAbsolute
	}
	if index := strings.LastIndex(trimmed, "/"); index >= 0 {
		return trimmed[:index], trimmed[index+1:], nil
	}
	return "", trimmed, nil
}

// Relative returns path relative to root, the absolute path of a database
// such as /local. A path that does not start with a slash is returned as it
// is, and an absolute one under root without root: `/local/ext/pg` is
// `ext/pg`. An absolute path outside root is refused with
// [ErrOutsideDatabase], and with an empty root every absolute path is refused
// with [ErrAbsolute].
func Relative(root, path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if !strings.HasPrefix(trimmed, "/") {
		return path, nil
	}
	if strings.Trim(root, "/") == "" {
		return "", ErrAbsolute
	}
	relative, under := strings.CutPrefix(pathpkg.Clean(trimmed), "/"+strings.Trim(root, "/")+"/")
	if !under {
		return "", ErrOutsideDatabase
	}
	return relative, nil
}
