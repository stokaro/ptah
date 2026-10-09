package ydbsecret

import (
	"errors"
	"fmt"
	pathpkg "path"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// RotateAction is the [schemaext.ChangeRequest] action that gives a secret the
// database holds the value its declared variable holds when the plan runs,
// through ALTER SECRET.
const RotateAction = "rotate"

// ErrRotateUndeclared is the error a comparison wraps when a rotation request
// names a secret the desired schema does not declare.
var ErrRotateUndeclared = errors.New("the desired schema declares no such secret")

// RotationRequests returns the comparison requests that give each secret at
// paths a new value. A secret's value is never read back, so a comparison
// cannot see a changed one; a rotation request is the only way one is planned.
// It belongs to one comparison, and no source format can write it.
//
// Each path is read by [ParsePath]. Asking twice for one secret rotates it
// once. The comparison refuses a path the desired schema does not declare with
// [ErrRotateUndeclared], so a typo cannot read as a rotation done. A declared
// secret the database does not hold is created instead, since CREATE SECRET
// already takes the value, and one whose presence is not established is
// reported rather than rotated.
func RotationRequests(paths []string) ([]schemaext.ChangeRequest, error) {
	var requests []schemaext.ChangeRequest
	seen := make(map[objectidentity.Key]bool, len(paths))
	for _, requested := range paths {
		ref, err := ParsePath(requested)
		if err != nil {
			return nil, fmt.Errorf("rotate secret: %w", err)
		}
		if seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		requests = append(requests, schemaext.ChangeRequest{Subject: ref, Action: RotateAction})
	}
	return requests, nil
}

// ErrAbsolutePath is what [ParsePath] wraps for a path that starts with a
// slash: such a path names the database it lies in, which nothing that spells
// a secret this way knows, so it is refused rather than read some other way.
var ErrAbsolutePath = errors.New("the path starts with a slash; write the secret's path relative to the database root, " +
	"without the database's own path")

// ErrOutsideDatabase is what [ResolvePath] wraps for an absolute path outside
// the database it is read against.
var ErrOutsideDatabase = errors.New("the path lies outside the database")

// ParsePath reads the path of a secret relative to the database root, as YDB
// writes it and as every Ptah spelling of a secret names it: a slash separates
// directories, the segment after the last slash is the name, and a dot is part
// of the segment that holds it. `ext/pg` is the secret pg in the directory ext,
// and `pg.pw` is the secret pg.pw at the root. Surrounding space is ignored. A
// path that starts with a slash is refused with [ErrAbsolutePath], and one
// with an empty, `.` or `..` segment, a trailing slash included, is refused.
// [ResolvePath] reads an absolute path where the database root is known.
func ParsePath(path string) (objectidentity.ID, error) {
	trimmed := strings.TrimSpace(path)
	if strings.HasPrefix(trimmed, "/") {
		return objectidentity.ID{}, fmt.Errorf("%q is not a secret path (dir/name): %w: %w", path, schemaext.ErrInvalidValue, ErrAbsolutePath)
	}
	schema, name := "", trimmed
	if index := strings.LastIndex(trimmed, "/"); index >= 0 {
		schema, name = trimmed[:index], trimmed[index+1:]
	}
	ref := Ref(schema, name)
	if err := ValidateIdentity(ref); err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a secret path (dir/name): %w", path, err)
	}
	return ref, nil
}

// ResolvePath reads path as [ParsePath] does, except that a path starting
// with a slash is read against root, the absolute path of the database, such
// as /local: `/local/ext/pg` is the secret ext/pg there. An absolute path
// outside root is refused with [ErrOutsideDatabase]; with an empty root, every
// absolute path is refused with [ErrAbsolutePath].
func ResolvePath(root, path string) (objectidentity.ID, error) {
	trimmed := strings.TrimSpace(path)
	if !strings.HasPrefix(trimmed, "/") || strings.Trim(root, "/") == "" {
		return ParsePath(path)
	}
	database := "/" + strings.Trim(root, "/")
	relative, under := strings.CutPrefix(pathpkg.Clean(trimmed), database+"/")
	if !under {
		return objectidentity.ID{}, fmt.Errorf("%q is outside the database %s: %w: %w", path, database, schemaext.ErrInvalidValue, ErrOutsideDatabase)
	}
	return ParsePath(relative)
}
