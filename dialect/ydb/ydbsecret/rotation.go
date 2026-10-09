package ydbsecret

import (
	"errors"
	"fmt"
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

// ParsePath reads the path of a secret relative to the database root, as YDB
// writes it and as every Ptah spelling of a secret names it: a slash separates
// directories, the segment after the last slash is the name, and a dot is part
// of the segment that holds it. `ext/pg` is the secret pg in the directory ext,
// and `pg.pw` is the secret pg.pw at the root. Leading and trailing slashes and
// surrounding space are ignored. A path with an empty, `.` or `..` segment is
// refused.
func ParsePath(path string) (objectidentity.ID, error) {
	ref := Ref(SplitPath(path))
	if err := ValidateIdentity(ref); err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a secret path (dir/name): %w", path, err)
	}
	return ref, nil
}

// SplitPath reads a secret path relative to the database root as its
// directory and its name, without validating either: the segments before the
// last slash are the directory, and the last is the name. Leading and trailing
// slashes and surrounding space are ignored. [ParsePath] also validates.
func SplitPath(path string) (schema, name string) {
	path = strings.Trim(strings.TrimSpace(path), "/")
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return "", path
	}
	return path[:index], path[index+1:]
}
