package ydbsecret

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/ydbpath"
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

// WithRotations adds to opts a rotation request for each secret at paths,
// read by [ParsePath], keeping one request per secret: a secret opts already
// asks to rotate, or that paths names twice, is asked for once. It is how a
// command or a library caller turns the secrets it names into the requests
// of one comparison; see [RotationRequests]. A path that names no secret is
// refused and opts is left as it was.
func WithRotations(opts *config.CompareOptions, paths []string) error {
	rotations, err := RotationRequests(paths)
	if err != nil {
		return err
	}
	requests := slices.Clone(opts.FeatureRequests)
	for _, rotation := range rotations {
		if !slices.ContainsFunc(requests, func(held schemaext.ChangeRequest) bool {
			return held.Action == rotation.Action && held.Subject.Key() == rotation.Subject.Key()
		}) {
			requests = append(requests, rotation)
		}
	}
	opts.FeatureRequests = requests
	return nil
}

// ErrAbsolutePath is what [ParsePath] wraps for a path that starts with a
// slash: such a path names the database it lies in, which nothing that spells
// a secret this way knows, so it is refused rather than read some other way.
var ErrAbsolutePath = ydbpath.ErrAbsolute

// ErrOutsideDatabase is what [ResolvePath] wraps for an absolute path outside
// the database it is read against.
var ErrOutsideDatabase = ydbpath.ErrOutsideDatabase

// ParsePath reads the path of a secret relative to the database root, as YDB
// writes it and as every Ptah spelling of a secret names it: a slash separates
// directories, the segment after the last slash is the name, and a dot is part
// of the segment that holds it. `ext/pg` is the secret pg in the directory ext,
// and `pg.pw` is the secret pg.pw at the root. Surrounding space is ignored. A
// path that starts with a slash is refused with [ErrAbsolutePath], and one
// with an empty, `.` or `..` segment, a trailing slash included, is refused.
// [ResolvePath] reads an absolute path where the database root is known.
func ParsePath(path string) (objectidentity.ID, error) {
	schema, name, err := ydbpath.Split(path)
	if err != nil {
		return objectidentity.ID{}, fmt.Errorf("%q is not a secret path (dir/name): %w: %w", path, schemaext.ErrInvalidValue, err)
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
	relative, err := ydbpath.Relative(root, path)
	if errors.Is(err, ydbpath.ErrOutsideDatabase) {
		return objectidentity.ID{}, fmt.Errorf("%q is outside the database /%s: %w: %w", path, strings.Trim(root, "/"), schemaext.ErrInvalidValue, err)
	}
	if err != nil {
		return ParsePath(path)
	}
	return ParsePath(relative)
}
