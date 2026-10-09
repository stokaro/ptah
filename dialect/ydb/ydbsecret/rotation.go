package ydbsecret

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// ErrRotateUndeclared is the error [RequestRotation] wraps when a request
// names a secret the declaration does not hold.
var ErrRotateUndeclared = errors.New("the desired schema declares no such secret")

// RequestRotation returns a copy of desired in which each named secret asks
// for a new value: the plan gives it the value its declared variable holds
// when the plan runs, through ALTER SECRET. A secret's value is never read
// back, so a comparison cannot see a changed one; this is the only way a
// rotation is planned. The rotation is part of one comparison's input and no
// source format can write it.
//
// A path names the secret relative to the database root, as YDB writes it:
// `dir/name`, or `name` for a secret at the root, whose name may hold a dot.
// A path the declaration does not hold is refused with [ErrRotateUndeclared],
// so a typo cannot read as a rotation done. Asking twice for one secret
// rotates it once. A secret the database does not hold is created instead,
// since CREATE SECRET already takes the value, and one whose presence is not
// established is reported rather than rotated.
//
// desired is not modified. A nil desired or an empty request returns desired.
func RequestRotation(desired *schemamodel.Database, paths []string) (*schemamodel.Database, error) {
	if desired == nil || len(paths) == 0 {
		return desired, nil
	}
	objects, err := rotate(desired.FeatureObjects, paths)
	if err != nil {
		return nil, err
	}
	rotated := *desired
	rotated.FeatureObjects = objects
	return &rotated, nil
}

func rotate(objects schemaext.Objects, paths []string) (schemaext.Objects, error) {
	for _, requested := range paths {
		schema, name := SplitPath(requested)
		object, found, err := objects.Get(Ref(schema, name))
		if err != nil {
			return schemaext.Objects{}, err
		}
		declared, ok := object.Value.(*Desired)
		if !found || !ok {
			return schemaext.Objects{}, fmt.Errorf("rotate secret %q: %w", requested, ErrRotateUndeclared)
		}
		rotated := *declared
		rotated.Rotate = true
		objects, err = objects.Replace(schemaext.Object{Ref: object.Ref, Value: &rotated})
		if err != nil {
			return schemaext.Objects{}, err
		}
	}
	return objects, nil
}

// SplitPath reads a secret path relative to the database root as its
// directory and its name: the segments before the last slash are the
// directory, and the last is the name, which a slash never splits. Leading
// and trailing slashes and surrounding space are ignored.
func SplitPath(path string) (schema, name string) {
	path = strings.Trim(strings.TrimSpace(path), "/")
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return "", path
	}
	return path[:index], path[index+1:]
}
