package difftypes

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// ErrRotateUndeclaredSecret is the error [SchemaDiff.RotateSecrets] wraps when
// a request names a secret the declaration does not hold.
var ErrRotateUndeclaredSecret = errors.New("the desired schema declares no such secret")

// RotateSecrets asks the plan to give each named YDB secret the value its
// declared environment variable holds when the plan runs, as ALTER SECRET.
//
// A comparison cannot find a rotation itself: the server never returns a
// secret's value, so a secret both sides hold is equal by its presence alone,
// and the request is how an operator says the value changed. Each name is the
// secret's path relative to the database root, as YDB writes it: `dir/name`,
// or `name` for a secret at the root, whose name may hold a dot. A name the
// declaration does not hold is refused, wrapping
// [ErrRotateUndeclaredSecret], so a typo cannot read as a rotation done. A
// declared secret the plan creates is not rotated as well, since CREATE
// SECRET already takes the value. Asking twice for one secret rotates it once.
func (d *SchemaDiff) RotateSecrets(names []string) error {
	for _, requested := range names {
		name := secretReference(requested)
		index := slices.IndexFunc(d.DeclaredSecrets, func(secret schemamodel.Secret) bool {
			return secret.QualifiedName() == name
		})
		if index < 0 {
			return fmt.Errorf("rotate secret %q: %w", requested, ErrRotateUndeclaredSecret)
		}
		matches := func(secret schemamodel.Secret) bool { return secret.QualifiedName() == name }
		if slices.ContainsFunc(d.SecretsAdded, matches) || slices.ContainsFunc(d.SecretsRotated, matches) {
			continue
		}
		d.SecretsRotated = append(d.SecretsRotated, d.DeclaredSecrets[index])
	}
	slices.SortFunc(d.SecretsRotated, func(a, b schemamodel.Secret) int {
		return strings.Compare(a.QualifiedName(), b.QualifiedName())
	})
	return nil
}

// secretReference reads a requested secret path as the secret's canonical
// reference: the segments before the last slash are its directory, and the
// last is its name, which a slash never splits.
func secretReference(requested string) string {
	path := strings.Trim(strings.TrimSpace(requested), "/")
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return tableref.Canonical("", path)
	}
	return tableref.Canonical(path[:index], path[index+1:])
}
