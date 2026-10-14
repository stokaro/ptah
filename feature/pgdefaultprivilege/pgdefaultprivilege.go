// Package pgdefaultprivilege owns PostgreSQL default-privilege declarations and
// observations. A global observation describes changes to the built-in default,
// including revoked privileges; an omitted global row does not mean no rights.
// Catalog access, SQL rendering, and provider selection live outside this model.
package pgdefaultprivilege

import (
	"fmt"

	"ptah.run/core/schemaext"
)

// Owner identifies the provider that owns default-privilege models and services.
const Owner = "ptah.run/pgdefaultprivilege"

// DefaultPrivilegeKind identifies the defaults for one grantor, schema, object
// class, and grantee. An empty schema denotes the global default.
const DefaultPrivilegeKind schemaext.Kind = "ptah.run/pgdefaultprivilege/default-privilege"

func invalid(representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: DefaultPrivilegeKind, Representation: representation, Message: err.Error()}
}

func validText(name, value string) error {
	return schemaext.ValidText("default privilege "+name, value)
}

func requiredText(name, value string) error {
	if err := validText(name, value); err != nil {
		return err
	}
	if value == "" {
		return fmt.Errorf("%w: default privilege requires %s", schemaext.ErrInvalidValue, name)
	}
	return nil
}
