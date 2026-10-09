package ydbast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretKind identifies one statement on a YDB secret.
const SecretKind schemaext.Kind = "ptah.run/ydb/secret-operation" // #nosec G101 -- a kind identifier, not a credential

// SecretOperation selects the statement.
type SecretOperation string

const (
	// SecretCreate writes CREATE SECRET with the value ValueEnv holds.
	SecretCreate SecretOperation = "create"
	// SecretRotate writes ALTER SECRET with the value ValueEnv holds.
	SecretRotate SecretOperation = "rotate"
	// SecretDrop writes DROP SECRET.
	SecretDrop SecretOperation = "drop"
)

// Secret is one statement on the secret Name in the directory Schema, relative
// to the database root. It never carries the value: ValueEnv names the
// environment variable the YDB connection reads it from when the statement
// runs, and a drop names none. A zero value is invalid.
type Secret struct {
	Operation SecretOperation `json:"operation"`
	Schema    string          `json:"schema"`
	Name      string          `json:"name"`
	ValueEnv  string          `json:"value_env"`
}

// Kind returns the stable operation identity.
func (*Secret) Kind() schemaext.Kind { return SecretKind }

// CloneExtension returns an independent operation. A nil receiver returns a
// typed nil for the envelope to reject.
func (v *Secret) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*Secret)(nil)
	}
	return new(*v)
}

// Subject returns the secret's schema-scoped identity.
func (v *Secret) Subject() objectidentity.ID { return ydbsecret.Ref(v.Schema, v.Name) }

// Path names the secret as YDB writes its path, for messages.
func (v *Secret) Path() string { return ydbsecret.Display(v.Schema, v.Name) }

// Validate refuses an invalid path, an unknown operation, a creation or
// rotation without a variable a value may come from, and a drop that names
// one.
func (v *Secret) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: secret operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbsecret.ValidateIdentity(v.Subject()); err != nil {
		return err
	}
	switch v.Operation {
	case SecretCreate, SecretRotate:
		if err := ydbsecret.CheckValueEnv(v.ValueEnv); err != nil {
			return fmt.Errorf("%w: secret %s: %w", schemaext.ErrInvalidValue, v.Path(), err)
		}
		return nil
	case SecretDrop:
		if v.ValueEnv != "" {
			return fmt.Errorf("%w: dropping secret %s names no variable", schemaext.ErrInvalidValue, v.Path())
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown secret operation %q", schemaext.ErrInvalidValue, v.Operation)
	}
}

// Effect classifies the statement: a creation adds, a rotation changes what
// every data source naming the secret signs in with, and a drop loses a value
// nothing can read back. An invalid operation has unknown effects.
func (v *Secret) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch v.Operation {
	case SecretCreate:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: ydbsecret.CreateReason}
	case SecretRotate:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbsecret.RotateReason}
	default:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbsecret.DropReason}
	}
}
