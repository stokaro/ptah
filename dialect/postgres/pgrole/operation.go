package pgrole

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/sqlident"
)

// OperationKind identifies the ALTER ROLE statements that change one role's
// settings.
const OperationKind schemaext.Kind = "ptah.run/postgres/alter-role-settings"

// AlterSettings changes the settings of one role: `ALTER ROLE role SET name =
// value` for each setting the change gives a value, and `ALTER ROLE role RESET
// name` for each it resets, in the change's name order. Role is the role's
// name without quotes.
type AlterSettings struct {
	Role   string         `json:"role"`
	Change SettingsChange `json:"change"`
}

// Kind returns the owned operation identity.
func (*AlterSettings) Kind() schemaext.Kind { return OperationKind }

// CloneExtension returns independent operands. A nil receiver remains typed nil.
func (op *AlterSettings) CloneExtension() ast.ExtensionPayload { return op.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (op *AlterSettings) Copy() *AlterSettings {
	if op == nil {
		return nil
	}
	return &AlterSettings{Role: op.Role, Change: *op.Change.Copy()}
}

// Validate refuses nil, a role without a name or with text a statement cannot
// carry, and a change [ValidateSettingsChange] refuses. Every error wraps
// schemaext.ErrInvalidValue.
func (op *AlterSettings) Validate() error {
	if op == nil {
		return fmt.Errorf("%w: nil role settings operation", schemaext.ErrInvalidValue)
	}
	if err := schemaext.ValidText("role name", op.Role); err != nil {
		return err
	}
	if op.Role == "" {
		return fmt.Errorf("%w: a role settings operation requires a role", schemaext.ErrInvalidValue)
	}
	return ValidateSettingsChange(&op.Change)
}

// Statements renders the operation as PostgreSQL statements, each ending in
// a semicolon. Names are quoted, so a name keeps its case and is never read as
// a keyword; values are written in SQL as [ParseValue] accepted them.
func (op *AlterSettings) Statements() ([]string, error) {
	if err := op.Validate(); err != nil {
		return nil, err
	}
	role := sqlident.Quote(platform.Postgres, op.Role)
	statements := make([]string, 0, len(op.Change.Settings))
	for _, setting := range op.Change.Settings {
		if setting.After == nil {
			statements = append(statements, fmt.Sprintf("ALTER ROLE %s RESET %s;", role, QuoteName(setting.Name)))
			continue
		}
		statements = append(statements, fmt.Sprintf("ALTER ROLE %s SET %s = %s;", role, QuoteName(setting.Name), *setting.After))
	}
	return statements, nil
}

// RequiredCapability names the capability that gates the statement, the one
// CREATE ROLE needs.
func (*AlterSettings) RequiredCapability() capability.Capability { return capability.RoleManagement }

// OmissionSubject names the role whose settings a target without role
// management leaves out.
func (op *AlterSettings) OmissionSubject() (kind, name string) { return "role settings", op.Role }

// SchemaChange reports an alteration of the role.
func (op *AlterSettings) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: op.Role}
}

// Subject is the identity of the role whose settings change.
func (op *AlterSettings) Subject() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres)).Role(op.Role)
}

// Effect describes the change of the role's settings.
func (op *AlterSettings) Effect() schemaext.Effect {
	if op.Validate() != nil {
		return schemaext.Effect{}
	}
	return op.Change.Effect()
}

// OperationCodec returns a strict version-one operation codec. Its change
// member uses the operand contract of [SettingsChangeCodec].
func OperationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*AlterSettings]{Prototype: &AlterSettings{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["role","change"],"additionalProperties":false,"properties":{` +
			`"role":{"type":"string","minLength":1},"change":` + settingsChangeDefinition + `}}`),
		Shape: operationShape, Validate: (*AlterSettings).Validate, Clone: (*AlterSettings).Copy}.Codec()
}

func operationShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "role settings operation", Allowed: []string{"role", "change"}, Required: []string{"role", "change"}})
	if err != nil {
		return err
	}
	_, err = SettingsChangeCodec().Decode(fields["change"])
	return err
}

// CreationOperation is the operation that gives a role the settings facets
// declare when CREATE ROLE creates it: SET for each declared value. A reset
// asks for nothing a new role holds, so it is left out, and facets that hold
// no value return nil.
func CreationOperation(role string, facets schemaext.Facets) (*AlterSettings, error) {
	declared, found, err := schemaext.FacetAs[*DesiredSettings](facets, SettingsKind)
	if err != nil || !found {
		return nil, err
	}
	if err := ValidateDesiredSettings(declared); err != nil {
		return nil, err
	}
	change := Changes(declared.Settings, nil)
	if change == nil {
		return nil, nil
	}
	op := &AlterSettings{Role: role, Change: *change}
	if err := op.Validate(); err != nil {
		return nil, err
	}
	return op, nil
}
