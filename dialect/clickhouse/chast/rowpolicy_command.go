package chast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// RowPolicyCommandKind identifies authored row-policy SQL. Unlike a planned
// transition, an authored ALTER has no observation of the policy it changes.
const RowPolicyCommandKind schemaext.Kind = "ptah.run/clickhouse/row-policy-command"

// RowPolicyCommand holds CREATE, ALTER, or DROP ROW POLICY syntax. Values uses
// the owner's declaration model. FilterSet distinguishes USING NONE from an
// omitted USING, and RolesSet distinguishes TO NONE from an omitted TO. An
// empty composition leaves AS unspecified. DROP carries no values or clauses.
// This command records no invented before state for standalone migration SQL.
type RowPolicyCommand struct {
	Action    string                     `json:"action"`
	Database  string                     `json:"database"`
	Table     string                     `json:"table"`
	Name      string                     `json:"name"`
	Values    *chschema.DesiredRowPolicy `json:"values"`
	FilterSet bool                       `json:"filter_set"`
	RolesSet  bool                       `json:"roles_set"`
}

// Kind returns the command identity.
func (*RowPolicyCommand) Kind() schemaext.Kind { return RowPolicyCommandKind }

// Copy returns independent clause values. A nil receiver remains nil.
func (v *RowPolicyCommand) Copy() *RowPolicyCommand {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Values = v.Values.Copy()
	return &cloned
}

// CloneExtension captures an independent command.
func (v *RowPolicyCommand) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Subject identifies the policy on its table.
func (v *RowPolicyCommand) Subject() objectidentity.ID {
	return chschema.RowPolicyRef(v.Database, v.Table, v.Name)
}

// Validate checks the action and clauses without assuming a policy exists.
func (v *RowPolicyCommand) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil row policy command", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateRowPolicyRef(v.Subject()); err != nil {
		return err
	}
	switch v.Action {
	case "DROP":
		if v.Values != nil || v.FilterSet || v.RolesSet {
			return fmt.Errorf("%w: DROP ROW POLICY takes no clauses", schemaext.ErrInvalidValue)
		}
		return nil
	case "CREATE", "ALTER":
	default:
		return fmt.Errorf("%w: unsupported row policy command %q", schemaext.ErrInvalidValue, v.Action)
	}
	if err := chschema.ValidateDesiredRowPolicy(v.Values); err != nil {
		return err
	}
	if v.Values.StructName != "" || v.Values.NormalizedFilter != nil {
		return fmt.Errorf("%w: row policy SQL carries authored clauses only", schemaext.ErrInvalidValue)
	}
	if !v.FilterSet && v.Values.Filter != nil || !v.RolesSet && !v.Values.Roles.IsZero() {
		return fmt.Errorf("%w: row policy value has no corresponding clause", schemaext.ErrInvalidValue)
	}
	if v.Action == "ALTER" && !v.FilterSet && !v.RolesSet && v.Values.Composition == "" {
		return fmt.Errorf("%w: ALTER ROW POLICY requires USING, AS, or TO", schemaext.ErrInvalidValue)
	}
	return nil
}

// SchemaChange describes the authored action without claiming a known state.
func (v *RowPolicyCommand) SchemaChange() ast.ExtensionChange {
	action := ast.ExtensionModify
	switch v.Action {
	case "CREATE":
		action = ast.ExtensionAdd
	case "DROP":
		action = ast.ExtensionDrop
	}
	return ast.ExtensionChange{Action: action, Name: v.Subject().String()}
}

// Effect reports the lifecycle impact; dropping a policy removes a protection.
func (v *RowPolicyCommand) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	if v.Action == "DROP" {
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "dropping a row policy removes a protection; it deletes no data"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "a row policy command changes which rows users may read"}
}

// AccessEffect withholds an access verdict without the policy and its siblings.
func (v *RowPolicyCommand) AccessEffect() schemaext.AccessEffect {
	return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "the access effect depends on the current policies and server settings"}
}

func rowPolicyCommandCodec() schemaext.Codec {
	return schemaext.ModelCodec[*RowPolicyCommand]{
		Prototype: &RowPolicyCommand{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"action":"CREATE, ALTER, or DROP","database":"optional database","table":"one table","name":"policy name","values":"authored policy values or null for DROP",` +
			`"filter_set":"USING is present","roles_set":"TO is present"}`),
		Shape: rowPolicyCommandShape, Validate: (*RowPolicyCommand).Validate, Clone: (*RowPolicyCommand).Copy,
	}.Codec()
}

func rowPolicyCommandShape(data json.RawMessage) error {
	keys := []string{"action", "database", "table", "name", "values", "filter_set", "roles_set"}
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "ClickHouse row policy command", Allowed: keys, Required: keys, Nullable: []string{"values"}})
	if err != nil || string(fields["values"]) == "null" {
		return err
	}
	for _, codec := range chschema.RowPolicyCodecs() {
		if codec.Representation == schemaext.Desired {
			_, err = codec.Decode(fields["values"])
			return err
		}
	}
	return fmt.Errorf("%w: missing row policy declaration codec", schemaext.ErrInvalidValue)
}
