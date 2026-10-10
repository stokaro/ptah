package chast

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

// RowPolicyKind identifies one row policy statement.
const RowPolicyKind schemaext.Kind = "ptah.run/clickhouse/row-policy-operation"

// RowPolicy creates, changes or drops one row policy. A nil Change.Before
// requests CREATE ROW POLICY, a nil Change.After DROP ROW POLICY, and both
// ALTER ROW POLICY, which sets the filter, the composition and the users in
// place. Database is empty when the source left the table in the connection's
// database. Use it as an ast.ExtensionStatement: a row policy is an access
// entity of its own, not part of its table, and ClickHouse keeps it when the
// table is dropped.
type RowPolicy struct {
	Database string           `json:"database"`
	Table    string           `json:"table"`
	Name     string           `json:"name"`
	Change   chdiff.RowPolicy `json:"change"`
}

// Kind returns the stable operation identity.
func (*RowPolicy) Kind() schemaext.Kind { return RowPolicyKind }

// CloneExtension returns an independent operation and both operands. A nil
// receiver remains typed nil.
func (v *RowPolicy) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*RowPolicy)(nil)
	}
	return &RowPolicy{Database: v.Database, Table: v.Table, Name: v.Name, Change: *v.Change.Copy()}
}

// Subject returns the policy's identity under ClickHouse's identifier rules.
func (v *RowPolicy) Subject() objectidentity.ID {
	return chschema.RowPolicyRef(v.Database, v.Table, v.Name)
}

// Validate refuses an invalid identity or change.
func (v *RowPolicy) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: ClickHouse row policy operation is nil", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateRowPolicyRef(v.Subject()); err != nil {
		return err
	}
	return chdiff.ValidateRowPolicy(&v.Change)
}

// Effect is the change's lifecycle effect.
func (v *RowPolicy) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// AccessEffect is the change's access assessment, which the operation carries
// unchanged so a saved plan reports what the comparison established.
func (v *RowPolicy) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Change.AccessEffect()
}

// QualifiedName is the policy as its server names it: `name ON db.table`,
// with the database left out when the source left it out.
func (v *RowPolicy) QualifiedName() string {
	table := v.Table
	if strings.TrimSpace(v.Database) != "" {
		table = v.Database + "." + v.Table
	}
	return v.Name + " ON " + table
}

// SchemaChange reports the logical action for schema-change reports.
func (v *RowPolicy) SchemaChange() ast.ExtensionChange {
	switch {
	case v.Change.Before == nil:
		return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.QualifiedName()}
	case v.Change.After == nil:
		return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.QualifiedName()}
	default:
		return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.QualifiedName()}
	}
}

// rowPolicyFields are the operation's wire properties, all required.
var rowPolicyFields = []string{"database", "table", "name", "change"}

func rowPolicyCodec() schemaext.Codec {
	change := chdiff.RowPolicyCodecs()[0]
	value := func(payload schemaext.Payload) (*RowPolicy, error) {
		v, ok := payload.(*RowPolicy)
		if !ok {
			return nil, fmt.Errorf("%w: expected ClickHouse row policy operation, got %T", schemaext.ErrInvalidValue, payload)
		}
		return v, v.Validate()
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := value(payload)
		if err != nil {
			return nil, err
		}
		encoded, err := change.Canonical(&v.Change)
		if err != nil {
			return nil, err
		}
		return json.Marshal(struct {
			Database string          `json:"database"`
			Table    string          `json:"table"`
			Name     string          `json:"name"`
			Change   json.RawMessage `json:"change"`
		}{v.Database, v.Table, v.Name, encoded})
	}
	return schemaext.Codec{
		Prototype: &RowPolicy{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"database":"the table's database, or empty for the connection's",`+
			`"table":"the table the policy filters","name":"the policy's own name","change":%s}`, change.Definition)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := value(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneExtension(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != len(rowPolicyFields) {
				return nil, fmt.Errorf("%w: ClickHouse row policy operation requires exactly database, table, name and change", schemaext.ErrInvalidValue)
			}
			names := make([]string, 0, 3)
			for _, key := range rowPolicyFields[:3] {
				raw, found := fields[key]
				if !found || string(raw) == "null" {
					return nil, fmt.Errorf("%w: ClickHouse row policy operation requires %s as a string", schemaext.ErrInvalidValue, key)
				}
				name, err := schemaext.DecodeJSON[string](raw)
				if err != nil {
					return nil, err
				}
				names = append(names, name)
			}
			raw, found := fields["change"]
			if !found {
				return nil, fmt.Errorf("%w: ClickHouse row policy operation requires change", schemaext.ErrInvalidValue)
			}
			decoded, err := change.Decode(raw)
			if err != nil {
				return nil, err
			}
			typed, ok := decoded.(*chdiff.RowPolicy)
			if !ok {
				return nil, fmt.Errorf("%w: unexpected ClickHouse row policy change %T", schemaext.ErrInvalidValue, decoded)
			}
			v, err := value(&RowPolicy{Database: names[0], Table: names[1], Name: names[2], Change: *typed})
			if err != nil {
				return nil, err
			}
			return v, nil
		},
	}
}
