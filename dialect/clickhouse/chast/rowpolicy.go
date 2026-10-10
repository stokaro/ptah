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
func (v *RowPolicy) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [RowPolicy.CloneExtension] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *RowPolicy) Copy() *RowPolicy {
	if v == nil {
		return nil
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

// rowPolicyShape is the operation object's keys, each required. The database
// may be empty, for the connection's.
var rowPolicyShape = schemaext.ObjectShape{Name: "ClickHouse row policy operation",
	Allowed: []string{"database", "table", "name", "change"}, Required: []string{"database", "table", "name", "change"}}

// rowPolicyCodec is the operation codec. The change takes its own codec's
// wire form, checked by that codec. Every refusal is a
// [schemaext.InvalidModelError].
func rowPolicyCodec() schemaext.Codec {
	change := chdiff.RowPolicyCodecs()[0]
	return schemaext.ModelCodec[*RowPolicy]{
		Prototype: &RowPolicy{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"database":"the table's database, or empty for the connection's",`+
			`"table":"the table the policy filters","name":"the policy's own name","change":%s}`, change.Definition)),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, rowPolicyShape)
			if err != nil {
				return err
			}
			_, err = change.Decode(fields["change"])
			return err
		},
		Validate: (*RowPolicy).Validate,
		Clone:    (*RowPolicy).Copy,
	}.Codec()
}
