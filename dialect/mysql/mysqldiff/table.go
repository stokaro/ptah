package mysqldiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TableKind identifies a change to a surviving table's options.
//
// No comparison produces one: the options apply when a table is created, and
// the engine and the next auto-increment value are not read, so a table that
// exists keeps the options it holds, as it always has. The kind is the
// change vocabulary the owner's comparison declares, and no planner accepts it.
const TableKind schemaext.Kind = "ptah.run/mysql/table-change"

// Table retains the options a table holds and the options a declaration
// states for it. Both are required.
type Table struct {
	Before *mysqlschema.ObservedTable `json:"before"`
	After  *mysqlschema.DesiredTable  `json:"after"`
}

// Kind returns the owned change identity.
func (*Table) Kind() schemaext.Kind { return TableKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Table) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [Table.CloneChange] without the interface. A nil receiver returns
// nil.
func (v *Table) Copy() *Table {
	if v == nil {
		return nil
	}
	result := &Table{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateTable requires both operands, each valid in its representation.
// Invalid operands wrap schemaext.ErrInvalidValue.
func ValidateTable(v *Table) error {
	if v == nil || v.Before == nil || v.After == nil {
		return fmt.Errorf("%w: a MySQL table options change requires both operands", schemaext.ErrInvalidValue)
	}
	if err := mysqlschema.ValidateObservedTable(v.Before); err != nil {
		return err
	}
	return mysqlschema.ValidateDesiredTable(v.After)
}

var tableShape = schemaext.ObjectShape{Name: "MySQL table options change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"}}

// TableCodec returns the table options change codec. Each operand takes the
// exact desired or observed wire form, checked by that model's codec. Every
// refusal is a [schemaext.InvalidModelError].
func TableCodec() schemaext.Codec {
	return schemaext.ModelCodec[*Table]{
		Prototype: &Table{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"the observed options","after":"the desired options"}`,
			mysqlschema.TableWireDefinition())),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, tableShape)
			if err != nil {
				return err
			}
			for _, codec := range mysqlschema.TableCodecs() {
				key := "after"
				if codec.Representation == schemaext.Observed {
					key = "before"
				}
				if _, err := codec.Decode(fields[key]); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: ValidateTable,
		Clone:    (*Table).Copy,
	}.Codec()
}
