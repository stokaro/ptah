package pgtable

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// DeleteColumnsOperationKind identifies a replacement of a foreign key that
// changes the columns its ON DELETE action sets.
const DeleteColumnsOperationKind schemaext.Kind = "ptah.run/postgres/replace-foreign-key-delete-columns"

// ReplaceDeleteColumns captures the complete foreign key to restore. Names and
// columns are logical identifiers; ForeignTable is a possibly qualified SQL
// reference. Its containing operation supplies the owning table. The operation
// requires a transaction, including the comment restoration.
type ReplaceDeleteColumns struct {
	Name           string   `json:"name"`
	Columns        []string `json:"columns"`
	ForeignTable   string   `json:"foreign_table"`
	ForeignColumns []string `json:"foreign_columns"`
	OnDelete       string   `json:"on_delete,omitempty"`
	OnUpdate       string   `json:"on_update,omitempty"`
	DeleteColumns  []string `json:"delete_columns,omitempty"`
	Match          string   `json:"match,omitempty"`
	Deferrable     bool     `json:"deferrable,omitempty"`
	Initially      string   `json:"initially,omitempty"`
	NotEnforced    bool     `json:"not_enforced,omitempty"`
	NotValid       bool     `json:"not_valid,omitempty"`
	Comment        string   `json:"comment,omitempty"`
}

// Kind returns the operation's stable identity.
func (*ReplaceDeleteColumns) Kind() schemaext.Kind { return DeleteColumnsOperationKind }

// CloneExtension returns independent column lists. A nil receiver stays nil.
func (op *ReplaceDeleteColumns) CloneExtension() ast.ExtensionPayload {
	return cloneDeleteColumnsOperation(op)
}
func cloneDeleteColumnsOperation(op *ReplaceDeleteColumns) *ReplaceDeleteColumns {
	if op == nil {
		return nil
	}
	cloned := *op
	cloned.Columns = slices.Clone(op.Columns)
	cloned.ForeignColumns = slices.Clone(op.ForeignColumns)
	cloned.DeleteColumns = slices.Clone(op.DeleteColumns)
	return &cloned
}

// Effect describes the foreign-key action change.
func (*ReplaceDeleteColumns) Effect() schemaext.Effect { return (&DeleteColumnsChange{}).Effect() }

// SchemaChange identifies the modified constraint for migration reports.
func (op *ReplaceDeleteColumns) SchemaChange() ast.ExtensionChange {
	if op == nil {
		return ast.ExtensionChange{}
	}
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: op.Name}
}

// ValidateReplaceDeleteColumns refuses incomplete key definitions and invalid
// SQL action keywords. Errors wrap schemaext.ErrInvalidValue. Target capability
// checks belong to the renderer and planner that select a server profile.
func ValidateReplaceDeleteColumns(op *ReplaceDeleteColumns) error {
	if op == nil || op.Name == "" || len(op.Columns) == 0 || len(op.Columns) != len(op.ForeignColumns) {
		return fmt.Errorf("%w: ON DELETE replacement requires a complete named foreign key", schemaext.ErrInvalidValue)
	}
	for _, value := range []string{op.Name, op.ForeignTable, op.Comment} {
		if err := schemaext.ValidText("foreign key replacement", value); err != nil {
			return err
		}
	}
	if _, ok := tableref.Parse(op.ForeignTable); !ok {
		return fmt.Errorf("%w: invalid referenced table %q", schemaext.ErrInvalidValue, op.ForeignTable)
	}
	for _, columns := range [][]string{op.Columns, op.ForeignColumns, op.DeleteColumns} {
		if err := validateDeleteColumns(columns); err != nil {
			return err
		}
	}
	if err := validateDeleteReplacementActions(op); err != nil {
		return err
	}
	if len(op.DeleteColumns) > 0 {
		if op.OnDelete != "SET NULL" && op.OnDelete != "SET DEFAULT" {
			return fmt.Errorf("%w: ON DELETE column list requires SET NULL or SET DEFAULT", schemaext.ErrInvalidValue)
		}
		for _, column := range op.DeleteColumns {
			if !slices.Contains(op.Columns, column) {
				return fmt.Errorf("%w: ON DELETE names column %q outside the key", schemaext.ErrInvalidValue, column)
			}
		}
	}
	if !slices.Contains([]string{"", "SIMPLE", "FULL", "PARTIAL"}, op.Match) {
		return fmt.Errorf("%w: invalid foreign key MATCH %q", schemaext.ErrInvalidValue, op.Match)
	}
	if !slices.Contains([]string{"", "immediate", "deferred"}, strings.ToLower(op.Initially)) || op.Initially != "" && !op.Deferrable {
		return fmt.Errorf("%w: foreign key timing requires DEFERRABLE and immediate or deferred", schemaext.ErrInvalidValue)
	}
	return nil
}

const deleteColumnsOperationDefinition = `{
  "type": "object",
  "required": [
    "name",
    "columns",
    "foreign_table",
    "foreign_columns"
  ],
  "additionalProperties": false,
  "properties": {
    "name": {
      "type": "string"
    },
    "foreign_table": {
      "type": "string"
    },
    "on_delete": {
      "type": "string"
    },
    "on_update": {
      "type": "string"
    },
    "match": {
      "type": "string"
    },
    "initially": {
      "type": "string"
    },
    "comment": {
      "type": "string"
    },
    "columns": {
      "type": "array",
      "items": {
        "type": "string",
        "minLength": 1
      }
    },
    "foreign_columns": {
      "type": "array",
      "items": {
        "type": "string",
        "minLength": 1
      }
    },
    "delete_columns": {
      "type": "array",
      "items": {
        "type": "string",
        "minLength": 1
      }
    },
    "deferrable": {
      "type": "boolean"
    },
    "not_enforced": {
      "type": "boolean"
    },
    "not_valid": {
      "type": "boolean"
    }
  }
}`

// DeleteColumnsOperationCodec is a strict version-one codec. It carries data
// across a process boundary without depending on a Go AST representation.
func DeleteColumnsOperationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*ReplaceDeleteColumns]{Prototype: &ReplaceDeleteColumns{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(deleteColumnsOperationDefinition), Shape: deleteColumnsOperationShape,
		Validate: ValidateReplaceDeleteColumns, Clone: cloneDeleteColumnsOperation}.Codec()
}
func deleteColumnsOperationShape(data json.RawMessage) error {
	_, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "foreign key delete-column replacement",
		Required: []string{"name", "columns", "foreign_table", "foreign_columns"},
		Allowed:  []string{"name", "columns", "foreign_table", "foreign_columns", "on_delete", "on_update", "delete_columns", "match", "deferrable", "initially", "not_enforced", "not_valid", "comment"}})
	return err
}

func validateDeleteReplacementActions(op *ReplaceDeleteColumns) error {
	for _, action := range []string{op.OnDelete, op.OnUpdate} {
		if !slices.Contains([]string{"", "NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}, action) {
			return fmt.Errorf("%w: invalid foreign key action %q", schemaext.ErrInvalidValue, action)
		}
	}
	return nil
}
