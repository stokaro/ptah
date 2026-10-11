package pgextension

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
)

// Action names the statement an [Operation] renders.
type Action string

const (
	// Create renders CREATE EXTENSION, with IF NOT EXISTS, WITH SCHEMA and
	// VERSION where the operation names them.
	Create Action = "create"
	// UpdateVersion renders ALTER EXTENSION ... UPDATE TO the operation's
	// version.
	UpdateVersion Action = "update-version"
	// SetSchema renders ALTER EXTENSION ... SET SCHEMA the operation's
	// schema.
	SetSchema Action = "set-schema"
	// Drop renders DROP EXTENSION IF EXISTS.
	Drop Action = "drop"
)

// Operation carries one extension statement through the common AST extension
// envelope. Name is the extension's name. Schema is the installation schema
// a creation names, empty for the connection's default, and the schema a
// move goes to. Version is the version a creation names, empty for the
// control file's default, and the version an update goes to. IfNotExists
// applies to a creation only. An extension's comment is a
// [CommentOperation] of its own, because a target can hold extensions and
// not their comments.
type Operation struct {
	Name        string `json:"name"`
	Action      Action `json:"action"`
	Schema      string `json:"schema"`
	Version     string `json:"version"`
	IfNotExists bool   `json:"if_not_exists"`
}

// Kind returns the stable operation identity.
func (*Operation) Kind() schemaext.Kind { return OperationKind }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *Operation) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [Operation.CloneExtension] without the interface. A nil receiver
// returns nil.
func (v *Operation) Copy() *Operation {
	if v == nil {
		return nil
	}
	copied := *v
	return &copied
}

// Subject returns the extension's identity.
func (v *Operation) Subject() objectidentity.ID { return Ref(v.Name) }

// Validate refuses an invalid name or action, and a field the action does not
// take: a move needs a schema, an update a version, and only a creation names
// IF NOT EXISTS.
func (v *Operation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: extension operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ValidateRef(v.Subject()); err != nil {
		return err
	}
	if err := validTexts("schema", v.Schema, "version", v.Version); err != nil {
		return err
	}
	switch v.Action {
	case Create:
		return nil
	case UpdateVersion:
		if v.Version == "" || v.Schema != "" || v.IfNotExists {
			return fmt.Errorf("%w: an extension update names its version and nothing else", schemaext.ErrInvalidValue)
		}
	case SetSchema:
		if v.Schema == "" || v.Version != "" || v.IfNotExists {
			return fmt.Errorf("%w: an extension move names its schema and nothing else", schemaext.ErrInvalidValue)
		}
	case Drop:
		if v.Schema != "" || v.Version != "" || v.IfNotExists {
			return fmt.Errorf("%w: an extension drop names its extension and nothing else", schemaext.ErrInvalidValue)
		}
	default:
		return fmt.Errorf("%w: unknown extension action %q", schemaext.ErrInvalidValue, v.Action)
	}
	return nil
}

// Effect is the statement's effect on the extension and the objects it owns.
func (v *Operation) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch v.Action {
	case Create:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "installs an extension"}
	case Drop:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP EXTENSION removes database objects owned by the extension"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes the version or the schema of an extension and of the objects it owns"}
	}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *Operation) SchemaChange() ast.ExtensionChange {
	switch v.Action {
	case Create:
		return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.Name}
	case Drop:
		return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.Name}
	default:
		return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Name}
	}
}

// CommentOperation sets one extension's comment: COMMENT ON EXTENSION, with
// NULL for an empty Comment. A target that does not store an extension's
// comment skips it, as it skips its own capability-gated statements.
type CommentOperation struct {
	Name    string `json:"name"`
	Comment string `json:"comment"`
}

// Kind returns the stable operation identity.
func (*CommentOperation) Kind() schemaext.Kind { return CommentOperationKind }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *CommentOperation) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [CommentOperation.CloneExtension] without the interface. A nil
// receiver returns nil.
func (v *CommentOperation) Copy() *CommentOperation {
	if v == nil {
		return nil
	}
	copied := *v
	return &copied
}

// Subject returns the extension's identity.
func (v *CommentOperation) Subject() objectidentity.ID { return Ref(v.Name) }

// Validate refuses an invalid name or comment.
func (v *CommentOperation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: extension comment operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ValidateRef(v.Subject()); err != nil {
		return err
	}
	return validTexts("comment", v.Comment)
}

// RequiredCapability names the capability a target needs for the statement.
func (*CommentOperation) RequiredCapability() capability.Capability {
	return capability.ExtensionComments
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *CommentOperation) OmissionSubject() (kind, name string) {
	return "extension comment", v.Name
}

// Effect is the statement's effect: a comment changes no object's behavior.
func (v *CommentOperation) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of an extension"}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *CommentOperation) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Name}
}

// OperationCodecs returns the version-one codecs of both operation payloads.
func OperationCodecs() []schemaext.Codec {
	return []schemaext.Codec{operationCodec(), commentOperationCodec()}
}

// The operation wires. Every key is required.
var (
	operationShape = schemaext.ObjectShape{Name: "extension operation",
		Allowed: []string{"name", "action", "schema", "version", "if_not_exists"}, Required: []string{"name", "action", "schema", "version", "if_not_exists"}}
	commentOperationShape = schemaext.ObjectShape{Name: "extension comment operation",
		Allowed: []string{"name", "comment"}, Required: []string{"name", "comment"}}
)

func operationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*Operation]{
		Prototype: &Operation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["name","action","schema","version","if_not_exists"],"additionalProperties":false,` +
			`"properties":{"name":{"type":"string","minLength":1},"action":{"enum":["create","update-version","set-schema","drop"]},` +
			`"schema":{"type":"string"},"version":{"type":"string"},"if_not_exists":{"type":"boolean"}}}`),
		Shape:    shape(operationShape),
		Validate: (*Operation).Validate,
		Clone:    (*Operation).Copy,
	}.Codec()
}

func commentOperationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*CommentOperation]{
		Prototype: &CommentOperation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["name","comment"],"additionalProperties":false,` +
			`"properties":{"name":{"type":"string","minLength":1},"comment":{"type":"string"}}}`),
		Shape:    shape(commentOperationShape),
		Validate: (*CommentOperation).Validate,
		Clone:    (*CommentOperation).Copy,
	}.Codec()
}
