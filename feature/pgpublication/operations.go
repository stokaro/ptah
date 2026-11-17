package pgpublication

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Action names the statement an [Operation] renders.
type Action string

const (
	// Create renders CREATE PUBLICATION with the operation's membership and
	// the options it names.
	Create Action = "create"
	// Drop renders DROP PUBLICATION, with IF EXISTS where the operation names
	// it.
	Drop Action = "drop"
	// AddMembers renders ALTER PUBLICATION ... ADD with the operation's
	// tables and schemas.
	AddMembers Action = "add-members"
	// DropMembers renders ALTER PUBLICATION ... DROP with the operation's
	// tables and schemas.
	DropMembers Action = "drop-members"
	// SetMembers renders ALTER PUBLICATION ... SET with the operation's tables
	// and schemas, which replace the publication's members.
	SetMembers Action = "set-members"
	// SetOptions renders ALTER PUBLICATION ... SET with every option the
	// operation carries.
	SetOptions Action = "set-options"
	// SetOwner renders ALTER PUBLICATION ... OWNER TO the operation's owner.
	SetOwner Action = "set-owner"
)

// Operation carries one publication statement through the common AST
// extension envelope. Name is the publication's name.
//
// A creation names its membership: AllTables, or the Tables and Schemas it
// publishes, none of them for an empty publication. Each table is written with
// ONLY, so the publication holds the tables it names and no inheritance child
// joins unnamed; a partitioned table publishes its partitions through itself.
// A membership change names the tables and schemas it adds, drops or sets.
// A table the planner names is qualified by its schema; one a migration
// statement names may not be.
//
// Publish, ViaPartitionRoot and GeneratedColumns are the options. A creation
// writes Publish where it is not nil, publish_via_partition_root where
// ViaPartitionRoot is set and publish_generated_columns where GeneratedColumns
// is not empty. SetOptions writes Publish and publish_via_partition_root
// always, and publish_generated_columns where GeneratedColumns is not empty,
// since a server without the setting refuses its name.
//
// Owner is the role SetOwner gives the publication. IfExists applies to a drop
// only.
type Operation struct {
	Name             string   `json:"name"`
	Action           Action   `json:"action"`
	AllTables        bool     `json:"all_tables"`
	Tables           []Member `json:"tables"`
	Schemas          []string `json:"schemas"`
	Publish          []string `json:"publish"`
	ViaPartitionRoot bool     `json:"via_partition_root"`
	GeneratedColumns string   `json:"generated_columns"`
	Owner            string   `json:"owner"`
	IfExists         bool     `json:"if_exists"`
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
	copied.Tables = copyMembers(v.Tables)
	copied.Schemas = slices.Clone(v.Schemas)
	copied.Publish = slices.Clone(v.Publish)
	return &copied
}

// Subject returns the publication's identity.
func (v *Operation) Subject() objectidentity.ID { return Ref(v.Name) }

// Validate refuses an invalid name or action, and a field the action does not
// take: only a creation names options with its membership, a membership change
// names a table or a schema and nothing else, SetOptions names options alone,
// SetOwner an owner alone, and only a drop names IF EXISTS.
func (v *Operation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: publication operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ValidateRef(v.Subject()); err != nil {
		return err
	}
	if err := validateShape(publicationShape{v.AllTables, v.Tables, v.Schemas, v.Publish, v.GeneratedColumns, v.Owner}); err != nil {
		return err
	}
	for _, member := range v.Tables {
		if member.Filtered() {
			return fmt.Errorf("%w: a publication operation names each table by its name alone", schemaext.ErrInvalidValue)
		}
	}
	if v.Action != Drop && v.IfExists {
		return fmt.Errorf("%w: only a publication drop names IF EXISTS", schemaext.ErrInvalidValue)
	}
	if v.Action != SetOwner && v.Owner != "" {
		return fmt.Errorf("%w: only a publication owner change names an owner", schemaext.ErrInvalidValue)
	}
	return v.validateAction()
}

// validateAction refuses a field the action does not take, and an unknown
// action.
func (v *Operation) validateAction() error {
	members, options := v.namesMembers(), v.namesOptions()
	switch v.Action {
	case Create:
		return nil
	case Drop:
		if members || options {
			return fmt.Errorf("%w: a publication drop names its publication and nothing else", schemaext.ErrInvalidValue)
		}
	case AddMembers, DropMembers, SetMembers:
		if options || v.AllTables || len(v.Tables) == 0 && len(v.Schemas) == 0 {
			return fmt.Errorf("%w: a publication membership change names its tables or schemas and nothing else", schemaext.ErrInvalidValue)
		}
	case SetOptions:
		if members || v.Publish == nil {
			return fmt.Errorf("%w: a publication option change names its publish set and its settings and nothing else", schemaext.ErrInvalidValue)
		}
	case SetOwner:
		if members || options || v.Owner == "" {
			return fmt.Errorf("%w: a publication owner change names its owner and nothing else", schemaext.ErrInvalidValue)
		}
	default:
		return fmt.Errorf("%w: unknown publication action %q", schemaext.ErrInvalidValue, v.Action)
	}
	return nil
}

// namesMembers reports whether the operation names a membership.
func (v *Operation) namesMembers() bool {
	return v.AllTables || len(v.Tables) != 0 || len(v.Schemas) != 0
}

// namesOptions reports whether the operation names an option.
func (v *Operation) namesOptions() bool {
	return v.Publish != nil || v.ViaPartitionRoot || v.GeneratedColumns != ""
}

// Effect is the statement's effect on the publication and its subscribers.
func (v *Operation) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch v.Action {
	case Create:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a publication; no consumer reads it until a subscription does"}
	case Drop:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP PUBLICATION stops logical replication to every subscriber that reads the publication"}
	case AddMembers:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "publishes more tables to the publication's subscribers"}
	case DropMembers:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "stops publishing tables to the publication's subscribers"}
	case SetMembers:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "replaces the tables the publication publishes to its subscribers"}
	case SetOptions:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes which changes the publication publishes"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "gives the publication to another role, which the publication's subscribers then depend on"}
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

// operationShape is the operation wire. Every key is required.
var operationShape = schemaext.ObjectShape{Name: "publication operation",
	Allowed: []string{"name", "action", "all_tables", "tables", "schemas", "publish", "via_partition_root", "generated_columns", "owner",
		"if_exists"},
	Required: []string{"name", "action", "all_tables", "tables", "schemas", "publish", "via_partition_root", "generated_columns", "owner",
		"if_exists"},
	Nullable: []string{"tables", "schemas", "publish"}}

// OperationCodecs returns the version-one codec of the operation payload.
func OperationCodecs() []schemaext.Codec {
	return []schemaext.Codec{schemaext.ModelCodec[*Operation]{
		Prototype: &Operation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["name","action","all_tables","tables","schemas","publish",` +
			`"via_partition_root","generated_columns","owner","if_exists"],"additionalProperties":false,"properties":{` +
			`"name":{"type":"string","minLength":1},` +
			`"action":{"enum":["create","drop","add-members","drop-members","set-members","set-options","set-owner"]},"all_tables":{"type":"boolean"},` +
			`"tables":{"anyOf":[{"type":"null"},{"type":"array","items":` + operationMemberDefinition + `}]},` +
			`"schemas":{"anyOf":[{"type":"null"},{"type":"array","items":{"type":"string","minLength":1}}]},` +
			`"publish":{"anyOf":[{"type":"null"},` + publishDefinition + `]},"via_partition_root":{"type":"boolean"},` +
			`"generated_columns":{"enum":["","none","stored"]},"owner":{"type":"string"},"if_exists":{"type":"boolean"}}}`),
		Shape:    operationShapeCheck,
		Validate: (*Operation).Validate,
		Clone:    (*Operation).Copy,
	}.Codec()}
}

// operationShapeCheck checks the operation's keys, then each table's.
func operationShapeCheck(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, operationShape)
	if err != nil {
		return err
	}
	if string(fields["tables"]) == "null" {
		return nil
	}
	var members []json.RawMessage
	if err := json.Unmarshal(fields["tables"], &members); err != nil {
		return fmt.Errorf("%w: publication operation tables: %w", schemaext.ErrInvalidValue, err)
	}
	for _, member := range members {
		if _, err := schemaext.DecodeObject(member, operationMemberShape); err != nil {
			return err
		}
	}
	return nil
}

// operationMemberDefinition is a table an operation names: by its name, and
// by its schema where the operation names one.
const operationMemberDefinition = `{"type":"object","required":["table"],"additionalProperties":false,"properties":{` +
	`"schema":{"type":"string","minLength":1},"table":{"type":"string","minLength":1}}}`

var operationMemberShape = schemaext.ObjectShape{Name: "publication operation member",
	Allowed: []string{"schema", "table"}, Required: []string{"table"}, NonEmpty: []string{"schema"}}
