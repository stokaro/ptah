package providerhost

import (
	"fmt"
	"reflect"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
	"ptah.run/internal/tableref"
)

// onlyFields makes the experimental projection fail closed when the host model
// grows. Metadata named here remains in the host; every other populated field
// needs a wire representation before this adapter can accept it.
func onlyFields(value any, allowed ...string) error {
	reflected := reflect.ValueOf(value)
	for i := range reflected.NumField() {
		field := reflected.Field(i)
		if slices.Contains(allowed, reflected.Type().Field(i).Name) || emptyField(field) {
			continue
		}
		return unsupported(fmt.Sprintf("%s.%s", reflected.Type().Name(), reflected.Type().Field(i).Name))
	}
	return nil
}

// Empty collections inside metadata records carry no semantic value. Pointers
// and interfaces remain present even when they point at an empty value.
func emptyField(field reflect.Value) bool {
	if field.IsZero() {
		return true
	}
	if field.CanInterface() {
		if zero, ok := reflect.TypeAssert[interface{ IsZero() bool }](field); ok && zero.IsZero() {
			return true
		}
	}
	switch field.Kind() {
	case reflect.Slice, reflect.Map:
		return field.Len() == 0
	case reflect.Struct:
		for _, field0 := range field.Fields() {
			if !emptyField(field0) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func commonColumn(field schemamodel.Field) (*ast.ColumnNode, error) {
	if err := onlyFields(field, "StructName", "FieldName", "Name", "Type", "Nullable", "Primary", "TypeRawSQL", "TypeIsDeclaredText"); err != nil {
		return nil, err
	}
	if field.Name == "" || field.Type == "" {
		return nil, fmt.Errorf("provider column requires a name and type")
	}
	return &ast.ColumnNode{Name: field.Name, Type: field.Type, Nullable: field.Nullable, Primary: field.Primary}, nil
}

func commonTable(declaration schemacapture.TableDeclaration) (*ast.CreateTableNode, error) {
	if err := onlyFields(declaration, "Table", "Fields", "FeatureCoverage"); err != nil {
		return nil, err
	}
	table := declaration.Table
	if err := onlyFields(table, "StructName", "Name", "Schema", "PrimaryKey", "DependsOn"); err != nil {
		return nil, err
	}
	if table.Name == "" {
		return nil, fmt.Errorf("provider table requires a name")
	}
	if len(table.PrimaryKey) > 1 {
		return nil, unsupported("composite primary keys")
	}
	node := &ast.CreateTableNode{Name: tableref.CanonicalExact(table.Schema, table.Name)}
	primary := 0
	for _, field := range declaration.Fields {
		column, err := commonColumn(field)
		if err != nil {
			return nil, err
		}
		column.Primary = column.Primary || slices.Contains(table.PrimaryKey, column.Name)
		if column.Primary {
			primary++
		}
		node.Columns = append(node.Columns, column)
	}
	if primary > 1 {
		return nil, unsupported("composite primary keys")
	}
	if len(node.Columns) == 0 {
		return nil, fmt.Errorf("provider table requires columns")
	}
	if len(table.PrimaryKey) != 0 && primary == 0 {
		return nil, fmt.Errorf("provider primary key names no declared column")
	}
	return node, nil
}

// commonSchema creates schema from nothing: the common objects beyond tables
// before and after the tables, as commonObjectCreations places them, and the
// tables in dependency order, each followed by the steps that set its
// comments where the engine carries them on the common-object wire.
func (t *target) commonSchema(schema *schemamodel.Database) ([]ast.Node, error) {
	defaultSchema := t.engine.GetDefaultSchema()
	if schema == nil {
		return nil, fmt.Errorf("provider schema is required")
	}
	// Declared rows are the host's data stage, which the provider serves
	// through its row services; they render no schema object. The common
	// objects beyond tables travel on the common-object wire, whose capture
	// has already refused a kind the engine does not carry.
	if err := onlyFields(*schema, "Tables", "Fields", "Schemas", "Dependencies", "FeatureCoverage", "NotDescribed", "ManagedData",
		"Views", "MaterializedViews", "Sequences", "Roles", "Grants", "RevokedGrants"); err != nil {
		return nil, err
	}
	for _, namespace := range schema.Schemas {
		if namespace.Name != defaultSchema {
			return nil, unsupported("schema creation")
		}
		if err := onlyFields(namespace, "Name"); err != nil {
			return nil, err
		}
	}
	creations := make(difftypes.TableChanges, 0, len(schema.Tables))
	assigned := 0
	owners := make(map[string]bool, len(schema.Tables))
	for _, table := range schema.Tables {
		if owners[table.StructName] {
			return nil, fmt.Errorf("provider schema has ambiguous column ownership")
		}
		owners[table.StructName] = true
		fields := make([]schemamodel.Field, 0)
		for _, field := range schema.Fields {
			if field.StructName == table.StructName {
				fields = append(fields, field)
				assigned++
			}
		}
		creations = append(creations, difftypes.TableCreation{Name: table.QualifiedName(), Table: table, Fields: fields, DependsOn: schema.Dependencies[table.QualifiedName()]})
	}
	if assigned != len(schema.Fields) {
		return nil, fmt.Errorf("provider schema has columns without exactly one table")
	}
	ordered := creations.InDependencyOrder()
	if len(ordered) != len(creations) {
		return nil, fmt.Errorf("provider schema contains duplicate tables")
	}
	before, after := commonObjectCreations(schema)
	nodes := make([]ast.Node, 0, len(before)+len(ordered)+len(after))
	nodes = append(nodes, before...)
	for _, creation := range ordered {
		declaration := schemacapture.TableDeclaration{Table: creation.Table, Fields: creation.Fields}
		var comments []ast.Node
		if t.commonKinds()[protocol.CommonObjectSupport_COMMENT] {
			declaration, comments = creationComments(declaration)
		}
		node, err := commonTable(declaration)
		if err != nil {
			return nil, err
		}
		nodes = append(append(nodes, node), comments...)
	}
	return append(nodes, after...), nil
}

// commonObjectCreations creates the common objects beyond tables that schema
// declares, where a plan from an empty database places them: the roles and
// the sequences before the tables, which may name them, and the views, the
// memberships, the grants and the revocations after them. A comment travels
// with the object it belongs to.
func commonObjectCreations(schema *schemamodel.Database) (before, after []ast.Node) {
	for _, role := range schema.Roles {
		before = append(before, modelast.FromRole(role))
	}
	for _, sequence := range schema.Sequences {
		before = append(before, modelast.FromSequence(sequence))
	}
	after = createdViews(&difftypes.SchemaDiff{ViewsAdded: schema.Views})
	for _, view := range schema.MaterializedViews {
		after = append(after, modelast.FromMaterializedView(view))
	}
	for _, role := range schema.Roles {
		for _, membership := range modelast.FromRoleMemberships(role) {
			after = append(after, membership)
		}
	}
	for _, grant := range schema.Grants {
		after = append(after, modelast.FromGrant(grant))
	}
	for _, grant := range schema.RevokedGrants {
		after = append(after, modelast.FromRevokedGrant(grant))
	}
	return before, after
}
