package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineargs"
)

// This file converts the schema objects beyond tables, indexes and enums that
// the SQL frontend parses. Before issue #932 every one of these nodes fell off
// the end of ToDatabase's switch, so a CREATE VIEW in a --schema-file parsed
// cleanly and then vanished from the rendered schema.

func toSequence(node *ast.CreateSequenceNode, sourcePlatform string) schemamodel.Sequence {
	return schemamodel.Sequence{
		Name:        normalizeSQLIdentifier(sourcePlatform, node.Name),
		Schema:      normalizeSQLIdentifier(sourcePlatform, node.Schema),
		AsType:      normalizeSQLIdentifier(sourcePlatform, node.AsType),
		Start:       node.Start,
		Increment:   node.Increment,
		MinValue:    node.MinValue,
		MaxValue:    node.MaxValue,
		Cache:       node.Cache,
		Cycle:       node.Cycle,
		OwnedBy:     normalizeSQLTableReference(sourcePlatform, node.OwnedBy),
		IfNotExists: node.IfNotExists,
		Comment:     node.Comment,
	}
}

// appendCreateRole checks the full ordered document before Finalize can merge
// duplicate names. PostgreSQL refuses a repeated CREATE ROLE regardless of
// whether an earlier declaration was top-level or inside a selected DO branch.
func appendCreateRole(database *schemamodel.Database, document *Document, node *ast.CreateRoleNode, dialect string) error {
	role := toRole(node, dialect)
	if factsOf(dialect).statements.CreateRoleOnce || dialect == "" {
		sources := []*schemamodel.Database{database, document.base}
		for _, source := range sources {
			if source == nil {
				continue
			}
			for _, declared := range source.Roles {
				if declared.Name == role.Name {
					return fmt.Errorf("%w: CREATE ROLE %q repeats a role already declared in this desired schema", ErrUnmodeledStatement, role.Name)
				}
			}
		}
	}
	database.Roles = append(database.Roles, role)
	return nil
}

func toRole(node *ast.CreateRoleNode, sourcePlatform string) schemamodel.Role {
	return schemamodel.Role{
		Name:        roleName(sourcePlatform, node.Name),
		Group:       node.Group,
		Login:       node.Login,
		Password:    node.Password,
		Superuser:   node.Superuser,
		CreateDB:    node.CreateDB,
		CreateRole:  node.CreateRole,
		Inherit:     node.Inherit,
		Replication: node.Replication,
		Comment:     node.Comment,
	}
}

func appendView(database *schemamodel.Database, document *Document, statement ast.Node, sourcePlatform string) bool {
	switch node := statement.(type) {
	case *ast.CreateViewNode:
		appendCreateView(database, document, node, sourcePlatform)
	case *ast.CreateMaterializedViewNode:
		database.MaterializedViews = append(database.MaterializedViews, toMaterializedView(node, sourcePlatform))
	default:
		return false
	}
	return true
}

func toView(node *ast.CreateViewNode, sourcePlatform string) schemamodel.View {
	return schemamodel.View{
		Name:      normalizeSQLTableReference(sourcePlatform, node.Name),
		Body:      strings.TrimSpace(node.Body),
		WithCheck: node.WithCheck,
		Comment:   node.Comment,
	}
}

func toMaterializedView(node *ast.CreateMaterializedViewNode, sourcePlatform string) schemamodel.MaterializedView {
	view := schemamodel.MaterializedView{
		Facets:  node.Facets,
		Name:    normalizeSQLTableReference(sourcePlatform, node.Name),
		Body:    strings.TrimSpace(node.Body),
		Comment: node.Comment,
	}
	return view
}

func toFunction(node *ast.CreateFunctionNode, sourcePlatform string) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(sourcePlatform, node.Name),
		Parameters: node.Parameters,
		Returns:    node.Returns,
		Language:   node.Language,
		Security:   node.Security,
		Volatility: node.Volatility,
		Settings:   node.Settings,
		Facets:     node.Facets,
		Body:       strings.TrimSpace(node.Body),
		Comment:    node.Comment,
	}
	function.Canonicalize()
	return function
}

func toTrigger(node *ast.CreateTriggerNode, sourcePlatform string) schemamodel.Trigger {
	trigger := schemamodel.Trigger{
		Name:    normalizeSQLIdentifier(sourcePlatform, node.Name),
		Table:   normalizeSQLTableReference(sourcePlatform, node.Table),
		Timing:  node.Timing,
		Event:   node.Event,
		ForEach: node.ForEach,
		Body:    strings.TrimSpace(node.Body),
		Comment: node.Comment,
		When:    strings.TrimSpace(node.When),
		// A transition table is a relation name, folded the way the server
		// folds it, because the catalog reports it folded.
		OldTable: normalizeSQLIdentifier(sourcePlatform, node.OldTable),
		NewTable: normalizeSQLIdentifier(sourcePlatform, node.NewTable),
	}
	trigger.Canonicalize()
	if node.ExternalFunction && node.FunctionName != "" {
		trigger.ExecuteFunction = normalizeSQLTableReference(sourcePlatform, node.FunctionName)
	}
	return trigger
}

// appendCreateType routes a CREATE TYPE node to the bucket its type definition
// belongs in. An enum is the one definition the node carries; a type owner
// reads a domain or a composite type.
func appendCreateType(database *schemamodel.Database, node *ast.CreateTypeNode, sourcePlatform string) {
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
	if definition, ok := node.TypeDef.(*ast.EnumTypeDef); ok {
		database.Enums = append(database.Enums, schemamodel.Enum{
			Name:   schemamodel.QualifyTableName(schema, name),
			Values: definition.Values,
		})
	}
}

func unquoteSQLStringLiteral(value string) string {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return value
	}
	return strings.ReplaceAll(value[1:len(value)-1], "''", "'")
}

// adoptTriggerFunctions folds each Ptah-owned trigger function back into the
// trigger that owns it. Rendering a trigger for PostgreSQL emits a
// CREATE FUNCTION plus a CREATE TRIGGER pair, so reading that SQL back has to
// recombine the two or the next render would emit the function twice.
func adoptTriggerFunctions(database *schemamodel.Database) {
	if len(database.Triggers) == 0 || len(database.Functions) == 0 {
		return
	}
	// A trigger function takes no arguments, and only that overload of the
	// name is the trigger's: another overload is a function of its own.
	bodies := make(map[string]string, len(database.Functions))
	for _, function := range database.Functions {
		if takesNoArguments(function) {
			bodies[function.Name] = function.Body
		}
	}

	owned := make(map[string]bool, len(database.Triggers))
	for index := range database.Triggers {
		trigger := &database.Triggers[index]
		if trigger.ExecuteFunction == "" || trigger.ExecuteFunction != trigger.FunctionName() {
			continue
		}
		body, exists := bodies[trigger.ExecuteFunction]
		if !exists {
			continue
		}
		trigger.Body = trimTriggerFunctionWrapper(body)
		trigger.ExecuteFunction = ""
		owned[trigger.FunctionName()] = true
	}
	if len(owned) == 0 {
		return
	}

	functions := make([]schemamodel.Function, 0, len(database.Functions))
	for _, function := range database.Functions {
		if !owned[function.Name] || !takesNoArguments(function) {
			functions = append(functions, function)
		}
	}
	database.Functions = functions
}

// takesNoArguments reports whether a routine takes no input arguments, which
// is the overload a trigger executes.
func takesNoArguments(function schemamodel.Function) bool {
	return routineargs.InputTypes(function.Parameters) == ""
}

// trimTriggerFunctionWrapper removes the BEGIN / END; envelope the PostgreSQL
// renderer wraps a trigger body in, so the recovered body renders identically.
func trimTriggerFunctionWrapper(body string) string {
	trimmed := strings.TrimSpace(body)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN") {
		return trimmed
	}
	inner := strings.TrimSpace(trimmed[len("BEGIN"):])
	if !strings.HasSuffix(strings.ToUpper(inner), "END;") {
		return trimmed
	}
	return strings.TrimSpace(inner[:len(inner)-len("END;")])
}
