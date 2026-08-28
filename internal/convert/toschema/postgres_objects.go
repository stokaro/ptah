package toschema

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/schemamodel"
)

// This file converts the schema objects beyond tables, indexes and enums that
// the SQL frontend parses. Before issue #932 every one of these nodes fell off
// the end of ToDatabase's switch, so a CREATE VIEW in a --schema-file parsed
// cleanly and then vanished from the rendered schema.

func toSequence(node *ast.CreateSequenceNode) schemamodel.Sequence {
	return schemamodel.Sequence{
		Name:        normalizeSQLIdentifier(node.Name),
		Schema:      normalizeSQLIdentifier(node.Schema),
		AsType:      normalizeSQLIdentifier(node.AsType),
		Start:       node.Start,
		Increment:   node.Increment,
		MinValue:    node.MinValue,
		MaxValue:    node.MaxValue,
		Cache:       node.Cache,
		Cycle:       node.Cycle,
		OwnedBy:     normalizeSQLTableReference(node.OwnedBy),
		IfNotExists: node.IfNotExists,
		Comment:     node.Comment,
	}
}

func toRole(node *ast.CreateRoleNode) schemamodel.Role {
	return schemamodel.Role{
		Name:        normalizeSQLIdentifier(node.Name),
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

func toGrant(node *ast.GrantPrivilegeNode) schemamodel.Grant {
	grant := schemamodel.Grant{
		Role:       normalizeSQLIdentifier(node.Role),
		Privileges: node.Privileges,
		WithOption: node.WithOption,
		Comment:    node.Comment,
	}
	target := normalizeSQLTableReference(node.ObjectName)
	switch strings.ToUpper(node.ObjectType) {
	case "SCHEMA":
		grant.OnSchema = normalizeSQLIdentifier(node.ObjectName)
	case "SEQUENCE":
		grant.OnSequence = target
	default:
		grant.OnTable = target
	}
	grant.Canonicalize()
	return grant
}

// toRLSPolicy resolves the policy's table through
// [catalogPostgresTableReference] rather than the plain unquoting every other
// object uses.
//
// A policy is an access-control declaration, and the relation it lands on is
// the whole question. `CREATE POLICY p ON "ORDERS"` and `CREATE POLICY p ON
// ORDERS` name different relations, so keeping the raw spelling left the rest
// of the pipeline to guess -- and the guess secured a relation the author did
// not name (stokaro/ptah#1311).
func toRLSPolicy(node *ast.CreatePolicyNode) schemamodel.RLSPolicy {
	return schemamodel.RLSPolicy{
		Name:                normalizeSQLIdentifier(node.Name),
		Table:               catalogPostgresTableReference(node.Table),
		PolicyFor:           node.PolicyFor,
		ToRoles:             normalizeRoleList(node.ToRoles),
		UsingExpression:     node.UsingExpression,
		WithCheckExpression: node.WithCheckExpression,
		Comment:             node.Comment,
	}
}

// normalizeRoleList unquotes each role in a policy's TO list while keeping the
// separator the renderer expects.
func normalizeRoleList(roles string) string {
	if strings.TrimSpace(roles) == "" {
		return ""
	}
	parts := strings.Split(roles, ",")
	for index, part := range parts {
		parts[index] = normalizeSQLIdentifier(strings.TrimSpace(part))
	}
	return strings.Join(parts, ", ")
}

// toRLSEnabledTable resolves its table the same way [toRLSPolicy] does. The
// enablement and the policies on it have to name one relation, or the render
// turns row-level security on for one table and protects another.
func toRLSEnabledTable(node *ast.AlterTableEnableRLSNode) schemamodel.RLSEnabledTable {
	return schemamodel.RLSEnabledTable{
		Table:   catalogPostgresTableReference(node.Table),
		Comment: node.Comment,
	}
}

func toView(node *ast.CreateViewNode) schemamodel.View {
	return schemamodel.View{
		Name:      normalizeSQLTableReference(node.Name),
		Body:      strings.TrimSpace(node.Body),
		WithCheck: node.WithCheck,
		Comment:   node.Comment,
	}
}

func toMaterializedView(node *ast.CreateMaterializedViewNode) schemamodel.MaterializedView {
	view := schemamodel.MaterializedView{
		Name:    normalizeSQLTableReference(node.Name),
		Body:    strings.TrimSpace(node.Body),
		Comment: node.Comment,
	}
	return view
}

func toFunction(node *ast.CreateFunctionNode) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(node.Name),
		Parameters: node.Parameters,
		Returns:    node.Returns,
		Language:   node.Language,
		Security:   node.Security,
		Volatility: node.Volatility,
		Settings:   node.Settings,
		Body:       strings.TrimSpace(node.Body),
		Comment:    node.Comment,
	}
	function.Canonicalize()
	return function
}

func toTrigger(node *ast.CreateTriggerNode) schemamodel.Trigger {
	trigger := schemamodel.Trigger{
		Name:    normalizeSQLIdentifier(node.Name),
		Table:   normalizeSQLTableReference(node.Table),
		Timing:  node.Timing,
		Event:   node.Event,
		ForEach: node.ForEach,
		Body:    strings.TrimSpace(node.Body),
		Comment: node.Comment,
	}
	trigger.Canonicalize()
	if node.ExternalFunction && node.FunctionName != "" {
		trigger.ExecuteFunction = normalizeSQLTableReference(node.FunctionName)
	}
	return trigger
}

// appendCreateType routes a CREATE TYPE node to the bucket its type definition
// belongs in. CREATE DOMAIN also arrives here, as a DomainTypeDef.
func appendCreateType(database *schemamodel.Database, node *ast.CreateTypeNode) {
	schema, name := normalizeSQLTableIdentifier(node.Name)
	switch definition := node.TypeDef.(type) {
	case *ast.EnumTypeDef:
		database.Enums = append(database.Enums, schemamodel.Enum{
			Name:   schemamodel.QualifyTableName(schema, name),
			Values: definition.Values,
		})
	case *ast.CompositeTypeDef:
		database.CompositeTypes = append(database.CompositeTypes, toCompositeType(schema, name, node, definition))
	case *ast.RangeTypeDef:
		database.Ranges = append(database.Ranges, toRange(schema, name, node, definition))
	case *ast.DomainTypeDef:
		database.Domains = append(database.Domains, toDomain(schema, name, node, definition))
	}
}

func toCompositeType(schema, name string, node *ast.CreateTypeNode, definition *ast.CompositeTypeDef) schemamodel.CompositeType {
	fields := make([]schemamodel.CompositeField, 0, len(definition.Fields))
	for _, field := range definition.Fields {
		fields = append(fields, schemamodel.CompositeField{
			Name: normalizeSQLIdentifier(field.Name),
			Type: field.Type,
		})
	}
	composite := schemamodel.CompositeType{
		Name:    name,
		Schema:  schema,
		Fields:  fields,
		Comment: node.Comment,
	}
	composite.Canonicalize()
	return composite
}

func toRange(schema, name string, node *ast.CreateTypeNode, definition *ast.RangeTypeDef) schemamodel.Range {
	rangeType := schemamodel.Range{
		Name:           name,
		Schema:         schema,
		Subtype:        definition.Subtype,
		SubtypeOpClass: definition.SubtypeOpClass,
		Collation:      definition.Collation,
		Canonical:      definition.Canonical,
		SubtypeDiff:    definition.SubtypeDiff,
		Comment:        node.Comment,
	}
	rangeType.Canonicalize()
	return rangeType
}

func toDomain(schema, name string, node *ast.CreateTypeNode, definition *ast.DomainTypeDef) schemamodel.Domain {
	domain := schemamodel.Domain{
		Name:     name,
		Schema:   schema,
		BaseType: definition.BaseType,
		NotNull:  !definition.Nullable,
		Check:    definition.Check,
		Comment:  node.Comment,
	}
	if definition.Default != nil {
		domain.Default = definition.Default.Value
		domain.DefaultExpr = definition.Default.Expression
	}
	domain.Canonicalize()
	return domain
}

// applyRoleComment attaches a COMMENT ON ROLE statement to the role it names.
// PostgreSQL has no inline role comment, so Ptah's renderer emits the comment
// as a second statement; reading it back is what keeps the pair round-tripping.
func applyRoleComment(database *schemamodel.Database, node *ast.CommentNode) {
	name, comment, ok := parseRoleComment(node.Text)
	if !ok {
		return
	}
	for index := range database.Roles {
		if database.Roles[index].Name == name {
			database.Roles[index].Comment = comment
			return
		}
	}
}

// parseRoleComment recognizes the COMMENT ON ROLE text that
// Parser.parseCommentStatement builds. The shape is fixed by that function, so
// this reads a known format rather than arbitrary SQL.
func parseRoleComment(text string) (name, comment string, ok bool) {
	const prefix = "COMMENT ON ROLE "
	if !strings.HasPrefix(text, prefix) {
		return "", "", false
	}
	rest := text[len(prefix):]
	separator := strings.LastIndex(rest, " IS ")
	if separator < 0 {
		return "", "", false
	}
	name = normalizeSQLIdentifier(strings.TrimSpace(rest[:separator]))
	comment = strings.TrimSpace(rest[separator+len(" IS "):])
	return name, unquoteSQLStringLiteral(comment), true
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
	bodies := make(map[string]string, len(database.Functions))
	for _, function := range database.Functions {
		bodies[function.Name] = function.Body
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
		if !owned[function.Name] {
			functions = append(functions, function)
		}
	}
	database.Functions = functions
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

// toPostgresRoutine converts a routine the PostgreSQL routine parser modelled.
//
// CREATE FUNCTION reaches this package as a CreateFunctionNode and CREATE
// PROCEDURE as this one, so a schema's procedures were parsed and then not
// converted (stokaro/ptah#2435).
func toPostgresRoutine(node *ast.PostgresRoutineNode) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(node.Name),
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Language:   node.Language,
		Body:       strings.TrimSpace(node.Body.SQL),
	}
	function.Canonicalize()
	return function
}

// toSQLServerRoutine converts a routine the SQL Server routine parser modelled.
func toSQLServerRoutine(node *ast.SQLServerRoutineNode) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(node.Name),
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Returns:    node.Returns,
		// The body is this dialect's own procedural language, not PL/pgSQL.
		// A routine reaching the model with no language is defaulted to
		// plpgsql, and the renderer then skips it saying it "declares language
		// plpgsql" -- a sentence about a T-SQL body that never said any such
		// thing (stokaro/ptah#2435).
		Language: "sql",
		Body:     strings.TrimSpace(node.Body.SQL),
	}
	function.Canonicalize()
	return function
}

// toMySQLRoutine converts a routine the MySQL routine parser modelled.
func toMySQLRoutine(node *ast.MySQLRoutineNode) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(node.Name),
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Returns:    node.Returns,
		// The body is this dialect's own procedural language, not PL/pgSQL.
		// A routine reaching the model with no language is defaulted to
		// plpgsql, and the renderer then skips it saying it "declares language
		// plpgsql" -- a sentence about a T-SQL body that never said any such
		// thing (stokaro/ptah#2435).
		Language: "sql",
		Body:     strings.TrimSpace(node.Body.SQL),
	}
	function.Canonicalize()
	return function
}
