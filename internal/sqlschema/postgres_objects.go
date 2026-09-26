package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
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

func toRole(node *ast.CreateRoleNode, sourcePlatform string) schemamodel.Role {
	return schemamodel.Role{
		Name:        roleName(sourcePlatform, node.Name),
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

// toDefaultPrivilege converts an ALTER DEFAULT PRIVILEGES ... GRANT statement
// into the declaration the model holds.
//
// The three identity strings are unquoted here because the identity is compared
// as text against what the catalog reports, and the catalog reports a role or a
// schema by its name rather than by the spelling a statement used. Without the
// unquoting, a rendered `FOR ROLE "app_owner"` reads back as a grantor whose
// name includes the quotes, so the declaration matches no live row and every
// comparison re-issues the statement.
//
// Canonicalize then upper-cases the privilege names and merges a name that
// appears twice, which is what makes the renderer's two statements -- one per
// grantability -- fold back into the one object they came from.
func toDefaultPrivilege(node *ast.DefaultPrivilegeNode, sourcePlatform string) schemamodel.DefaultPrivilege {
	privileges := make([]schemamodel.PrivilegeGrant, 0, len(node.Privileges))
	for _, privilege := range node.Privileges {
		privileges = append(privileges, schemamodel.PrivilegeGrant{
			Privilege:  privilege.Privilege,
			WithOption: privilege.WithOption,
		})
	}
	defaultPrivilege := schemamodel.DefaultPrivilege{
		Grantor:    roleName(sourcePlatform, node.Grantor),
		Schema:     normalizeSQLIdentifier(sourcePlatform, node.Schema),
		ObjectType: node.ObjectType,
		Grantee:    roleTarget(sourcePlatform, node.Grantee),
		Privileges: privileges,
		Comment:    node.Comment,
	}
	defaultPrivilege.Canonicalize()
	return defaultPrivilege
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
func toRLSPolicy(node *ast.CreatePolicyNode, sourcePlatform string) schemamodel.RLSPolicy {
	return schemamodel.RLSPolicy{
		Name:                normalizeSQLIdentifier(sourcePlatform, node.Name),
		Table:               normalizeSQLTableReference(sourcePlatform, node.Table),
		PolicyFor:           node.PolicyFor,
		ToRoles:             normalizeRoleList(sourcePlatform, node.ToRoles),
		UsingExpression:     node.UsingExpression,
		WithCheckExpression: node.WithCheckExpression,
		Restrictive:         node.Restrictive,
		Comment:             node.Comment,
	}
}

// normalizeRoleList reads each role in a policy's TO list through
// [roleTarget] while keeping the separator the renderer expects.
func normalizeRoleList(sourcePlatform, roles string) string {
	if strings.TrimSpace(roles) == "" {
		return ""
	}
	parts := strings.Split(roles, ",")
	for index, part := range parts {
		parts[index] = roleTarget(sourcePlatform, part)
	}
	return strings.Join(parts, ", ")
}

// toRLSEnabledTable resolves its table the same way [toRLSPolicy] does. The
// enablement and the policies on it have to name one relation, or the render
// turns row-level security on for one table and protects another.
func toRLSEnabledTable(node *ast.AlterTableEnableRLSNode, sourcePlatform string) schemamodel.RLSEnabledTable {
	return schemamodel.RLSEnabledTable{
		Table:   normalizeSQLTableReference(sourcePlatform, node.Table),
		Comment: node.Comment,
	}
}

// appendRowSecurity reads the two table-level row-level security statements
// and reports whether stmt was one of them.
//
// ENABLE becomes an enablement. FORCE is accepted here and folded into its
// enablement by [applyForcedRowSecurity], which runs after every statement is
// read because the ENABLE it qualifies may come later in the document. NO FORCE
// is refused: a schema says the owner is exempt by not declaring FORCE.
func appendRowSecurity(database *schemamodel.Database, stmt ast.Node, sourcePlatform string) (bool, error) {
	switch node := stmt.(type) {
	case *ast.AlterTableEnableRLSNode:
		database.RLSEnabledTables = append(database.RLSEnabledTables, toRLSEnabledTable(node, sourcePlatform))
		return true, nil
	case *ast.AlterTableForceRLSNode:
		if node.NoForce {
			return true, fmt.Errorf(
				"%w: ALTER TABLE %s NO FORCE ROW LEVEL SECURITY; a schema says its owner is exempt by not declaring FORCE",
				ErrUnmodeledStatement, node.Table)
		}
		return true, nil
	default:
		return false, nil
	}
}

// applyForcedRowSecurity marks each table a FORCE ROW LEVEL SECURITY statement
// names as forced on the enablement the same document declares.
//
// ENABLE and FORCE are two statements and may come in either order, so this
// runs after every statement is read. The table is resolved the way
// [toRLSEnabledTable] resolves it and then matched under the target's
// identifier rules, so `t1` and `public.t1` are one relation here as they are
// to the comparison that later reads the model.
//
// A FORCE for a table the document never enables is refused. PostgreSQL accepts
// it: measured on 18.6, the table then reports relforcerowsecurity true and
// relrowsecurity false, and its owner still reads every row, because FORCE
// changes nothing until row-level security is enabled. The model has no state
// for a forced table that is not enabled, and the other schema frontends cannot
// declare one either (an HCL row_security block requires enabled = true), so
// dropping the statement would lose it and inventing an enablement would add a
// control nobody wrote.
func applyForcedRowSecurity(database *schemamodel.Database, statements []ast.Node, sourcePlatform string) error {
	semantics := identifier.ForDialect(sourcePlatform)
	for _, statement := range statements {
		force, ok := statement.(*ast.AlterTableForceRLSNode)
		if !ok || force.NoForce {
			continue
		}
		key := semantics.QualifiedTableIdentityKey(normalizeSQLTableReference(sourcePlatform, force.Table))
		found := false
		for index := range database.RLSEnabledTables {
			if semantics.QualifiedTableIdentityKey(database.RLSEnabledTables[index].Table) == key {
				database.RLSEnabledTables[index].Forced = true
				found = true
			}
		}
		if !found {
			return fmt.Errorf(
				"ALTER TABLE %s FORCE ROW LEVEL SECURITY has no effect until the table enables row-level security; "+
					"declare ALTER TABLE %s ENABLE ROW LEVEL SECURITY too",
				force.Table, force.Table)
		}
	}
	return nil
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
		Leakproof:  node.Leakproof,
		Parallel:   node.Parallel,
		Strict:     node.Strict,
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
// belongs in. CREATE DOMAIN also arrives here, as a DomainTypeDef.
func appendCreateType(database *schemamodel.Database, node *ast.CreateTypeNode, sourcePlatform string) {
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
	switch definition := node.TypeDef.(type) {
	case *ast.EnumTypeDef:
		database.Enums = append(database.Enums, schemamodel.Enum{
			Name:   schemamodel.QualifyTableName(schema, name),
			Values: definition.Values,
		})
	case *ast.CompositeTypeDef:
		database.CompositeTypes = append(database.CompositeTypes, toCompositeType(schema, name, node, definition, sourcePlatform))
	case *ast.RangeTypeDef:
		database.Ranges = append(database.Ranges, toRange(schema, name, node, definition))
	case *ast.DomainTypeDef:
		database.Domains = append(database.Domains, toDomain(schema, name, node, definition, sourcePlatform))
	}
}

func toCompositeType(schema, name string, node *ast.CreateTypeNode, definition *ast.CompositeTypeDef, sourcePlatform string) schemamodel.CompositeType {
	fields := make([]schemamodel.CompositeField, 0, len(definition.Fields))
	for _, field := range definition.Fields {
		fields = append(fields, schemamodel.CompositeField{
			Name: normalizeSQLIdentifier(sourcePlatform, field.Name),
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

func toDomain(
	schema, name string, node *ast.CreateTypeNode, definition *ast.DomainTypeDef, sourcePlatform string,
) schemamodel.Domain {
	domain := schemamodel.Domain{
		Name:     name,
		Schema:   schema,
		BaseType: definition.BaseType,
		NotNull:  !definition.Nullable,
		Check:    definition.Check,
		Comment:  node.Comment,
	}
	if definition.Default != nil {
		domain.Default, domain.DefaultExpr = domainDefault(definition.Default, sourcePlatform)
	}
	domain.Canonicalize()
	return domain
}

// domainDefault splits a domain's DEFAULT the way [schemamodel.Domain] keeps
// it: Default holds a value, which the planner quotes, and DefaultExpr holds
// SQL, which it writes as it is.
//
// The parser hands over SQL either way, so only a string constant becomes a
// value, read as the server reads it. Copied into Default with its quotes,
// DEFAULT 'ab' is quoted a second time, and the plan sets the default to the
// four characters 'ab', quotes included (stokaro/ptah#3740). Anything else --
// a number, a keyword, a cast, a call -- is SQL and stays SQL, and DEFAULT
// NULL is no default at all, which is what the server stores for it.
func domainDefault(value *ast.DefaultValue, sourcePlatform string) (literal, expression string) {
	if value.Expression != "" {
		return "", value.Expression
	}
	written := strings.TrimSpace(value.Value)
	if !strings.HasPrefix(written, `"`) {
		if text, ok := lexer.StringValue(written, dialectlexer.Options(sourcePlatform)); ok {
			return text, ""
		}
	}
	if strings.EqualFold(written, "NULL") {
		return "", ""
	}
	return "", written
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

// toPostgresRoutine converts a routine the PostgreSQL routine parser modelled.
//
// CREATE FUNCTION reaches this package as a CreateFunctionNode and CREATE
// PROCEDURE as this one, so a schema's procedures were parsed and then not
// converted (stokaro/ptah#2435).
func toPostgresRoutine(node *ast.PostgresRoutineNode, sourcePlatform string) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(sourcePlatform, node.Name),
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Language:   node.Language,
		Body:       strings.TrimSpace(node.Body.SQL),
	}
	function.Canonicalize()
	return function
}

// toSQLServerRoutine converts a routine the SQL Server routine parser modelled.
func toSQLServerRoutine(node *ast.SQLServerRoutineNode, sourcePlatform string) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(sourcePlatform, node.Name),
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
func toMySQLRoutine(node *ast.MySQLRoutineNode, sourcePlatform string) schemamodel.Function {
	function := schemamodel.Function{
		Name:       normalizeSQLTableReference(sourcePlatform, node.Name),
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
