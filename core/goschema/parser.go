package goschema

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/goschema/internal/parseutils"
)

// knownIndexAttributes lists every attribute key recognized on a
// //migrator:schema:index annotation. Keys with the "platform." prefix are
// also accepted. The strict-unknown-key rejection mechanism (see
// validateAttributes) is reused so typos like "granluarity" surface at parse
// time rather than being silently dropped.
//
// "columns" is accepted as a legacy synonym for "fields"; several integration
// fixtures use it. parseIndexComment falls back to it when "fields" is empty.
var knownIndexAttributes = map[string]bool{
	"name":        true,
	"fields":      true,
	"columns":     true,
	"unique":      true,
	"comment":     true,
	"type":        true,
	"condition":   true,
	"ops":         true,
	"table":       true,
	"granularity": true,
}

// knownFieldAttributes lists every attribute key recognized on a
// //migrator:schema:field annotation. Keys with the "platform." prefix are
// also accepted (handled separately).
//
// "nullable", "index", and "autoincrement" are whitelisted because parseutils
// auto-promotes them to booleans when written as bare words.
var knownFieldAttributes = map[string]bool{
	"name":                true,
	"type":                true,
	"not_null":            true,
	"nullable":            true,
	"primary":             true,
	"auto_increment":      true,
	"autoincrement":       true,
	"identity_generation": true,
	"identity_start":      true,
	"identity_increment":  true,
	"identity_options":    true,
	"unique":              true,
	"unique_expr":         true,
	"index":               true,
	"default":             true,
	"default_expr":        true,
	"foreign":             true,
	"foreign_key_name":    true,
	"on_delete":           true,
	"on_update":           true,
	"enum":                true,
	"check":               true,
	"check_name":          true,
	"comment":             true,
}

var knownViewAttributes = map[string]bool{
	"name":       true,
	"body":       true,
	"with_check": true,
	"comment":    true,
}

var knownMaterializedViewAttributes = map[string]bool{
	"name":             true,
	"body":             true,
	"refresh_strategy": true,
	"comment":          true,
}

var knownTriggerAttributes = map[string]bool{
	"name":    true,
	"table":   true,
	"timing":  true,
	"event":   true,
	"for":     true,
	"body":    true,
	"comment": true,
}

var knownSchemaAttributes = map[string]bool{
	"name":    true,
	"comment": true,
}

// validateAttributes panics if kv contains any key the directive does not
// recognize. Platform-specific overrides (platform.*) are always allowed.
// This catches typos like default_fn-vs-default_expr at parse time instead of
// silently dropping them and producing wrong SQL.
func validateAttributes(kv map[string]string, known map[string]bool, directive, location string) {
	for k := range kv {
		if known[k] || strings.HasPrefix(k, "platform.") {
			continue
		}
		slog.Error("unknown annotation attribute",
			"directive", directive,
			"attribute", k,
			"location", location,
		)
		panic(fmt.Sprintf("unknown annotation attribute %q on %s at %s", k, directive, location))
	}
}

func requireAttributes(kv map[string]string, required []string, directive, location string) {
	for _, key := range required {
		if strings.TrimSpace(kv[key]) != "" {
			continue
		}
		slog.Error("missing required annotation attribute",
			"directive", directive,
			"attribute", key,
			"location", location,
		)
		panic(fmt.Sprintf("missing required annotation attribute %q on %s at %s", key, directive, location))
	}
}

func parseFieldComment(comment *ast.Comment, field *ast.Field, structName string, globalEnumsMap map[string]Enum, schemaFields *[]Field) {
	kv := parseutils.ParseKeyValueComment(comment.Text)

	// Validate the directive itself, not each named carrier. For anonymous /
	// embedded fields field.Names is nil and the loop below would never run,
	// so doing this inside the loop would let unknown keys slip through.
	location := structName
	if len(field.Names) > 0 {
		location = structName + "." + field.Names[0].Name
	}
	validateAttributes(kv, knownFieldAttributes, "//migrator:schema:field", location)

	for _, name := range field.Names {
		enumRaw := kv["enum"]
		var enum []string
		if enumRaw != "" {
			enum = strings.Split(enumRaw, ",")
			for i := range enum {
				enum[i] = strings.TrimSpace(enum[i])
			}
		}

		// Determine the field type - if it's ENUM with enum values, use the generated enum name
		fieldType := kv["type"]
		if len(enumRaw) > 0 && kv["type"] == "ENUM" {
			enumName := "enum_" + strings.ToLower(structName) + "_" + strings.ToLower(name.Name)
			globalEnumsMap[enumName] = Enum{
				Name:   enumName,
				Values: enum,
			}
			// Update the field type to use the generated enum name
			fieldType = enumName
		}

		identityGeneration := normalizeIdentityGeneration(kv["identity_generation"])
		if kv["identity_generation"] != "" && identityGeneration == "" {
			panic(fmt.Sprintf("invalid identity_generation %q on //migrator:schema:field at %s", kv["identity_generation"], location))
		}
		if identityGeneration == "" && hasIdentitySettings(kv) {
			identityGeneration = "BY_DEFAULT"
		}
		_, defaultSet := kv["default"]
		*schemaFields = append(*schemaFields, Field{
			StructName:         structName,
			FieldName:          name.Name,
			Name:               kv["name"],
			Type:               fieldType,
			Nullable:           kv["not_null"] != "true",
			Primary:            kv["primary"] == "true",
			AutoInc:            kv["auto_increment"] == "true" || identityGeneration != "",
			IdentityGeneration: identityGeneration,
			IdentityStart:      kv["identity_start"],
			IdentityIncrement:  kv["identity_increment"],
			IdentityOptions:    kv["identity_options"],
			Unique:             kv["unique"] == "true",
			UniqueExpr:         kv["unique_expr"],
			Default:            kv["default"],
			DefaultSet:         defaultSet,
			DefaultExpr:        kv["default_expr"],
			Foreign:            kv["foreign"],
			ForeignKeyName:     kv["foreign_key_name"],
			OnDelete:           kv["on_delete"],
			OnUpdate:           kv["on_update"],
			Enum:               enum,
			Check:              kv["check"],
			CheckName:          kv["check_name"],
			Comment:            kv["comment"],
			Overrides:          parseutils.ParsePlatformSpecific(kv),
		})
	}
}

func hasIdentitySettings(kv map[string]string) bool {
	return kv["identity_start"] != "" || kv["identity_increment"] != "" || kv["identity_options"] != ""
}

func parseEmbeddedComment(comment *ast.Comment, field *ast.Field, structName string, embeddedFields *[]EmbeddedField) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	// Handle embedded fields - get the field type name
	var fieldTypeName string
	if field.Type != nil {
		switch t := field.Type.(type) {
		case *ast.Ident:
			// Value embedded field: BaseID
			fieldTypeName = t.Name
		case *ast.StarExpr:
			// Pointer embedded field: *BaseID
			if ident, ok := t.X.(*ast.Ident); ok {
				fieldTypeName = ident.Name
			}
		}
	}

	*embeddedFields = append(*embeddedFields, EmbeddedField{
		StructName:       structName,
		Mode:             kv["mode"],
		Prefix:           kv["prefix"],
		Name:             kv["name"],
		Type:             kv["type"],
		Nullable:         kv["nullable"] == "true",
		Index:            kv["index"] == "true",
		Field:            kv["field"],
		Ref:              kv["ref"],
		OnDelete:         kv["on_delete"],
		OnUpdate:         kv["on_update"],
		Comment:          kv["comment"],
		EmbeddedTypeName: fieldTypeName,
		Overrides:        parseutils.ParsePlatformSpecific(kv),
	})
}

func parseIndexComment(comment *ast.Comment, structName string, schemaIndexes *[]Index) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	validateAttributes(kv, knownIndexAttributes, "//migrator:schema:index", structName)

	// "columns=" is a legacy synonym for "fields=" (several integration
	// fixtures still spell it that way); prefer the modern name and fall
	// back to the legacy form so neither is silently dropped.
	fieldsRaw := kv["fields"]
	if fieldsRaw == "" {
		fieldsRaw = kv["columns"]
	}
	fields := strings.Split(fieldsRaw, ",")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}

	// Determine target table name - use 'table' attribute if specified, otherwise leave empty for later resolution
	tableName := kv["table"]

	// Granularity is optional and only meaningful for ClickHouse data-skipping
	// indexes. Empty / unset => 0, which the ClickHouse renderer interprets as
	// "use the documented default". Invalid integers panic at parse time so
	// users see the typo immediately rather than getting a wrong default.
	var granularity int
	if g := strings.TrimSpace(kv["granularity"]); g != "" {
		n, err := strconv.Atoi(g)
		if err != nil || n < 0 {
			panic(fmt.Sprintf("invalid granularity %q on //migrator:schema:index at %s (must be a non-negative integer)", g, structName))
		}
		granularity = n
	}

	*schemaIndexes = append(*schemaIndexes, Index{
		StructName:  structName,
		Name:        kv["name"],
		Fields:      fields,
		Unique:      kv["unique"] == "true",
		Comment:     kv["comment"],
		Type:        kv["type"],      // PG: GIN/GIST/BTREE/HASH; CH: minmax/set(N)/bloom_filter/...
		Condition:   kv["condition"], // PG only: WHERE clause for partial indexes
		Operator:    kv["ops"],       // PG only: operator class (gin_trgm_ops, etc.)
		TableName:   tableName,       // Target table name
		Granularity: granularity,     // CH only: GRANULARITY n for data-skipping indexes
	})
}

// ParseConstraintComment parses a constraint comment and adds it to the constraints slice.
// This function is exported for testing purposes.
func ParseConstraintComment(comment *ast.Comment, structName string, schemaConstraints *[]Constraint) {
	kv := parseutils.ParseKeyValueComment(comment.Text)

	// Parse columns for UNIQUE/PRIMARY KEY constraints
	var columns []string
	if kv["columns"] != "" {
		columns = strings.Split(kv["columns"], ",")
		for i := range columns {
			columns[i] = strings.TrimSpace(columns[i])
		}
	}

	// Determine target table name - use 'table' attribute if specified, otherwise leave empty for later resolution
	tableName := kv["table"]

	*schemaConstraints = append(*schemaConstraints, Constraint{
		StructName: structName,
		Name:       kv["name"],
		Type:       strings.ToUpper(kv["type"]), // EXCLUDE, CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY
		Table:      tableName,

		// EXCLUDE constraint specific fields
		UsingMethod:     kv["using"],     // Index method (gist, btree, etc.)
		ExcludeElements: kv["elements"],  // Elements specification
		WhereCondition:  kv["condition"], // WHERE clause

		// CHECK constraint specific fields
		CheckExpression: kv["check"], // Check expression

		// UNIQUE/PRIMARY KEY constraint specific fields
		Columns: columns, // Column names

		// FOREIGN KEY constraint specific fields
		ForeignTable:  kv["foreign_table"],  // Referenced table
		ForeignColumn: kv["foreign_column"], // Referenced column
		OnDelete:      kv["on_delete"],      // ON DELETE action
		OnUpdate:      kv["on_update"],      // ON UPDATE action

		Comment: kv["comment"], // Constraint comment
	})
}

func parseExtensionComment(comment *ast.Comment, extensions *[]Extension) {
	kv := parseutils.ParseKeyValueComment(comment.Text)

	*extensions = append(*extensions, Extension{
		Name:        kv["name"],
		IfNotExists: kv["if_not_exists"] == "true",
		Version:     kv["version"],
		Comment:     kv["comment"],
	})
}

func parseSchemaComment(comment *ast.Comment, schemas *[]Schema) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	validateAttributes(kv, knownSchemaAttributes, "//migrator:schema:schema", kv["name"])
	requireAttributes(kv, []string{"name"}, "//migrator:schema:schema", kv["name"])

	*schemas = append(*schemas, Schema{
		Name:    kv["name"],
		Comment: kv["comment"],
	})
}

func parseTableComment(comment *ast.Comment, structName string, tableDirectives *[]Table) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	*tableDirectives = append(*tableDirectives, Table{
		StructName: structName,
		Name:       kv["name"],
		Schema:     kv["schema"],
		Engine:     kv["engine"],
		Comment:    kv["comment"],
		PrimaryKey: strings.Split(kv["primary_key"], ","),
		Checks:     strings.Split(kv["checks"], ","),
		CustomSQL:  kv["custom"],
		Overrides:  parseutils.ParsePlatformSpecific(kv),
	})
}

func parseComment(
	comment *ast.Comment,
	structName string,
	field *ast.Field,
	globalEnumsMap map[string]Enum,
	schemaFields *[]Field,
	embeddedFields *[]EmbeddedField,
	schemaIndexes *[]Index,
	schemaConstraints *[]Constraint,
	extensions *[]Extension,
	functions *[]Function,
	views *[]View,
	materializedViews *[]MaterializedView,
	triggers *[]Trigger,
	rlsPolicies *[]RLSPolicy,
	rlsEnabledTables *[]RLSEnabledTable,
	roles *[]Role,
	grants *[]Grant,
) {
	switch {
	case strings.HasPrefix(comment.Text, "//migrator:schema:field"):
		parseFieldComment(comment, field, structName, globalEnumsMap, schemaFields)
	case strings.HasPrefix(comment.Text, "//migrator:embedded"):
		parseEmbeddedComment(comment, field, structName, embeddedFields)
	case strings.HasPrefix(comment.Text, "//migrator:schema:index"):
		parseIndexComment(comment, structName, schemaIndexes)
	case strings.HasPrefix(comment.Text, "//migrator:schema:constraint"):
		ParseConstraintComment(comment, structName, schemaConstraints)
	case strings.HasPrefix(comment.Text, "//migrator:schema:extension"):
		parseExtensionComment(comment, extensions)
	case strings.HasPrefix(comment.Text, "//migrator:schema:function"):
		parseFunctionComment(comment, structName, functions)
	case strings.HasPrefix(comment.Text, "//migrator:schema:view"):
		parseViewComment(comment, structName, views)
	case strings.HasPrefix(comment.Text, "//migrator:schema:matview"):
		parseMaterializedViewComment(comment, structName, materializedViews)
	case strings.HasPrefix(comment.Text, "//migrator:schema:trigger"):
		parseTriggerComment(comment, structName, triggers)
	case strings.HasPrefix(comment.Text, "//migrator:schema:rls:policy"):
		parseRLSPolicyComment(comment, structName, rlsPolicies)
	case strings.HasPrefix(comment.Text, "//migrator:schema:rls:enable"):
		parseRLSEnableComment(comment, structName, rlsEnabledTables)
	case strings.HasPrefix(comment.Text, "//migrator:schema:role"):
		parseRoleComment(comment, structName, roles)
	case strings.HasPrefix(comment.Text, "//migrator:schema:grant"):
		parseGrantComment(comment, structName, grants)
	}
}

func processTableComments(
	structName string,
	genDecl *ast.GenDecl,
	tableDirectives *[]Table,
	schemaConstraints *[]Constraint,
	extensions *[]Extension,
	functions *[]Function,
	views *[]View,
	materializedViews *[]MaterializedView,
	triggers *[]Trigger,
	rlsPolicies *[]RLSPolicy,
	rlsEnabledTables *[]RLSEnabledTable,
	roles *[]Role,
	grants *[]Grant,
	schemas *[]Schema,
) {
	if genDecl.Doc == nil {
		return
	}

	for _, comment := range genDecl.Doc.List {
		switch {
		case strings.HasPrefix(comment.Text, "//migrator:schema:table"):
			parseTableComment(comment, structName, tableDirectives)
		case strings.HasPrefix(comment.Text, "//migrator:schema:constraint"):
			ParseConstraintComment(comment, structName, schemaConstraints)
		case strings.HasPrefix(comment.Text, "//migrator:schema:extension"):
			parseExtensionComment(comment, extensions)
		case strings.HasPrefix(comment.Text, "//migrator:schema:function"):
			parseFunctionComment(comment, structName, functions)
		case strings.HasPrefix(comment.Text, "//migrator:schema:view"):
			parseViewComment(comment, structName, views)
		case strings.HasPrefix(comment.Text, "//migrator:schema:matview"):
			parseMaterializedViewComment(comment, structName, materializedViews)
		case strings.HasPrefix(comment.Text, "//migrator:schema:trigger"):
			parseTriggerComment(comment, structName, triggers)
		case strings.HasPrefix(comment.Text, "//migrator:schema:rls:policy"):
			parseRLSPolicyComment(comment, structName, rlsPolicies)
		case strings.HasPrefix(comment.Text, "//migrator:schema:rls:enable"):
			parseRLSEnableComment(comment, structName, rlsEnabledTables)
		case strings.HasPrefix(comment.Text, "//migrator:schema:role"):
			parseRoleComment(comment, structName, roles)
		case strings.HasPrefix(comment.Text, "//migrator:schema:grant"):
			parseGrantComment(comment, structName, grants)
		case strings.HasPrefix(comment.Text, "//migrator:schema:schema"):
			parseSchemaComment(comment, schemas)
		}
	}
}

func processFieldComments(
	structName string,
	structType *ast.StructType,
	globalEnumsMap map[string]Enum,
	schemaFields *[]Field,
	embeddedFields *[]EmbeddedField,
	schemaIndexes *[]Index,
	schemaConstraints *[]Constraint,
	extensions *[]Extension,
	functions *[]Function,
	views *[]View,
	materializedViews *[]MaterializedView,
	triggers *[]Trigger,
	rlsPolicies *[]RLSPolicy,
	rlsEnabledTables *[]RLSEnabledTable,
	roles *[]Role,
	grants *[]Grant,
) {
	for _, field := range structType.Fields.List {
		if field.Doc == nil {
			continue
		}
		for _, comment := range field.Doc.List {
			parseComment(comment, structName, field, globalEnumsMap, schemaFields, embeddedFields, schemaIndexes, schemaConstraints, extensions, functions, views, materializedViews, triggers, rlsPolicies, rlsEnabledTables, roles, grants)
		}
	}
}

func ParseFile(filename string) Database {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		panic("Failed to parse file")
	}

	return parseFileAST(f)
}

// ParseSource parses a Go source string and returns the database schema
// source can be a string, []byte, or io.Reader
func ParseSource(filename string, source any) Database {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, source, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		panic("Failed to parse file")
	}

	return parseFileAST(f)
}

func parseFileAST(f *ast.File) Database {
	var embeddedFields []EmbeddedField
	var schemaFields []Field
	var schemaIndexes []Index
	var schemaConstraints []Constraint
	var tableDirectives []Table
	var extensions []Extension
	var functions []Function
	var views []View
	var materializedViews []MaterializedView
	var triggers []Trigger
	var rlsPolicies []RLSPolicy
	var rlsEnabledTables []RLSEnabledTable
	var roles []Role
	var grants []Grant
	var schemas []Schema
	globalEnumsMap := make(map[string]Enum)

	// Single pass: collect table names and process all declarations and comments
	tableNameToStructName := make(map[string]string)
	processFileAST(
		f,
		tableNameToStructName,
		globalEnumsMap,
		&embeddedFields,
		&schemaFields,
		&schemaIndexes,
		&schemaConstraints,
		&tableDirectives,
		&extensions,
		&functions,
		&views,
		&materializedViews,
		&triggers,
		&rlsPolicies,
		&rlsEnabledTables,
		&roles,
		&grants,
		&schemas,
	)

	enums := make([]Enum, 0, len(globalEnumsMap))
	keys := make([]string, 0, len(globalEnumsMap))
	for k := range globalEnumsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		enums = append(enums, globalEnumsMap[k])
	}

	// Sort extensions alphabetically for consistent output
	sort.Slice(extensions, func(i, j int) bool {
		return extensions[i].Name < extensions[j].Name
	})

	result := Database{
		Schemas:           schemas,
		Tables:            tableDirectives,
		Fields:            schemaFields,
		Indexes:           schemaIndexes,
		Constraints:       schemaConstraints,
		Enums:             enums,
		EmbeddedFields:    embeddedFields,
		Extensions:        extensions,
		Functions:         functions,
		Views:             views,
		MaterializedViews: materializedViews,
		Triggers:          triggers,
		RLSPolicies:       rlsPolicies,
		RLSEnabledTables:  rlsEnabledTables,
		Roles:             roles,
		Grants:            grants,
		Dependencies:      make(map[string][]string),
	}
	normalizeTableScopedNames(&result)
	buildDependencyGraph(&result)
	return result
}

// processFileAST processes the entire AST file in a single optimized pass
func processFileAST(
	f *ast.File,
	tableNameToStructName map[string]string,
	globalEnumsMap map[string]Enum,
	embeddedFields *[]EmbeddedField,
	schemaFields *[]Field,
	schemaIndexes *[]Index,
	schemaConstraints *[]Constraint,
	tableDirectives *[]Table,
	extensions *[]Extension,
	functions *[]Function,
	views *[]View,
	materializedViews *[]MaterializedView,
	triggers *[]Trigger,
	rlsPolicies *[]RLSPolicy,
	rlsEnabledTables *[]RLSEnabledTable,
	roles *[]Role,
	grants *[]Grant,
	schemas *[]Schema,
) {
	// First, collect table names from struct declarations
	for _, decl := range f.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structName := typeSpec.Name.Name
			_, ok = typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}

			mapTableDirectiveStructNames(genDecl.Doc, structName, tableNameToStructName)
		}
	}

	// Process all struct declarations
	processDeclarations(
		f,
		tableNameToStructName,
		globalEnumsMap,
		embeddedFields,
		schemaFields,
		schemaIndexes,
		schemaConstraints,
		tableDirectives,
		extensions,
		functions,
		views,
		materializedViews,
		triggers,
		rlsPolicies,
		rlsEnabledTables,
		roles,
		grants,
		schemas,
	)

	// Process all file comments for RLS annotations that might not be associated with struct declarations
	processAllFileComments(f, tableNameToStructName, rlsPolicies, rlsEnabledTables)
}

func mapTableDirectiveStructNames(doc *ast.CommentGroup, structName string, tableNameToStructName map[string]string) {
	if doc == nil {
		return
	}
	for _, comment := range doc.List {
		if !strings.HasPrefix(comment.Text, "//migrator:schema:table") {
			continue
		}
		kv := parseutils.ParseKeyValueComment(comment.Text)
		tableName := kv["name"]
		if tableName == "" {
			continue
		}
		tableNameToStructName[tableName] = structName
		if schemaName := kv["schema"]; schemaName != "" {
			tableNameToStructName[QualifyTableName(schemaName, tableName)] = structName
		}
	}
}

// processDeclarations processes all struct declarations in the file
func processDeclarations(
	f *ast.File,
	tableNameToStructName map[string]string,
	globalEnumsMap map[string]Enum,
	embeddedFields *[]EmbeddedField,
	schemaFields *[]Field,
	schemaIndexes *[]Index,
	schemaConstraints *[]Constraint,
	tableDirectives *[]Table,
	extensions *[]Extension,
	functions *[]Function,
	views *[]View,
	materializedViews *[]MaterializedView,
	triggers *[]Trigger,
	rlsPolicies *[]RLSPolicy,
	rlsEnabledTables *[]RLSEnabledTable,
	roles *[]Role,
	grants *[]Grant,
	schemas *[]Schema,
) {
	for _, decl := range f.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structName := typeSpec.Name.Name
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}

			processTableComments(structName, genDecl, tableDirectives, schemaConstraints, extensions, functions, views, materializedViews, triggers, rlsPolicies, rlsEnabledTables, roles, grants, schemas)
			processFieldComments(structName, structType, globalEnumsMap, schemaFields, embeddedFields, schemaIndexes, schemaConstraints, extensions, functions, views, materializedViews, triggers, rlsPolicies, rlsEnabledTables, roles, grants)
		}
	}
}

// processAllFileComments scans all comments in the file for RLS annotations
// that might not be directly associated with struct declarations due to blank lines
func processAllFileComments(f *ast.File, tableNameToStructName map[string]string, rlsPolicies *[]RLSPolicy, rlsEnabledTables *[]RLSEnabledTable) {
	// Create sets to track already processed RLS policies and enabled tables to avoid duplicates
	existingPolicies := make(map[string]bool)
	existingEnabledTables := make(map[string]bool)

	for _, policy := range *rlsPolicies {
		existingPolicies[policy.Name] = true
	}

	for _, table := range *rlsEnabledTables {
		existingEnabledTables[table.Table] = true
	}

	// Scan all comment groups in the file
	for _, commentGroup := range f.Comments {
		for _, comment := range commentGroup.List {
			switch {
			case strings.HasPrefix(comment.Text, "//migrator:schema:rls:policy"):
				kv := parseutils.ParseKeyValueComment(comment.Text)
				policyName := kv["name"]
				tableName := kv["table"]

				// Skip if we already have this policy, if policy name is empty, or if we can't find the struct name
				if existingPolicies[policyName] || policyName == "" || tableName == "" {
					continue
				}

				structName, exists := tableNameToStructName[tableName]
				if !exists {
					continue
				}

				policy := RLSPolicy{
					StructName:          structName,
					Name:                policyName,
					Table:               tableName,
					PolicyFor:           kv["for"],
					ToRoles:             kv["to"],
					UsingExpression:     kv["using"],
					WithCheckExpression: kv["with_check"],
					Comment:             kv["comment"],
				}

				*rlsPolicies = append(*rlsPolicies, policy)
				existingPolicies[policyName] = true

			case strings.HasPrefix(comment.Text, "//migrator:schema:rls:enable"):
				kv := parseutils.ParseKeyValueComment(comment.Text)
				tableName := kv["table"]

				// Skip if we already have this table enabled or if we can't find the struct name
				if existingEnabledTables[tableName] || tableName == "" {
					continue
				}

				structName, exists := tableNameToStructName[tableName]
				if !exists {
					continue
				}

				rlsEnabled := RLSEnabledTable{
					StructName: structName,
					Table:      tableName,
					Comment:    kv["comment"],
				}

				*rlsEnabledTables = append(*rlsEnabledTables, rlsEnabled)
				existingEnabledTables[tableName] = true
			}
		}
	}
}

func parseFunctionComment(comment *ast.Comment, structName string, functions *[]Function) {
	kv := parseutils.ParseKeyValueComment(comment.Text)

	fn := Function{
		StructName: structName,
		Name:       kv["name"],
		Parameters: kv["params"],
		Returns:    kv["returns"],
		Language:   kv["language"],
		Security:   kv["security"],
		Volatility: kv["volatility"],
		Body:       kv["body"],
		Comment:    kv["comment"],
	}
	// Canonicalize so every downstream consumer (planner, renderer,
	// comparator) sees the same values regardless of how the annotation was
	// typed. See Function.Canonicalize for the per-field rules.
	fn.Canonicalize()
	*functions = append(*functions, fn)
}

func parseViewComment(comment *ast.Comment, structName string, views *[]View) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	validateAttributes(kv, knownViewAttributes, "//migrator:schema:view", structName)
	requireAttributes(kv, []string{"name", "body"}, "//migrator:schema:view", structName)
	*views = append(*views, View{
		StructName: structName,
		Name:       kv["name"],
		Body:       kv["body"],
		WithCheck:  kv["with_check"] == "true",
		Comment:    kv["comment"],
	})
}

func parseMaterializedViewComment(comment *ast.Comment, structName string, materializedViews *[]MaterializedView) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	validateAttributes(kv, knownMaterializedViewAttributes, "//migrator:schema:matview", structName)
	requireAttributes(kv, []string{"name", "body"}, "//migrator:schema:matview", structName)
	refreshStrategy := kv["refresh_strategy"]
	if refreshStrategy == "" {
		refreshStrategy = "manual"
	}
	matView := MaterializedView{
		StructName:      structName,
		Name:            kv["name"],
		Body:            kv["body"],
		RefreshStrategy: strings.ToLower(refreshStrategy),
		Comment:         kv["comment"],
	}
	matView.Canonicalize()
	*materializedViews = append(*materializedViews, matView)
}

func parseTriggerComment(comment *ast.Comment, structName string, triggers *[]Trigger) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	validateAttributes(kv, knownTriggerAttributes, "//migrator:schema:trigger", structName)
	requireAttributes(kv, []string{"name", "table", "timing", "event", "body"}, "//migrator:schema:trigger", structName)
	trigger := Trigger{
		StructName: structName,
		Name:       kv["name"],
		Table:      kv["table"],
		Timing:     kv["timing"],
		Event:      kv["event"],
		ForEach:    kv["for"],
		Body:       kv["body"],
		Comment:    kv["comment"],
	}
	trigger.Canonicalize()
	*triggers = append(*triggers, trigger)
}

func parseRLSPolicyComment(comment *ast.Comment, structName string, rlsPolicies *[]RLSPolicy) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	*rlsPolicies = append(*rlsPolicies, RLSPolicy{
		StructName:          structName,
		Name:                kv["name"],
		Table:               kv["table"],
		PolicyFor:           kv["for"],
		ToRoles:             kv["to"],
		UsingExpression:     kv["using"],
		WithCheckExpression: kv["with_check"],
		Comment:             kv["comment"],
	})
}

func parseRLSEnableComment(comment *ast.Comment, structName string, rlsEnabledTables *[]RLSEnabledTable) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	*rlsEnabledTables = append(*rlsEnabledTables, RLSEnabledTable{
		StructName: structName,
		Table:      kv["table"],
		Comment:    kv["comment"],
	})
}

func parseRoleComment(comment *ast.Comment, structName string, roles *[]Role) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	*roles = append(*roles, Role{
		StructName:  structName,
		Name:        kv["name"],
		Login:       kv["login"] == "true",
		Password:    kv["password"],
		Superuser:   kv["superuser"] == "true",
		CreateDB:    kv["createdb"] == "true" || kv["create_db"] == "true",
		CreateRole:  kv["createrole"] == "true" || kv["create_role"] == "true",
		Inherit:     kv["inherit"] != "false", // Default to true unless explicitly set to false
		Replication: kv["replication"] == "true",
		Comment:     kv["comment"],
	})
}

func parseGrantComment(comment *ast.Comment, structName string, grants *[]Grant) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	privileges := splitCommaList(kv["privilege"])
	if len(privileges) == 0 {
		privileges = splitCommaList(kv["privileges"])
	}
	grant := Grant{
		StructName: structName,
		Role:       kv["role"],
		Privileges: privileges,
		OnTable:    kv["on_table"],
		OnSchema:   kv["on_schema"],
		WithOption: kv["with_option"] == "true" || kv["grant_option"] == "true",
		Comment:    kv["comment"],
	}
	grant.Canonicalize()
	*grants = append(*grants, grant)
}

func splitCommaList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// ParseFileWithDependencies parses a Go file and automatically discovers and parses
// related files in the same directory to resolve embedded type references
func ParseFileWithDependencies(filename string) Database {
	// Parse the main file
	database := ParseFile(filename)

	// Get the directory of the main file
	dir := filepath.Dir(filename)

	// Parse all other .go files in the same directory to find embedded type definitions
	pattern := filepath.Join(dir, "*.go")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		slog.Warn("Failed to find related files", "error", err)
		return database
	}

	// Collect embedded type names that we need to resolve
	embeddedTypeNames := make(map[string]bool)
	for _, embedded := range database.EmbeddedFields {
		embeddedTypeNames[embedded.EmbeddedTypeName] = true
	}

	// Parse each related file to collect embedded type definitions
	for _, match := range matches {
		if match == filename {
			continue // Skip the main file as it's already parsed
		}

		// Parse the related file
		dbmatch := ParseFile(match)
		relatedFields := dbmatch.Fields

		// Only add fields from embedded types that we actually need
		for _, field := range relatedFields {
			if embeddedTypeNames[field.StructName] {
				database.Fields = append(database.Fields, field)
			}
		}
	}

	buildDependencyGraph(&database)
	return database
}
