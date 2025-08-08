package goschema

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema/internal/parseutils"
)

func parseFieldComment(comment *ast.Comment, field *ast.Field, structName string, globalEnumsMap map[string]Enum, schemaFields *[]Field) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
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

		*schemaFields = append(*schemaFields, Field{
			StructName:     structName,
			FieldName:      name.Name,
			Name:           kv["name"],
			Type:           fieldType,
			Nullable:       kv["not_null"] != "true",
			Primary:        kv["primary"] == "true",
			AutoInc:        kv["auto_increment"] == "true",
			Unique:         kv["unique"] == "true",
			UniqueExpr:     kv["unique_expr"],
			Default:        kv["default"],
			DefaultExpr:    kv["default_expr"],
			Foreign:        kv["foreign"],
			ForeignKeyName: kv["foreign_key_name"],
			Enum:           enum,
			Check:          kv["check"],
			Comment:        kv["comment"],
			Overrides:      parseutils.ParsePlatformSpecific(kv),
		})
	}
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
	fields := strings.Split(kv["fields"], ",")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}

	// Determine target table name - use 'table' attribute if specified, otherwise leave empty for later resolution
	tableName := kv["table"]

	*schemaIndexes = append(*schemaIndexes, Index{
		StructName: structName,
		Name:       kv["name"],
		Fields:     fields,
		Unique:     kv["unique"] == "true",
		Comment:    kv["comment"],
		// PostgreSQL-specific features
		Type:      kv["type"],      // GIN, GIST, BTREE, HASH, etc.
		Condition: kv["condition"], // WHERE clause for partial indexes
		Operator:  kv["ops"],       // Operator class (gin_trgm_ops, etc.)
		TableName: tableName,       // Target table name
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

func parseTableComment(comment *ast.Comment, structName string, tableDirectives *[]Table) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	*tableDirectives = append(*tableDirectives, Table{
		StructName: structName,
		Name:       kv["name"],
		Engine:     kv["engine"],
		Comment:    kv["comment"],
		PrimaryKey: strings.Split(kv["primary_key"], ","),
		Checks:     strings.Split(kv["checks"], ","),
		CustomSQL:  kv["custom"],
		Overrides:  parseutils.ParsePlatformSpecific(kv),
	})
}

func parseComment(comment *ast.Comment, structName string, field *ast.Field, globalEnumsMap map[string]Enum, schemaFields *[]Field, embeddedFields *[]EmbeddedField, schemaIndexes *[]Index, extensions *[]Extension, functions *[]Function, rlsPolicies *[]RLSPolicy, rlsEnabledTables *[]RLSEnabledTable) {
	switch {
	case strings.HasPrefix(comment.Text, "//migrator:schema:field"):
		parseFieldComment(comment, field, structName, globalEnumsMap, schemaFields)
	case strings.HasPrefix(comment.Text, "//migrator:embedded"):
		parseEmbeddedComment(comment, field, structName, embeddedFields)
	case strings.HasPrefix(comment.Text, "//migrator:schema:index"):
		parseIndexComment(comment, structName, schemaIndexes)
	case strings.HasPrefix(comment.Text, "//migrator:schema:extension"):
		parseExtensionComment(comment, extensions)
	case strings.HasPrefix(comment.Text, "//migrator:schema:function"):
		parseFunctionComment(comment, structName, functions)
	case strings.HasPrefix(comment.Text, "//migrator:schema:rls:policy"):
		parseRLSPolicyComment(comment, structName, rlsPolicies)
	case strings.HasPrefix(comment.Text, "//migrator:schema:rls:enable"):
		parseRLSEnableComment(comment, structName, rlsEnabledTables)
	}
}

func processTableComments(structName string, genDecl *ast.GenDecl, tableDirectives *[]Table, extensions *[]Extension, functions *[]Function, rlsPolicies *[]RLSPolicy, rlsEnabledTables *[]RLSEnabledTable) {
	if genDecl.Doc == nil {
		return
	}

	for _, comment := range genDecl.Doc.List {
		switch {
		case strings.HasPrefix(comment.Text, "//migrator:schema:table"):
			parseTableComment(comment, structName, tableDirectives)
		case strings.HasPrefix(comment.Text, "//migrator:schema:extension"):
			parseExtensionComment(comment, extensions)
		case strings.HasPrefix(comment.Text, "//migrator:schema:function"):
			parseFunctionComment(comment, structName, functions)
		case strings.HasPrefix(comment.Text, "//migrator:schema:rls:policy"):
			parseRLSPolicyComment(comment, structName, rlsPolicies)
		case strings.HasPrefix(comment.Text, "//migrator:schema:rls:enable"):
			parseRLSEnableComment(comment, structName, rlsEnabledTables)
		}
	}
}

func processFieldComments(structName string, structType *ast.StructType, globalEnumsMap map[string]Enum, schemaFields *[]Field, embeddedFields *[]EmbeddedField, schemaIndexes *[]Index, extensions *[]Extension, functions *[]Function, rlsPolicies *[]RLSPolicy, rlsEnabledTables *[]RLSEnabledTable) {
	for _, field := range structType.Fields.List {
		if field.Doc == nil {
			continue
		}
		for _, comment := range field.Doc.List {
			parseComment(comment, structName, field, globalEnumsMap, schemaFields, embeddedFields, schemaIndexes, extensions, functions, rlsPolicies, rlsEnabledTables)
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

	var embeddedFields []EmbeddedField
	var schemaFields []Field
	var schemaIndexes []Index
	var tableDirectives []Table
	var extensions []Extension
	var functions []Function
	var rlsPolicies []RLSPolicy
	var rlsEnabledTables []RLSEnabledTable
	globalEnumsMap := make(map[string]Enum)

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
			processTableComments(structName, genDecl, &tableDirectives, &extensions, &functions, &rlsPolicies, &rlsEnabledTables)
			processFieldComments(structName, structType, globalEnumsMap, &schemaFields, &embeddedFields, &schemaIndexes, &extensions, &functions, &rlsPolicies, &rlsEnabledTables)
		}
	}

	enums := make([]Enum, 0, len(globalEnumsMap))
	keys := make([]string, 0, len(globalEnumsMap))
	for k := range globalEnumsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		enums = append(enums, globalEnumsMap[k])
	}

	result := Database{
		Tables:           tableDirectives,
		Fields:           schemaFields,
		Indexes:          schemaIndexes,
		Enums:            enums,
		EmbeddedFields:   embeddedFields,
		Extensions:       extensions,
		Functions:        functions,
		RLSPolicies:      rlsPolicies,
		RLSEnabledTables: rlsEnabledTables,
		Dependencies:     make(map[string][]string),
	}
	buildDependencyGraph(&result)
	return result
}

func parseFunctionComment(comment *ast.Comment, structName string, functions *[]Function) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	*functions = append(*functions, Function{
		StructName: structName,
		Name:       kv["name"],
		Parameters: kv["params"],
		Returns:    kv["returns"],
		Language:   kv["language"],
		Security:   kv["security"],
		Volatility: kv["volatility"],
		Body:       kv["body"],
		Comment:    kv["comment"],
	})
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
