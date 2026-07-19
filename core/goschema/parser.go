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

// validateAttributes rejects any key the directive does not recognize.
// Platform-specific overrides (platform.*) are always allowed. This catches
// typos like default_fn-vs-default_expr at parse time instead of silently
// dropping them and producing wrong SQL.
func validateAttributes(kv map[string]string, known map[string]bool, directive, location string) error {
	for k := range kv {
		if known[k] || strings.HasPrefix(k, "platform.") {
			continue
		}
		slog.Error("unknown annotation attribute",
			"directive", directive,
			"attribute", k,
			"location", location,
		)
		return fmt.Errorf("unknown annotation attribute %q on %s at %s", k, directive, location)
	}
	return nil
}

func requireAttributes(kv map[string]string, required []string, directive, location string) error {
	for _, key := range required {
		if strings.TrimSpace(kv[key]) != "" {
			continue
		}
		slog.Error("missing required annotation attribute",
			"directive", directive,
			"attribute", key,
			"location", location,
		)
		return fmt.Errorf("missing required annotation attribute %q on %s at %s", key, directive, location)
	}
	return nil
}

func (s *schemaParseState) parseFieldComment(
	comment *ast.Comment,
	field *ast.Field,
	structName string,
) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)

	// Validate the directive itself, not each named carrier. For anonymous /
	// embedded fields field.Names is nil and the loop below would never run,
	// so doing this inside the loop would let unknown keys slip through.
	location := structName
	if len(field.Names) > 0 {
		location = structName + "." + field.Names[0].Name
	}
	if err := validateAttributes(kv, knownFieldAttributes, "//migrator:schema:field", location); err != nil {
		return err
	}

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
			s.globalEnumsMap[enumName] = Enum{
				Name:   enumName,
				Values: enum,
			}
			// Update the field type to use the generated enum name
			fieldType = enumName
		}

		identityGeneration := normalizeIdentityGeneration(kv["identity_generation"])
		if kv["identity_generation"] != "" && identityGeneration == "" {
			return fmt.Errorf("invalid identity_generation %q on //migrator:schema:field at %s", kv["identity_generation"], location)
		}
		if identityGeneration == "" && hasIdentitySettings(kv) {
			identityGeneration = "BY_DEFAULT"
		}
		_, defaultSet := kv["default"]
		s.schemaFields = append(s.schemaFields, Field{
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
	return nil
}

func hasIdentitySettings(kv map[string]string) bool {
	return kv["identity_start"] != "" || kv["identity_increment"] != "" || kv["identity_options"] != ""
}

func (s *schemaParseState) parseEmbeddedComment(comment *ast.Comment, field *ast.Field, structName string) {
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

	s.embeddedFields = append(s.embeddedFields, EmbeddedField{
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

func (s *schemaParseState) parseIndexComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(kv, knownIndexAttributes, "//migrator:schema:index", structName); err != nil {
		return err
	}

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
			return fmt.Errorf("invalid granularity %q on //migrator:schema:index at %s (must be a non-negative integer)", g, structName)
		}
		granularity = n
	}

	s.schemaIndexes = append(s.schemaIndexes, Index{
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
	return nil
}

// ParseConstraintComment parses a constraint comment and adds it to the constraints slice.
// This function is exported for testing purposes.
func ParseConstraintComment(comment *ast.Comment, structName string, schemaConstraints *[]Constraint) {
	*schemaConstraints = append(*schemaConstraints, parseConstraintComment(comment, structName))
}

func (s *schemaParseState) parseConstraintComment(comment *ast.Comment, structName string) {
	s.schemaConstraints = append(s.schemaConstraints, parseConstraintComment(comment, structName))
}

func parseConstraintComment(comment *ast.Comment, structName string) Constraint {
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

	return Constraint{
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
	}
}

func (s *schemaParseState) parseExtensionComment(comment *ast.Comment) {
	kv := parseutils.ParseKeyValueComment(comment.Text)

	s.extensions = append(s.extensions, Extension{
		Name:        kv["name"],
		IfNotExists: kv["if_not_exists"] == "true",
		Version:     kv["version"],
		Comment:     kv["comment"],
	})
}

func (s *schemaParseState) parseSchemaComment(comment *ast.Comment) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(kv, knownSchemaAttributes, "//migrator:schema:schema", kv["name"]); err != nil {
		return err
	}
	if err := requireAttributes(kv, []string{"name"}, "//migrator:schema:schema", kv["name"]); err != nil {
		return err
	}

	s.schemas = append(s.schemas, Schema{
		Name:    kv["name"],
		Comment: kv["comment"],
	})
	return nil
}

func (s *schemaParseState) parseTableComment(comment *ast.Comment, structName string) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	s.tableDirectives = append(s.tableDirectives, Table{
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

type schemaParseState struct {
	tableNameToStructName map[string]string
	globalEnumsMap        map[string]Enum
	embeddedFields        []EmbeddedField
	schemaFields          []Field
	schemaIndexes         []Index
	schemaConstraints     []Constraint
	tableDirectives       []Table
	extensions            []Extension
	functions             []Function
	views                 []View
	materializedViews     []MaterializedView
	triggers              []Trigger
	rlsPolicies           []RLSPolicy
	rlsEnabledTables      []RLSEnabledTable
	roles                 []Role
	grants                []Grant
	schemas               []Schema
}

func newSchemaParseState() *schemaParseState {
	return &schemaParseState{
		tableNameToStructName: make(map[string]string),
		globalEnumsMap:        make(map[string]Enum),
	}
}

func (s *schemaParseState) parseStructComment(comment *ast.Comment, structName string) error {
	if handled, err := s.parseStructScopedComment(comment, structName); handled || err != nil {
		return err
	}

	return s.parseSharedComment(comment, structName)
}

func (s *schemaParseState) parseStructFieldComment(comment *ast.Comment, structName string, field *ast.Field) error {
	if handled, err := s.parseFieldScopedComment(comment, structName, field); handled || err != nil {
		return err
	}

	return s.parseSharedComment(comment, structName)
}

func (s *schemaParseState) parseFieldScopedComment(comment *ast.Comment, structName string, field *ast.Field) (bool, error) {
	switch {
	case strings.HasPrefix(comment.Text, "//migrator:schema:field"):
		return true, s.parseFieldComment(comment, field, structName)
	case strings.HasPrefix(comment.Text, "//migrator:embedded"):
		s.parseEmbeddedComment(comment, field, structName)
		return true, nil
	case strings.HasPrefix(comment.Text, "//migrator:schema:index"):
		return true, s.parseIndexComment(comment, structName)
	default:
		return false, nil
	}
}

func (s *schemaParseState) parseStructScopedComment(comment *ast.Comment, structName string) (bool, error) {
	switch {
	case strings.HasPrefix(comment.Text, "//migrator:schema:table"):
		s.parseTableComment(comment, structName)
		return true, nil
	case strings.HasPrefix(comment.Text, "//migrator:schema:schema"):
		return true, s.parseSchemaComment(comment)
	default:
		return false, nil
	}
}

func (s *schemaParseState) parseSharedComment(comment *ast.Comment, structName string) error {
	switch {
	case strings.HasPrefix(comment.Text, "//migrator:schema:constraint"):
		s.parseConstraintComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:extension"):
		s.parseExtensionComment(comment)
	case strings.HasPrefix(comment.Text, "//migrator:schema:function"):
		s.parseFunctionComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:view"):
		return s.parseViewComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:matview"):
		return s.parseMaterializedViewComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:trigger"):
		return s.parseTriggerComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:rls:policy"):
		s.parseRLSPolicyComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:rls:enable"):
		s.parseRLSEnableComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:role"):
		s.parseRoleComment(comment, structName)
	case strings.HasPrefix(comment.Text, "//migrator:schema:grant"):
		s.parseGrantComment(comment, structName)
	}
	return nil
}

func (s *schemaParseState) processTableComments(structName string, genDecl *ast.GenDecl) error {
	if genDecl.Doc == nil {
		return nil
	}

	for _, comment := range genDecl.Doc.List {
		if err := s.parseStructComment(comment, structName); err != nil {
			return err
		}
	}
	return nil
}

func (s *schemaParseState) processFieldComments(structName string, structType *ast.StructType) error {
	for _, field := range structType.Fields.List {
		if field.Doc == nil {
			continue
		}
		for _, comment := range field.Doc.List {
			if err := s.parseStructFieldComment(comment, structName, field); err != nil {
				return err
			}
		}
	}
	return nil
}

func ParseFile(filename string) (Database, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		return Database{}, fmt.Errorf("parse Go file %q: %w", filename, err)
	}

	return parseFileAST(f)
}

// ParseSource parses a Go source string and returns the database schema.
// source can be a string, []byte, or io.Reader.
func ParseSource(filename string, source any) (Database, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, source, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		return Database{}, fmt.Errorf("parse Go source %q: %w", filename, err)
	}

	return parseFileAST(f)
}

func parseFileAST(f *ast.File) (Database, error) {
	state := newSchemaParseState()
	if err := state.processFileAST(f); err != nil {
		return Database{}, err
	}

	enums := make([]Enum, 0, len(state.globalEnumsMap))
	keys := make([]string, 0, len(state.globalEnumsMap))
	for k := range state.globalEnumsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		enums = append(enums, state.globalEnumsMap[k])
	}

	// Sort extensions alphabetically for consistent output
	sort.Slice(state.extensions, func(i, j int) bool {
		return state.extensions[i].Name < state.extensions[j].Name
	})

	result := Database{
		Schemas:           state.schemas,
		Tables:            state.tableDirectives,
		Fields:            state.schemaFields,
		Indexes:           state.schemaIndexes,
		Constraints:       state.schemaConstraints,
		Enums:             enums,
		EmbeddedFields:    state.embeddedFields,
		Extensions:        state.extensions,
		Functions:         state.functions,
		Views:             state.views,
		MaterializedViews: state.materializedViews,
		Triggers:          state.triggers,
		RLSPolicies:       state.rlsPolicies,
		RLSEnabledTables:  state.rlsEnabledTables,
		Roles:             state.roles,
		Grants:            state.grants,
		Dependencies:      make(map[string][]string),
	}
	normalizeTableScopedNames(&result)
	buildDependencyGraph(&result)
	return result, nil
}

// processFileAST processes the entire AST file in a single optimized pass
func (s *schemaParseState) processFileAST(f *ast.File) error {
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

			mapTableDirectiveStructNames(genDecl.Doc, structName, s.tableNameToStructName)
		}
	}

	// Process all struct declarations
	if err := s.processDeclarations(f); err != nil {
		return err
	}

	// Process all file comments for RLS annotations that might not be associated with struct declarations
	processAllFileComments(f, s.tableNameToStructName, &s.rlsPolicies, &s.rlsEnabledTables)
	return nil
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
func (s *schemaParseState) processDeclarations(f *ast.File) error {
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

			if err := s.processTableComments(structName, genDecl); err != nil {
				return err
			}
			if err := s.processFieldComments(structName, structType); err != nil {
				return err
			}
		}
	}
	return nil
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

func (s *schemaParseState) parseFunctionComment(comment *ast.Comment, structName string) {
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
	s.functions = append(s.functions, fn)
}

func (s *schemaParseState) parseViewComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(kv, knownViewAttributes, "//migrator:schema:view", structName); err != nil {
		return err
	}
	if err := requireAttributes(kv, []string{"name", "body"}, "//migrator:schema:view", structName); err != nil {
		return err
	}
	s.views = append(s.views, View{
		StructName: structName,
		Name:       kv["name"],
		Body:       kv["body"],
		WithCheck:  kv["with_check"] == "true",
		Comment:    kv["comment"],
	})
	return nil
}

func (s *schemaParseState) parseMaterializedViewComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(kv, knownMaterializedViewAttributes, "//migrator:schema:matview", structName); err != nil {
		return err
	}
	if err := requireAttributes(kv, []string{"name", "body"}, "//migrator:schema:matview", structName); err != nil {
		return err
	}
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
	s.materializedViews = append(s.materializedViews, matView)
	return nil
}

func (s *schemaParseState) parseTriggerComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(kv, knownTriggerAttributes, "//migrator:schema:trigger", structName); err != nil {
		return err
	}
	if err := requireAttributes(kv, []string{"name", "table", "timing", "event", "body"}, "//migrator:schema:trigger", structName); err != nil {
		return err
	}
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
	s.triggers = append(s.triggers, trigger)
	return nil
}

func (s *schemaParseState) parseRLSPolicyComment(comment *ast.Comment, structName string) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	s.rlsPolicies = append(s.rlsPolicies, RLSPolicy{
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

func (s *schemaParseState) parseRLSEnableComment(comment *ast.Comment, structName string) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	s.rlsEnabledTables = append(s.rlsEnabledTables, RLSEnabledTable{
		StructName: structName,
		Table:      kv["table"],
		Comment:    kv["comment"],
	})
}

func (s *schemaParseState) parseRoleComment(comment *ast.Comment, structName string) {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	s.roles = append(s.roles, Role{
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

func (s *schemaParseState) parseGrantComment(comment *ast.Comment, structName string) {
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
	s.grants = append(s.grants, grant)
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
// related files in the same directory to resolve embedded type references.
func ParseFileWithDependencies(filename string) (Database, error) {
	// Parse the main file
	database, err := ParseFile(filename)
	if err != nil {
		return Database{}, err
	}

	// Get the directory of the main file
	dir := filepath.Dir(filename)

	// Parse all other .go files in the same directory to find embedded type definitions
	pattern := filepath.Join(dir, "*.go")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return Database{}, fmt.Errorf("find related Go files for %q: %w", filename, err)
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
		dbmatch, err := ParseFile(match)
		if err != nil {
			return Database{}, fmt.Errorf("parse related Go file %q: %w", match, err)
		}
		relatedFields := dbmatch.Fields

		// Only add fields from embedded types that we actually need
		for _, field := range relatedFields {
			if embeddedTypeNames[field.StructName] {
				database.Fields = append(database.Fields, field)
			}
		}
	}

	buildDependencyGraph(&database)
	return database, nil
}
