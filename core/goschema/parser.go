package goschema

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	ptahast "go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema/internal/parseutils"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/annotationmeta"
	"go.5x5.cz/ptah/internal/chrefresh"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/internal/dialectscope"
	"go.5x5.cz/ptah/internal/tableref"
)

// annotationErrorContext locates one annotation in the source being parsed, so
// a diagnostic can name the file, line, and directive it came from.
type annotationErrorContext struct {
	file      string
	line      int
	directive string
	location  string
}

// validateAttributes rejects any key the directive does not recognize.
// Platform-specific overrides are accepted only for directives whose schema IR
// retains them. This catches typos and unsupported overrides at parse time
// instead of silently dropping them and producing wrong SQL.
func validateAttributes(kv map[string]string, ctx annotationErrorContext) error {
	directive := strings.TrimPrefix(ctx.directive, "//")
	for _, k := range slices.Sorted(maps.Keys(kv)) {
		// A retired attribute is checked before the unknown one, and sorted so
		// a directive carrying two of them names the same one every run. It is
		// still RECOGNIZED -- that is what put a bareword spelling in this map
		// at all -- so without this branch it would pass validation and be
		// dropped without a word (stokaro/ptah#1625).
		if reason, retired := annotationmeta.RetiredAttribute(directive, k); retired {
			slog.Error("retired annotation attribute",
				"directive", ctx.directive,
				"attribute", k,
				"location", ctx.location,
			)
			return &ptaherr.ParseError{
				File:      ctx.file,
				Line:      ctx.line,
				Directive: directive,
				Attribute: k,
				Err:       ptaherr.ErrRetiredAttribute,
				Message:   fmt.Sprintf("%s on %s at %s: %s", k, ctx.directive, ctx.location, reason),
			}
		}
		if annotationmeta.AllowsAttribute(directive, k) {
			continue
		}
		slog.Error("unknown annotation attribute",
			"directive", ctx.directive,
			"attribute", k,
			"location", ctx.location,
		)
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: k,
			Err:       ptaherr.ErrUnknownAttribute,
			Message:   fmt.Sprintf("unknown annotation attribute %q on %s at %s", k, ctx.directive, ctx.location),
		}
	}
	return nil
}

// parseDialectScope resolves the `dialects=` attribute of a directive that
// declares a standalone schema object.
//
// It is one helper rather than one expression per directive because a scope
// that is read differently in one place is a scope that omits an object from a
// target its author named. A value naming no supported dialect is a parse
// error: the alternative reading -- "belongs to nothing" -- turns a typo into
// an object silently missing from every target, with every command still
// exiting 0.
func parseDialectScope(kv map[string]string, ctx annotationErrorContext) ([]string, error) {
	raw, ok := kv[dialectscope.Attribute]
	if !ok {
		return nil, nil
	}
	scope, err := dialectscope.Parse(raw)
	if err == nil {
		return scope, nil
	}
	slog.Error("invalid annotation dialect scope",
		"directive", ctx.directive,
		"attribute", dialectscope.Attribute,
		"location", ctx.location,
	)
	return nil, &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: strings.TrimPrefix(ctx.directive, "//"),
		Attribute: dialectscope.Attribute,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message: fmt.Sprintf(
			"invalid %q value %q on %s at %s: %s",
			dialectscope.Attribute, raw, ctx.directive, ctx.location, err.Error(),
		),
	}
}

func requireAttributes(kv map[string]string, ctx annotationErrorContext) error {
	for _, key := range annotationmeta.RequiredAttributes(ctx.directive) {
		if strings.TrimSpace(kv[key]) != "" {
			continue
		}
		slog.Error("missing required annotation attribute",
			"directive", ctx.directive,
			"attribute", key,
			"location", ctx.location,
		)
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: key,
			Err:       ptaherr.ErrMissingRequiredAttribute,
			Message:   fmt.Sprintf("missing required annotation attribute %q on %s at %s", key, ctx.directive, ctx.location),
		}
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
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:field", location),
	); err != nil {
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
			return &ptaherr.ParseError{
				File:      s.filename,
				Line:      s.annotationContext(comment, "//ptah:schema:field", location).line,
				Directive: "ptah:schema:field",
				Attribute: "identity_generation",
				Err:       ptaherr.ErrInvalidAttributeValue,
				Message:   fmt.Sprintf("invalid identity_generation %q on //ptah:schema:field at %s", kv["identity_generation"], location),
			}
		}
		if identityGeneration == "" && hasIdentitySettings(kv) {
			identityGeneration = "BY_DEFAULT"
		}
		_, defaultSet := kv["default"]
		s.schemaFields = append(s.schemaFields, Field{
			StructName:          structName,
			FieldName:           name.Name,
			Name:                kv["name"],
			APIName:             kv["api_name"],
			APINames:            targetNames(kv),
			APIType:             kv["api_type"],
			APIExpose:           kv["api_expose"],
			Type:                fieldType,
			Nullable:            kv["not_null"] != "true",
			Primary:             kv["primary"] == "true",
			AutoInc:             kv["auto_increment"] == "true" || identityGeneration != "",
			IdentityGeneration:  identityGeneration,
			IdentityStart:       kv["identity_start"],
			IdentityIncrement:   kv["identity_increment"],
			IdentityOptions:     kv["identity_options"],
			Unique:              kv["unique"] == "true",
			UniqueExpr:          kv["unique_expr"],
			Default:             kv["default"],
			DefaultSet:          defaultSet,
			DefaultExpr:         kv["default_expr"],
			Foreign:             kv["foreign"],
			ForeignKeyName:      kv["foreign_key_name"],
			OnDelete:            kv["on_delete"],
			OnUpdate:            kv["on_update"],
			Enum:                enum,
			Check:               kv["check"],
			CheckName:           kv["check_name"],
			GeneratedExpression: kv["generated"],
			GeneratedKind:       generatedColumnKind(kv),
			Comment:             kv["comment"],
			Overrides:           parseutils.ParsePlatformSpecific(kv),
		})
	}
	return nil
}

func generatedColumnKind(kv map[string]string) string {
	if strings.TrimSpace(kv["generated"]) == "" {
		return ""
	}
	if kind := strings.TrimSpace(kv["generated_kind"]); kind != "" {
		return strings.ToUpper(kind)
	}
	if stored := strings.TrimSpace(kv["stored"]); stored != "" {
		if strings.EqualFold(stored, "true") {
			return "STORED"
		}
		return "VIRTUAL"
	}
	return ""
}

func hasIdentitySettings(kv map[string]string) bool {
	return kv["identity_start"] != "" || kv["identity_increment"] != "" || kv["identity_options"] != ""
}

func (s *schemaParseState) parseEmbeddedComment(comment *ast.Comment, field *ast.Field, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:embedded", structName),
	); err != nil {
		return err
	}

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
		Field:            kv["field"],
		Ref:              kv["ref"],
		OnDelete:         kv["on_delete"],
		OnUpdate:         kv["on_update"],
		Comment:          kv["comment"],
		EmbeddedTypeName: fieldTypeName,
		Overrides:        parseutils.ParsePlatformSpecific(kv),
	})
	return nil
}

func (s *schemaParseState) parseIndexComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:index", structName),
	); err != nil {
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
	var includeColumns []string
	if includeRaw, present := kv["include"]; present {
		parts := strings.Split(includeRaw, ",")
		includeColumns = make([]string, len(parts))
		for i, part := range parts {
			includeColumns[i] = strings.TrimSpace(part)
			if includeColumns[i] != "" {
				continue
			}
			ctx := s.annotationContext(comment, "//ptah:schema:index", structName)
			return &ptaherr.ParseError{
				File:      ctx.file,
				Line:      ctx.line,
				Directive: "ptah:schema:index",
				Attribute: "include",
				Err:       ptaherr.ErrInvalidAttributeValue,
				Message: fmt.Sprintf(
					"invalid include list for \"include\" on //ptah:schema:index at %s: column %d is empty; expected non-empty comma-separated column names",
					structName,
					i+1,
				),
			}
		}
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
			return &ptaherr.ParseError{
				File:      s.filename,
				Line:      s.annotationContext(comment, "//ptah:schema:index", structName).line,
				Directive: "ptah:schema:index",
				Attribute: "granularity",
				Err:       ptaherr.ErrInvalidAttributeValue,
				Message:   fmt.Sprintf("invalid granularity %q on //ptah:schema:index at %s (must be a non-negative integer)", g, structName),
			}
		}
		granularity = n
	}

	s.schemaIndexes = append(s.schemaIndexes, Index{
		StructName:     structName,
		Name:           kv["name"],
		Fields:         fields,
		Unique:         kv["unique"] == "true",
		Comment:        kv["comment"],
		Type:           kv["type"],                                  // PG: GIN/GIST/BTREE/HASH; CH: minmax/set(N)/bloom_filter/...
		Condition:      firstNonEmpty(kv["where"], kv["condition"]), // PG/SQLite partial and SQL Server filtered indexes: WHERE clause
		Operator:       kv["ops"],                                   // PG only: operator class (gin_trgm_ops, etc.)
		IncludeColumns: includeColumns,
		NullsDistinct:  parseBoolPtr(kv["nulls_distinct"]),
		TableName:      tableName,   // Target table name
		Granularity:    granularity, // CH only: GRANULARITY n for data-skipping indexes
	})
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (s *schemaParseState) parseConstraintComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:constraint", structName),
	); err != nil {
		return err
	}
	s.schemaConstraints = append(s.schemaConstraints, parseConstraintComment(comment, structName))
	return nil
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
	foreignColumns := splitCommaList(kv["foreign_columns"])
	if len(foreignColumns) == 0 && kv["foreign_column"] != "" {
		foreignColumns = []string{kv["foreign_column"]}
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
		Columns:        columns, // Column names
		IncludeColumns: splitCommaList(kv["include"]),
		NullsDistinct:  parseBoolPtr(kv["nulls_distinct"]),

		// FOREIGN KEY constraint specific fields
		ForeignTable:   kv["foreign_table"],  // Referenced table
		ForeignColumn:  kv["foreign_column"], // Referenced column
		ForeignColumns: foreignColumns,
		OnDelete:       kv["on_delete"], // ON DELETE action
		OnUpdate:       kv["on_update"], // ON UPDATE action

		Comment: kv["comment"], // Constraint comment
	}
}

func parseBoolPtr(value string) *bool {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return new(strings.EqualFold(value, "true"))
}

func (s *schemaParseState) parseExtensionComment(comment *ast.Comment) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:extension", kv["name"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	s.extensions = append(s.extensions, Extension{
		Name:        kv["name"],
		Schema:      kv["schema"],
		IfNotExists: kv["if_not_exists"] == "true",
		Version:     kv["version"],
		Comment:     kv["comment"],
		Dialects:    scope,
	})
	return nil
}

func (s *schemaParseState) parseSchemaComment(comment *ast.Comment) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:schema", kv["name"]),
	); err != nil {
		return err
	}
	if err := requireAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:schema", kv["name"]),
	); err != nil {
		return err
	}

	s.schemas = append(s.schemas, Schema{
		Name:    kv["name"],
		Comment: kv["comment"],
	})
	return nil
}

func (s *schemaParseState) parseTableComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	schemaName, tableName := tableDirectiveName(kv["schema"], kv["name"])
	// The TTL attributes are read BEFORE the general allowlist, and the order
	// is the point. Two ttl_ names are real CockroachDB parameters Ptah refuses
	// for measured reasons, and the allowlist would answer them with a generic
	// "unknown annotation attribute" — telling an author their spelling is
	// wrong when it is right and the parameter is unsupported. Running this
	// first means ttl_expire_after gets the reason and the alternative, and a
	// genuine ttl_ typo gets a message listing the managed surface, which is
	// also better than the generic one (stokaro/ptah#1027).
	rowTTL, err := crdbttl.FromAttributes(tableName, kv)
	if err != nil {
		return &ptaherr.ParseError{
			File:      s.filename,
			Line:      s.annotationContext(comment, "//ptah:schema:table", structName).line,
			Directive: "ptah:schema:table",
			Err:       ptaherr.ErrUnknownAttribute,
			Message:   err.Error(),
		}
	}
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:table", structName),
	); err != nil {
		return err
	}
	s.tableDirectives = append(s.tableDirectives, Table{
		StructName: structName,
		Name:       tableName,
		APIName:    kv["api_name"],
		APINames:   targetNames(kv),
		Schema:     schemaName,
		Engine:     kv["engine"],
		Comment:    kv["comment"],
		PrimaryKey: splitCSVAttribute(kv["primary_key"]),
		Checks:     splitCSVAttribute(kv["checks"]),
		CustomSQL:  kv["custom"],
		RowTTL:     rowTTL,
		Overrides:  parseutils.ParsePlatformSpecific(kv),
	})
	return nil
}

func tableDirectiveName(rawSchema, rawName string) (schemaName, tableName string) {
	schemaName = strings.TrimSpace(rawSchema)
	tableName = strings.TrimSpace(rawName)
	if schemaName != "" {
		if ref, ok := tableref.Parse(tableName); ok && !ref.Qualified {
			tableName = ref.Name
		}
		return schemaName, tableName
	}
	ref, ok := tableref.Parse(tableName)
	if !ok {
		return "", tableName
	}
	if !ref.Qualified {
		return "", ref.Name
	}
	return ref.Schema, ref.Name
}

func splitCSVAttribute(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			values = append(values, part)
		}
	}
	return values
}

type schemaParseState struct {
	filename              string
	fset                  *token.FileSet
	tableNameToStructName map[string]string
	globalEnumsMap        map[string]Enum
	embeddedFields        []EmbeddedField
	schemaFields          []Field
	schemaIndexes         []Index
	schemaConstraints     []Constraint
	tableDirectives       []Table
	extensions            []Extension
	functions             []Function
	sequences             []Sequence
	domains               []Domain
	compositeTypes        []CompositeType
	ranges                []Range
	views                 []View
	synonyms              []Synonym
	extendedProperties    []ExtendedProperty
	materializedViews     []MaterializedView
	triggers              []Trigger
	rlsPolicies           []RLSPolicy
	rlsEnabledTables      []RLSEnabledTable
	hypertables           []Hypertable
	continuousAggregates  []ContinuousAggregate
	roles                 []Role
	grants                []Grant
	managedData           []ManagedData
	schemas               []Schema
}

type structDeclaration struct {
	name       string
	genDecl    *ast.GenDecl
	structType *ast.StructType
}

type schemaCommentTarget struct {
	structName string
	field      *ast.Field
}

func newSchemaParseState(filename string, fset *token.FileSet) *schemaParseState {
	return &schemaParseState{
		filename:              filename,
		fset:                  fset,
		tableNameToStructName: make(map[string]string),
		globalEnumsMap:        make(map[string]Enum),
	}
}

func (s *schemaParseState) annotationContext(
	comment *ast.Comment,
	directive,
	location string,
) annotationErrorContext {
	ctx := annotationErrorContext{
		file:      s.filename,
		directive: directive,
		location:  location,
	}
	if s.fset != nil && comment != nil {
		ctx.line = s.fset.Position(comment.Slash).Line
	}
	return ctx
}

func (s *schemaParseState) parseStructComment(comment *ast.Comment, target schemaCommentTarget) error {
	return s.parseAttachedComment(comment, annotationmeta.ScopeStruct, target)
}

func (s *schemaParseState) parseStructFieldComment(comment *ast.Comment, target schemaCommentTarget) error {
	return s.parseAttachedComment(comment, annotationmeta.ScopeField, target)
}

func (s *schemaParseState) parseAttachedComment(
	comment *ast.Comment,
	scope annotationmeta.Scope,
	target schemaCommentTarget,
) error {
	directive, ok := annotationmeta.MatchCommentDirective(comment.Text)
	if !ok {
		return nil
	}
	if !annotationmeta.AllowsScope(directive, scope) {
		return nil
	}
	if handled, err := s.parsePlacementDirective(comment, directive.Name, target); handled || err != nil {
		return err
	}
	return s.parseSharedDirective(comment, directive.Name, target)
}

func (s *schemaParseState) parsePlacementDirective(
	comment *ast.Comment,
	directive string,
	target schemaCommentTarget,
) (bool, error) {
	switch directive {
	case "ptah:schema:field":
		return true, s.parseFieldComment(comment, target.field, target.structName)
	case "ptah:embedded":
		return true, s.parseEmbeddedComment(comment, target.field, target.structName)
	case "ptah:schema:index":
		return true, s.parseIndexComment(comment, target.structName)
	case "ptah:schema:table":
		return true, s.parseTableComment(comment, target.structName)
	case "ptah:schema:schema":
		return true, s.parseSchemaComment(comment)
	default:
		return false, nil
	}
}

// sharedDirectiveParser reads one schema directive. Every parser takes the
// struct the comment was attached to, and the two that have no use for it are
// adapted by [ignoringStruct] rather than given a signature of their own.
type sharedDirectiveParser func(*schemaParseState, *ast.Comment, string) error

// sharedDirectiveParsers is the directive dispatch every schema comment target
// shares: a struct's doc comment and a field's carry the same set.
//
// It is a table rather than a run of switch arms because the switch reached
// twenty-one of them, all identical in shape. A table says which directive
// belongs to which parser and nothing else, and a new object family adds one
// line instead of pushing the function past the complexity gate.
var sharedDirectiveParsers = map[string]sharedDirectiveParser{
	"ptah:schema:constraint":          (*schemaParseState).parseConstraintComment,
	"ptah:schema:enum":                ignoringStruct((*schemaParseState).parseEnumComment),
	"ptah:schema:extension":           ignoringStruct((*schemaParseState).parseExtensionComment),
	"ptah:schema:function":            (*schemaParseState).parseFunctionComment,
	"ptah:schema:procedure":           (*schemaParseState).parseProcedureComment,
	"ptah:schema:sequence":            (*schemaParseState).parseSequenceComment,
	"ptah:schema:domain":              (*schemaParseState).parseDomainComment,
	"ptah:schema:composite":           (*schemaParseState).parseCompositeComment,
	"ptah:schema:range":               (*schemaParseState).parseRangeComment,
	"ptah:schema:view":                (*schemaParseState).parseViewComment,
	"ptah:schema:matview":             (*schemaParseState).parseMaterializedViewComment,
	"ptah:schema:hypertable":          (*schemaParseState).parseHypertableComment,
	"ptah:schema:continuousaggregate": (*schemaParseState).parseContinuousAggregateComment,
	"ptah:schema:synonym":             (*schemaParseState).parseSynonymComment,
	"ptah:schema:extendedproperty":    (*schemaParseState).parseExtendedPropertyComment,
	"ptah:schema:trigger":             (*schemaParseState).parseTriggerComment,
	"ptah:schema:rls:policy":          (*schemaParseState).parseRLSPolicyComment,
	"ptah:schema:rls:enable":          (*schemaParseState).parseRLSEnableComment,
	"ptah:schema:role":                (*schemaParseState).parseRoleComment,
	"ptah:schema:grant":               (*schemaParseState).parseGrantComment,
	"ptah:schema:data":                (*schemaParseState).parseManagedDataComment,
}

// ignoringStruct adapts a parser that does not need the owning struct's name.
func ignoringStruct(
	parse func(*schemaParseState, *ast.Comment) error,
) sharedDirectiveParser {
	return func(s *schemaParseState, comment *ast.Comment, _ string) error {
		return parse(s, comment)
	}
}

func (s *schemaParseState) parseSharedDirective(
	comment *ast.Comment,
	directive string,
	target schemaCommentTarget,
) error {
	parse, known := sharedDirectiveParsers[directive]
	if !known {
		return nil
	}
	return parse(s, comment, target.structName)
}

func (s *schemaParseState) parseEnumComment(comment *ast.Comment) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:enum", kv["name"]),
	); err != nil {
		return err
	}
	if err := requireAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:enum", kv["name"]),
	); err != nil {
		return err
	}

	s.globalEnumsMap[kv["name"]] = Enum{
		Name:   kv["name"],
		Values: splitCommaList(kv["values"]),
	}
	return nil
}

func (s *schemaParseState) processStructComments(structDecl structDeclaration) error {
	if structDecl.genDecl.Doc == nil {
		return nil
	}

	target := schemaCommentTarget{structName: structDecl.name}
	for _, comment := range structDecl.genDecl.Doc.List {
		if err := s.parseStructComment(comment, target); err != nil {
			return err
		}
	}
	return nil
}

func (s *schemaParseState) processFieldComments(structDecl structDeclaration) error {
	for _, field := range structDecl.structType.Fields.List {
		if field.Doc == nil {
			continue
		}
		target := schemaCommentTarget{
			structName: structDecl.name,
			field:      field,
		}
		for _, comment := range field.Doc.List {
			if err := s.parseStructFieldComment(comment, target); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParseFile parses one annotated Go file and returns the schema it declares.
//
// It is the single-file member of the parse family: [ParseSource] takes the
// same file as bytes a caller already holds, and [ParseDir], [ParseDirs] and
// [ParseFS] walk a tree. A file naming another file's struct declares a schema
// that is incomplete on purpose -- the join is by struct name -- so a caller
// reading one file of a package gets that package's fields and no others.
//
// A file that does not parse as Go returns a [ptaherr.ParseError] naming it.
func ParseFile(filename string) (Database, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		return Database{}, &ptaherr.ParseError{
			File:    filename,
			Err:     err,
			Message: fmt.Sprintf("parse Go file %q: %v", filename, err),
		}
	}

	return parseFileAST(filename, fset, f)
}

// ParseSource parses a Go source string and returns the database schema.
// source can be a string, []byte, or io.Reader.
func ParseSource(filename string, source any) (Database, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, source, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		return Database{}, &ptaherr.ParseError{
			File:    filename,
			Err:     err,
			Message: fmt.Sprintf("parse Go source %q: %v", filename, err),
		}
	}

	return parseFileAST(filename, fset, f)
}

func parseFileAST(filename string, fset *token.FileSet, f *ast.File) (Database, error) {
	state := newSchemaParseState(filename, fset)
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
		Schemas:              state.schemas,
		Tables:               state.tableDirectives,
		Fields:               state.schemaFields,
		Indexes:              state.schemaIndexes,
		Constraints:          state.schemaConstraints,
		Enums:                enums,
		EmbeddedFields:       state.embeddedFields,
		Extensions:           state.extensions,
		Functions:            state.functions,
		Sequences:            state.sequences,
		Domains:              state.domains,
		CompositeTypes:       state.compositeTypes,
		Ranges:               state.ranges,
		Views:                state.views,
		Synonyms:             state.synonyms,
		ExtendedProperties:   state.extendedProperties,
		MaterializedViews:    state.materializedViews,
		Triggers:             state.triggers,
		RLSPolicies:          state.rlsPolicies,
		RLSEnabledTables:     state.rlsEnabledTables,
		Hypertables:          state.hypertables,
		ContinuousAggregates: state.continuousAggregates,
		Roles:                state.roles,
		Grants:               state.grants,
		ManagedData:          state.managedData,
		Dependencies:         make(map[string][]string),
	}
	normalizeTableScopedNames(&result)
	buildDependencyGraph(&result)
	return result, nil
}

// processFileAST processes the entire AST file.
func (s *schemaParseState) processFileAST(f *ast.File) error {
	structDecls := collectStructDeclarations(f)
	s.mapTableDirectiveStructNames(structDecls)

	// Process all struct declarations
	if err := s.processDeclarations(structDecls); err != nil {
		return err
	}

	// Process all file comments for RLS annotations that might not be associated with struct declarations
	return s.processAllFileComments(f)
}

func collectStructDeclarations(f *ast.File) []structDeclaration {
	var structDecls []structDeclaration
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
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			structDecls = append(structDecls, structDeclaration{
				name:       typeSpec.Name.Name,
				genDecl:    genDecl,
				structType: structType,
			})
		}
	}
	return structDecls
}

func (s *schemaParseState) mapTableDirectiveStructNames(structDecls []structDeclaration) {
	for _, structDecl := range structDecls {
		if structDecl.genDecl.Doc == nil {
			continue
		}
		for _, comment := range structDecl.genDecl.Doc.List {
			s.mapTableDirectiveStructName(comment, structDecl.name)
		}
	}
}

func (s *schemaParseState) mapTableDirectiveStructName(comment *ast.Comment, structName string) {
	directive, ok := annotationmeta.MatchCommentDirective(comment.Text)
	if !ok || directive.Name != "ptah:schema:table" {
		return
	}
	kv := parseutils.ParseKeyValueComment(comment.Text)
	tableName := kv["name"]
	if tableName == "" {
		return
	}
	s.tableNameToStructName[tableName] = structName
	if schemaName := kv["schema"]; schemaName != "" {
		s.tableNameToStructName[QualifyTableName(schemaName, tableName)] = structName
	}
}

// processDeclarations processes all struct declarations in the file.
func (s *schemaParseState) processDeclarations(structDecls []structDeclaration) error {
	for _, structDecl := range structDecls {
		if err := s.processDeclaration(structDecl); err != nil {
			return err
		}
	}
	return nil
}

func (s *schemaParseState) processDeclaration(structDecl structDeclaration) error {
	if err := s.processStructComments(structDecl); err != nil {
		return err
	}
	return s.processFieldComments(structDecl)
}

// processAllFileComments scans comments for RLS annotations that are separated
// from struct declarations by blank lines.
func (s *schemaParseState) processAllFileComments(f *ast.File) error {
	seen := s.newRLSCommentSet()
	placements := annotationmeta.CommentPlacements(f)

	for _, commentGroup := range f.Comments {
		for _, comment := range commentGroup.List {
			if placements[comment].Scope != annotationmeta.ScopeFile {
				continue
			}
			if err := s.parseFileScopedRLSComment(comment, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

// rlsPolicyKey identifies an RLS policy the way PostgreSQL does: by the table
// that owns it together with its name. A policy name is scoped to its table,
// not to the schema, so two tables may each carry one called
// "tenant_isolation". Recording only the name here dropped the second of the
// two before anything downstream could see it (stokaro/ptah#1276).
type rlsPolicyKey struct {
	table  string
	policy string
}

type rlsCommentSet struct {
	policies      map[rlsPolicyKey]struct{}
	enabledTables map[string]struct{}
}

func (s *schemaParseState) newRLSCommentSet() rlsCommentSet {
	seen := rlsCommentSet{
		policies:      make(map[rlsPolicyKey]struct{}, len(s.rlsPolicies)),
		enabledTables: make(map[string]struct{}, len(s.rlsEnabledTables)),
	}

	for _, policy := range s.rlsPolicies {
		seen.policies[rlsPolicyKey{table: policy.Table, policy: policy.Name}] = struct{}{}
	}
	for _, table := range s.rlsEnabledTables {
		seen.enabledTables[table.Table] = struct{}{}
	}

	return seen
}

func (s *schemaParseState) parseFileScopedRLSComment(comment *ast.Comment, seen rlsCommentSet) error {
	directive, ok := annotationmeta.MatchCommentDirective(comment.Text)
	if !ok {
		return nil
	}
	switch directive.Name {
	case "ptah:schema:rls:policy":
		return s.parseFileScopedRLSPolicyComment(comment, seen)
	case "ptah:schema:rls:enable":
		return s.parseFileScopedRLSEnableComment(comment, seen)
	}
	return nil
}

func (s *schemaParseState) parseFileScopedRLSPolicyComment(comment *ast.Comment, seen rlsCommentSet) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:rls:policy", kv["table"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	policyName := kv["name"]
	tableName := kv["table"]
	if policyName == "" || tableName == "" {
		return nil
	}
	key := rlsPolicyKey{table: tableName, policy: policyName}
	if _, exists := seen.policies[key]; exists {
		return nil
	}

	structName, exists := s.tableNameToStructName[tableName]
	if !exists {
		return nil
	}

	s.rlsPolicies = append(s.rlsPolicies, RLSPolicy{
		StructName:          structName,
		Name:                policyName,
		Table:               tableName,
		PolicyFor:           kv["for"],
		ToRoles:             kv["to"],
		UsingExpression:     kv["using"],
		WithCheckExpression: kv["with_check"],
		Comment:             kv["comment"],
		Dialects:            scope,
	})
	seen.policies[key] = struct{}{}
	return nil
}

func (s *schemaParseState) parseFileScopedRLSEnableComment(comment *ast.Comment, seen rlsCommentSet) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:rls:enable", kv["table"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	tableName := kv["table"]
	if tableName == "" {
		return nil
	}
	if _, exists := seen.enabledTables[tableName]; exists {
		return nil
	}

	structName, exists := s.tableNameToStructName[tableName]
	if !exists {
		return nil
	}

	s.rlsEnabledTables = append(s.rlsEnabledTables, RLSEnabledTable{
		StructName: structName,
		Table:      tableName,
		Comment:    kv["comment"],
		Dialects:   scope,
	})
	seen.enabledTables[tableName] = struct{}{}
	return nil
}

// parseProcedureComment parses a //ptah:schema:procedure annotation.
//
// A procedure is the same catalog object as a function with one property
// removed, so it reuses the function parser and sets the kind rather than
// carrying a second model. `returns` is refused rather than ignored: a
// procedure that named one would be a declaration the server cannot take, and
// dropping the attribute silently is the failure this issue is about
// (stokaro/ptah#1722).
func (s *schemaParseState) parseProcedureComment(comment *ast.Comment, structName string) error {
	// The attributes are validated against the procedure's own directive, so
	// `returns=` is refused by name rather than accepted and then rejected
	// below -- the registry is where an operator reads what a directive takes.
	kv := parseutils.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(kv, s.annotationContext(comment, "//ptah:schema:procedure", structName)); err != nil {
		return err
	}

	before := len(s.functions)
	if err := s.parseFunctionComment(comment, structName); err != nil {
		return err
	}
	if len(s.functions) == before {
		return nil
	}
	routine := &s.functions[len(s.functions)-1]
	if routine.Returns != "" {
		ctx := s.annotationContext(comment, "//ptah:schema:procedure", structName)
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "returns",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf(
				"procedure %q declares returns=%q at %s; a procedure returns nothing and is invoked with CALL",
				routine.Name, routine.Returns, ctx.location),
		}
	}
	routine.Kind = FunctionKindProcedure
	return nil
}

func (s *schemaParseState) parseFunctionComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:function", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

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
		Dialects:   scope,
	}
	// Canonicalize so every downstream consumer (planner, renderer,
	// comparator) sees the same values regardless of how the annotation was
	// typed. See Function.Canonicalize for the per-field rules.
	fn.Canonicalize()
	s.functions = append(s.functions, fn)
	return nil
}

func (s *schemaParseState) parseSequenceComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:sequence", kv["name"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	seq := Sequence{
		StructName:  structName,
		Name:        kv["name"],
		Schema:      kv["schema"],
		AsType:      kv["as"],
		Cycle:       kv["cycle"] == "true",
		OwnedBy:     kv["owned_by"],
		IfNotExists: kv["if_not_exists"] == "true",
		Comment:     kv["comment"],
		Dialects:    scope,
	}

	for _, opt := range []struct {
		key    string
		target **int64
	}{
		{"start", &seq.Start},
		{"increment", &seq.Increment},
		{"minvalue", &seq.MinValue},
		{"maxvalue", &seq.MaxValue},
		{"cache", &seq.Cache},
	} {
		value, err := parseOptionalInt64(kv[opt.key])
		if err != nil {
			return &ptaherr.ParseError{
				File:      ctx.file,
				Line:      ctx.line,
				Directive: strings.TrimPrefix(ctx.directive, "//"),
				Attribute: opt.key,
				Err:       ptaherr.ErrInvalidAttributeValue,
				Message:   fmt.Sprintf("invalid integer value %q for %q on %s at %s", kv[opt.key], opt.key, ctx.directive, ctx.location),
			}
		}
		*opt.target = value
	}

	seq.Canonicalize()
	if !IsValidSequenceType(seq.AsType) {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "as",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message:   fmt.Sprintf("invalid sequence type %q for \"as\" on %s at %s; expected smallint, integer, or bigint", kv["as"], ctx.directive, ctx.location),
		}
	}
	s.sequences = append(s.sequences, seq)
	return nil
}

// parseOptionalInt64 parses a decimal integer attribute value. An empty string
// yields a nil pointer (attribute absent), so callers can distinguish "not set"
// from an explicit zero.
func parseOptionalInt64(value string) (*int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil //nolint:nilnil // nil pointer + nil error means "attribute absent"
	}
	n, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func (s *schemaParseState) parseDomainComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:domain", kv["name"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}

	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	domain := Domain{
		StructName:  structName,
		Name:        kv["name"],
		Schema:      kv["schema"],
		BaseType:    kv["type"],
		NotNull:     kv["not_null"] == "true",
		Default:     kv["default"],
		DefaultExpr: kv["default_expr"],
		Check:       kv["check"],
		Comment:     kv["comment"],
		Dialects:    scope,
	}
	domain.Canonicalize()
	s.domains = append(s.domains, domain)
	return nil
}

func (s *schemaParseState) parseCompositeComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:composite", kv["name"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}

	fields, err := parseCompositeFields(kv["fields"])
	if err != nil {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "fields",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message:   fmt.Sprintf("%v for \"fields\" on %s at %s; expected \"name:type,name:type\"", err, ctx.directive, ctx.location),
		}
	}

	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	composite := CompositeType{
		StructName: structName,
		Name:       kv["name"],
		Schema:     kv["schema"],
		Fields:     fields,
		Comment:    kv["comment"],
		Dialects:   scope,
	}
	composite.Canonicalize()
	s.compositeTypes = append(s.compositeTypes, composite)
	return nil
}

// parseCompositeFields parses a "name:type,name:type" list into ordered fields.
// Splitting is paren-aware so a parameterized type (e.g. NUMERIC(10,2)) whose
// own comma would otherwise be read as a field separator survives intact.
func parseCompositeFields(value string) ([]CompositeTypeField, error) {
	var fields []CompositeTypeField
	for _, part := range splitTopLevelCommaList(value) {
		name, typ, ok := strings.Cut(part, ":")
		name = strings.TrimSpace(name)
		typ = strings.TrimSpace(typ)
		if !ok || name == "" || typ == "" {
			return nil, fmt.Errorf("invalid composite field %q", part)
		}
		fields = append(fields, CompositeTypeField{Name: name, Type: typ})
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("at least one field is required")
	}
	return fields, nil
}

// splitTopLevelCommaList splits on commas that are not nested inside parentheses,
// trimming each entry and dropping empties.
func splitTopLevelCommaList(value string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, r := range value {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				if trimmed := strings.TrimSpace(value[start:i]); trimmed != "" {
					parts = append(parts, trimmed)
				}
				start = i + 1
			}
		}
	}
	if trimmed := strings.TrimSpace(value[start:]); trimmed != "" {
		parts = append(parts, trimmed)
	}
	return parts
}

func (s *schemaParseState) parseRangeComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:range", kv["name"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}

	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	rangeType := Range{
		StructName:     structName,
		Name:           kv["name"],
		Schema:         kv["schema"],
		Subtype:        kv["subtype"],
		SubtypeOpClass: kv["subtype_opclass"],
		Collation:      kv["collation"],
		Canonical:      kv["canonical"],
		SubtypeDiff:    kv["subtype_diff"],
		Comment:        kv["comment"],
		Dialects:       scope,

		ClearedAttributes: clearedAttributes(kv, rangeClearableAttributes),
	}
	rangeType.Canonicalize()
	s.ranges = append(s.ranges, rangeType)
	return nil
}

// rangeClearableAttributes are the range attributes a declaration may write
// empty to say the type has none of them.
//
// The subtype is not one: a range without one is not a range, and an empty one
// is a declaration the server refuses rather than a request to remove anything.
var rangeClearableAttributes = []string{"subtype_opclass", "collation", "canonical", "subtype_diff"}

// clearedAttributes lists the given attributes the annotation wrote with an
// empty value, in the order they are listed rather than the map's.
//
// An omitted attribute and one written `key=""` reach the same empty string in
// the parsed struct, and only the key's presence separates them: omission says
// nothing about the attribute, while an empty value says the object has none
// (stokaro/ptah#2223).
func clearedAttributes(kv map[string]string, clearable []string) []string {
	var cleared []string
	for _, attribute := range clearable {
		value, present := kv[attribute]
		if present && strings.TrimSpace(value) == "" {
			cleared = append(cleared, attribute)
		}
	}
	return cleared
}

func (s *schemaParseState) parseViewComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:view", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	s.views = append(s.views, View{
		StructName: structName,
		Name:       kv["name"],
		Body:       kv["body"],
		WithCheck:  kv["with_check"] == "true",
		Comment:    kv["comment"],
		Dialects:   scope,
	})
	return nil
}

// parseHypertableComment reads a TimescaleDB hypertable declaration.
//
// There is no dialect scope here, for the reason [schemaParseState.parseSynonymComment]
// gives: a hypertable belongs to TimescaleDB and to nothing else.
//
// `chunk_interval` is kept as the string it was written as. The catalog reports
// the server's own spelling -- `7 days`, `1 day` -- and a declaration converted
// to something else to compare would differ from it on every run.
func (s *schemaParseState) parseHypertableComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:hypertable", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	s.hypertables = append(s.hypertables, Hypertable{
		StructName:    structName,
		Table:         kv["table"],
		Column:        kv["column"],
		ChunkInterval: kv["chunk_interval"],
		IfNotExists:   kv["if_not_exists"] == "true",
		Comment:       kv["comment"],
	})
	return nil
}

// parseContinuousAggregateComment reads a TimescaleDB continuous aggregate
// declaration.
//
// The body is kept as it was written. The catalog stores a rewritten SELECT,
// and the comparison puts the declaration through the same rewrite rather than
// folding either text -- so a declaration normalized here would be normalized
// twice and match nothing.
//
// There is no dialect scope here, for the reason
// [schemaParseState.parseSynonymComment] gives.
func (s *schemaParseState) parseContinuousAggregateComment(
	comment *ast.Comment,
	structName string,
) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:continuousaggregate", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	s.continuousAggregates = append(s.continuousAggregates, ContinuousAggregate{
		StructName:       structName,
		Name:             kv["name"],
		Schema:           kv["schema"],
		Body:             kv["body"],
		MaterializedOnly: optionalBoolAttribute(kv, "materialized_only"),
		Comment:          kv["comment"],
	})
	return nil
}

// optionalBoolAttribute answers nil for an attribute the annotation did not
// write, so a caller can tell "unset" from "set to false".
func optionalBoolAttribute(kv map[string]string, name string) *bool {
	written, present := kv[name]
	if !present {
		return nil
	}
	value := written == "true"
	return &value
}

// parseSynonymComment reads a SQL Server synonym declaration.
//
// There is no dialect scope here, and the omission is deliberate: a synonym is
// a SQL Server object and nothing else, so a scope attribute would let a schema
// claim it belongs to a target that has no such construct.
func (s *schemaParseState) parseSynonymComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:synonym", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	s.synonyms = append(s.synonyms, Synonym{
		StructName: structName,
		Name:       kv["name"],
		Schema:     kv["schema"],
		Target:     kv["target"],
		Comment:    kv["comment"],
	})
	return nil
}

// parseExtendedPropertyComment reads a SQL Server extended-property
// declaration.
//
// Three refusals, and they are all about identity.
//
// A table without a schema and a column without a table are refused because
// SQL Server has no level N without a level N-1: the statement that writes
// such a property cannot be composed, and accepting the declaration would
// defer the failure to a plan that has already been reviewed. Omitting ALL of
// them is not an omission -- it is the database's own property, which is the
// one address that names no object.
//
// MS_Description is refused because Ptah models it as the object's comment.
// Accepting it here would give the same live row two owners -- the comment
// comparator and the property comparator -- each planning against it without
// seeing the other, so an inspect-then-apply round trip would emit both a
// comment change and a property change for one value.
//
// There is no dialect scope here, and the omission is deliberate, exactly as
// on a synonym: an extended property is a SQL Server object and nothing else.
func (s *schemaParseState) parseExtendedPropertyComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:extendedproperty", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	if strings.TrimSpace(kv["table"]) != "" && strings.TrimSpace(kv["schema"]) == "" {
		return fmt.Errorf(
			"%s: extended property %q names a table and no schema; SQL Server addresses a table "+
				"through the schema that holds it, so there is no property to write",
			ctx.location, kv["name"])
	}
	if strings.TrimSpace(kv["column"]) != "" && strings.TrimSpace(kv["table"]) == "" {
		return fmt.Errorf(
			"%s: extended property %q names a column and no table; SQL Server addresses a column "+
				"through the table that holds it, so there is no property to write",
			ctx.location, kv["name"])
	}
	if strings.EqualFold(strings.TrimSpace(kv["name"]), "MS_Description") {
		return fmt.Errorf(
			"%s: extended property MS_Description is the object comment, which Ptah already manages; "+
				"declare it with the comment attribute of the object it belongs to",
			ctx.location)
	}
	s.extendedProperties = append(s.extendedProperties, ExtendedProperty{
		StructName: structName,
		Name:       kv["name"],
		Schema:     kv["schema"],
		Table:      kv["table"],
		Column:     kv["column"],
		Value:      kv["value"],
		Comment:    kv["comment"],
	})
	return nil
}

func (s *schemaParseState) parseMaterializedViewComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:matview", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	// No refresh strategy is read. validateAttributes has already refused the
	// retired attribute by name, so a declaration carrying one never reaches
	// this line (stokaro/ptah#1625). What IS read is the ClickHouse schedule,
	// which is the opposite kind of thing: engine-native state the server owns
	// (stokaro/ptah#1802).
	refresh, err := parseMatViewRefresh(kv, ctx)
	if err != nil {
		return err
	}
	s.materializedViews = append(s.materializedViews, MaterializedView{
		StructName: structName,
		Name:       kv["name"],
		Body:       kv["body"],
		Comment:    kv["comment"],
		Dialects:   scope,
		Refresh:    refresh,
	})
	return nil
}

// parseMatViewRefresh reads the ClickHouse refresh schedule a materialized view
// declares, and returns nil for one declaring none.
//
// The attribute carries the clause as ClickHouse spells it -- `every 1 hour`,
// `after 30 minute offset 5 minute` -- rather than a set of sub-attributes, so
// an operator moving a schedule out of a CREATE statement moves the text. It is
// canonicalized here, before it reaches the model, so every later layer sees the
// spelling the server would have stored and a comparison against the catalog
// starts from the same place.
func parseMatViewRefresh(
	kv map[string]string,
	ctx annotationErrorContext,
) (*ptahast.MatViewRefreshSpec, error) {
	declared := strings.TrimSpace(kv["refresh"])
	if declared == "" {
		return nil, nil
	}
	spec := chrefresh.ParseClause(declared)
	if spec == nil {
		return nil, matViewRefreshError(ctx, fmt.Errorf(
			"refresh %q is not a ClickHouse refresh clause; expected EVERY or AFTER "+
				"followed by an interval", declared))
	}
	canonical, err := chrefresh.Canonical(spec, "")
	if err != nil {
		return nil, matViewRefreshError(ctx, err)
	}
	return canonical, nil
}

// matViewRefreshError reports a refresh declaration the parser refused, in the
// shape every other attribute refusal in this file takes.
func matViewRefreshError(ctx annotationErrorContext, err error) error {
	slog.Error("invalid refresh schedule",
		"directive", ctx.directive,
		"location", ctx.location,
		"error", err,
	)
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: strings.TrimPrefix(ctx.directive, "//"),
		Attribute: "refresh",
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("refresh on %s at %s: %s", ctx.directive, ctx.location, err),
	}
}

func (s *schemaParseState) parseTriggerComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:trigger", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
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
		Dialects:   scope,
	}
	trigger.Canonicalize()
	s.triggers = append(s.triggers, trigger)
	return nil
}

func (s *schemaParseState) parseRLSPolicyComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:rls:policy", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	s.rlsPolicies = append(s.rlsPolicies, RLSPolicy{
		StructName:          structName,
		Name:                kv["name"],
		Table:               kv["table"],
		PolicyFor:           kv["for"],
		ToRoles:             kv["to"],
		UsingExpression:     kv["using"],
		WithCheckExpression: kv["with_check"],
		Comment:             kv["comment"],
		Dialects:            scope,
	})
	return nil
}

func (s *schemaParseState) parseRLSEnableComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:rls:enable", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	s.rlsEnabledTables = append(s.rlsEnabledTables, RLSEnabledTable{
		StructName: structName,
		Table:      kv["table"],
		Comment:    kv["comment"],
		Dialects:   scope,
	})
	return nil
}

func (s *schemaParseState) parseRoleComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:role", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
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
		Dialects:    scope,
	})
	return nil
}

func (s *schemaParseState) parseGrantComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:grant", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
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
		OnSequence: kv["on_sequence"],
		WithOption: kv["with_option"] == "true" || kv["grant_option"] == "true",
		Comment:    kv["comment"],
		Dialects:   scope,
	}
	grant.Canonicalize()
	s.grants = append(s.grants, grant)
	return nil
}

func (s *schemaParseState) parseManagedDataComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:data", kv["table"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}

	keys := splitCommaList(kv["key"])
	if len(keys) == 0 {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "key",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message:   fmt.Sprintf("empty key list for \"key\" on %s at %s; expected one or more comma-separated key columns", ctx.directive, ctx.location),
		}
	}

	s.managedData = append(s.managedData, ManagedData{
		StructName: structName,
		Table:      kv["table"],
		Schema:     kv["schema"],
		Keys:       keys,
		File:       kv["file"],
		SourceDir:  filepath.Dir(s.filename),
	})
	return nil
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

// targetNames reads the per-target name attributes. They share one shape on the
// field and the table directives, so they are read in one place: a target that
// gained an attribute on only one of the two would be a silent asymmetry.
func targetNames(kv map[string]string) TargetNames {
	return TargetNames{
		OpenAPI:  kv["openapi_name"],
		GraphQL:  kv["graphql_name"],
		Protobuf: kv["proto_name"],
	}
}
