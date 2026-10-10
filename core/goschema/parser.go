package goschema

import (
	"errors"
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

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/routinesetting"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbsource"
)

// annotationErrorContext locates one annotation in the source being parsed, so
// a diagnostic can name the file, line, and directive it came from.
type annotationErrorContext struct {
	file      string
	line      int
	directive string
	location  string
	// catalog is the directives the parse knows, the selected owners' among
	// them, against which the attributes are validated.
	catalog annotationmeta.Catalog
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
		if reason, retired := ctx.catalog.RetiredAttribute(directive, k); retired {
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
		if ctx.catalog.AllowsAttribute(directive, k) {
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
	for _, key := range ctx.catalog.RequiredAttributes(ctx.directive) {
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
	kv := s.kv.ParseKeyValueComment(comment.Text)

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
			s.globalEnumsMap[enumName] = schemamodel.Enum{
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
		s.schemaFields = append(s.schemaFields, schemamodel.Field{
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	s.embeddedFields = append(s.embeddedFields, schemamodel.EmbeddedField{
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	keyBlockSize, err := s.unsignedAttribute(kv, comment, structName, "index", "key_block_size")
	if err != nil {
		return err
	}
	facets, err := s.indexPartitioning(kv, comment, structName)
	if err != nil {
		return err
	}
	vector, err := s.indexVector(kv, comment, structName)
	if err != nil {
		return err
	}
	if vector != nil {
		if facets, err = facets.With(vector); err != nil {
			return err
		}
	}
	fullText, err := ydbindex.ParseOptionsDeclaration(kv)
	if err != nil {
		return fmt.Errorf("index %q at %s: %w", kv["name"], structName, err)
	}
	s.schemaIndexes = append(s.schemaIndexes, schemamodel.Index{
		Facets:         facets,
		StructName:     structName,
		Name:           kv["name"],
		Fields:         fields,
		Unique:         kv["unique"] == "true",
		Comment:        kv["comment"],
		Overrides:      parseutils.ParsePlatformProperties(kv),
		Invisible:      kv["invisible"] == "true",
		KeyBlockSize:   keyBlockSize,
		Type:           kv["type"],                                  // PG: GIN/GIST/BTREE/HASH; the ClickHouse owner consumes it as the skipping-index type
		Condition:      firstNonEmpty(kv["where"], kv["condition"]), // PG/SQLite partial and SQL Server filtered indexes: WHERE clause
		Operator:       kv["ops"],                                   // PG only: operator class (gin_trgm_ops, etc.)
		IncludeColumns: includeColumns,
		NullsDistinct:  parseBoolPtr(kv["nulls_distinct"]),
		TableName:      tableName, // Target table name
		StorageParams:  fullText,
	})
	return nil
}

// indexPartitioning reads the partitioning attributes of an index directive,
// which YDB's global indexes carry (see [ydbindex.ParseDeclaration]), as the
// YDB owner's facet, or no facet where the directive states none.
func (s *schemaParseState) indexPartitioning(kv map[string]string, comment *ast.Comment, structName string) (schemaext.Facets, error) {
	partitioning, err := ydbindex.ParseDeclaration(kv)
	if declaration, ok := errors.AsType[*ydbpartition.DeclarationError](err); ok {
		return schemaext.Facets{}, &ptaherr.ParseError{
			File: s.filename, Line: s.annotationContext(comment, "//ptah:schema:index", structName).line,
			Directive: "ptah:schema:index", Attribute: declaration.Attribute, Err: ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("%s on //ptah:schema:index at %s", declaration.Error(), structName),
		}
	}
	if err != nil {
		return schemaext.Facets{}, err
	}
	return ydbindex.WithPartitioning(schemaext.Facets{}, partitioning)
}

// indexVector reads the settings of a YDB vector index from an index
// directive as the YDB owner's facet value; see [ydbindex.DeclareVector].
func (s *schemaParseState) indexVector(kv map[string]string, comment *ast.Comment, structName string) (*ydbschema.DesiredVectorIndex, error) {
	vector, err := ydbindex.DeclareVector(kv, kv["type"], kv["ops"])
	if declaration, ok := errors.AsType[*ydbpartition.DeclarationError](err); ok {
		return nil, &ptaherr.ParseError{
			File: s.filename, Line: s.annotationContext(comment, "//ptah:schema:index", structName).line,
			Directive: "ptah:schema:index", Attribute: declaration.Attribute, Err: ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("%s on //ptah:schema:index at %s", declaration.Error(), structName),
		}
	}
	if err != nil {
		return nil, fmt.Errorf("index %q at %s: %w", kv["name"], structName, err)
	}
	return vector, nil
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:constraint", structName),
	); err != nil {
		return err
	}
	size, err := s.unsignedAttribute(kv, comment, structName, "constraint", "key_block_size")
	if err != nil {
		return err
	}
	constraint := parseConstraintComment(s.kv, comment, structName)
	constraint.KeyBlockSize = size
	s.schemaConstraints = append(s.schemaConstraints, constraint)
	return nil
}

func parseConstraintComment(kvParser parseutils.KeyValueParser, comment *ast.Comment, structName string) schemamodel.Constraint {
	kv := kvParser.ParseKeyValueComment(comment.Text)

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

	return schemamodel.Constraint{
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

// normalizeIdentityGeneration folds the identity_generation attribute to the
// two spellings the model records, and to the empty string for anything else.
func normalizeIdentityGeneration(value string) string {
	switch strings.ToUpper(strings.ReplaceAll(value, " ", "_")) {
	case "ALWAYS":
		return "ALWAYS"
	case "BY_DEFAULT":
		return "BY_DEFAULT"
	default:
		return ""
	}
}

func parseBoolPtr(value string) *bool {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return new(strings.EqualFold(value, "true"))
}

func (s *schemaParseState) parseExtensionComment(comment *ast.Comment) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:extension", kv["name"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	s.extensions = append(s.extensions, schemamodel.Extension{
		Name:        kv["name"],
		Schema:      kv["schema"],
		IfNotExists: kv["if_not_exists"] == "true",
		Version:     kv["version"],
		Comment:     kv["comment"],
		Dialects:    scope,
	})
	return nil
}

// parseNotDescribedComment records an object family, or one object in it, that
// this description declines to describe.
//
// A Go schema is not a serialized document, so it carries no leading comment
// header for the `ptah:not-described` directive that an HCL or SQL description
// uses. This is the same statement in the grammar Go annotations already have.
// Common families use [coverage.Set]. Standalone YDB limits use their owners'
// feature coverage so they cannot recreate a shared dialect model.
//
// `kind` is required and names a common coverage kind or an owned YDB family;
// an unknown one is refused rather than ignored, because ignoring it turns the
// absence it was protecting into a removal. `name` is optional: without it the
// whole family is declined, which is what a bare directive means in the
// serialized grammar too.
//
// The provenance is [coverage.Declared] because a person wrote it, and the
// reason is left unspecified: the annotation carries no room for one, and
// guessing would put a sentence in a diagnostic that the author never said.
func (s *schemaParseState) parseNotDescribedComment(comment *ast.Comment) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:notdescribed", kv["kind"])
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	if s.featureLimits.Add(kv["kind"], kv["name"]) {
		return nil
	}
	kind, err := coverage.ParseKind(kv["kind"])
	if err != nil {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: "ptah:schema:notdescribed",
			Attribute: "kind",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message:   fmt.Sprintf("%s on %s at %s", err, ctx.directive, ctx.location),
		}
	}

	s.notDescribed = append(s.notDescribed, coverage.Object{
		Kind:       kind,
		Name:       kv["name"],
		Provenance: coverage.Declared,
	})
	return nil
}

func (s *schemaParseState) parseSchemaComment(comment *ast.Comment) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	s.schemas = append(s.schemas, schemamodel.Schema{
		Name:    kv["name"],
		Comment: kv["comment"],
	})
	return nil
}

func (s *schemaParseState) parseTableComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	schemaName, tableName := tableDirectiveName(kv["schema"], kv["name"])
	if err := validateAttributes(
		kv,
		s.annotationContext(comment, "//ptah:schema:table", structName),
	); err != nil {
		return err
	}
	size, err := s.unsignedAttribute(kv, comment, structName, "table", "primary_key_block_size")
	if err != nil {
		return err
	}
	facets, err := s.tablePartitioning(kv, comment, structName)
	if err != nil {
		return err
	}
	columnTable, err := ydbcolumn.Parse(kv)
	if err != nil {
		return &ptaherr.ParseError{File: s.filename, Directive: "ptah:schema:table", Err: ptaherr.ErrInvalidAttributeValue, Message: err.Error()}
	}
	if columnTable != nil {
		if facets, err = facets.With(&ydbschema.DesiredColumnStore{ColumnStore: *columnTable}); err != nil {
			return err
		}
	}
	s.tableDirectives = append(s.tableDirectives, schemamodel.Table{
		StructName:          structName,
		Name:                tableName,
		APIName:             kv["api_name"],
		APINames:            targetNames(kv),
		Schema:              schemaName,
		Engine:              kv["engine"],
		Comment:             kv["comment"],
		PrimaryKey:          splitCSVAttribute(kv["primary_key"]),
		PrimaryKeyComment:   kv["primary_key_comment"],
		PrimaryKeyBlockSize: size,
		Checks:              splitCSVAttribute(kv["checks"]),
		DependsOn:           splitDependsOn(kv["depends_on"]),
		CustomSQL:           kv["custom"],
		Facets:              facets,
		Overrides:           parseutils.ParsePlatformSpecific(kv),
	})
	return nil
}

// tablePartitioning reads the settings of a table directive that a YDB row
// table carries (see [ydbpartition.ParseTableDeclaration]) as the YDB owner's
// facet, or no facet where the directive states none.
func (s *schemaParseState) tablePartitioning(kv map[string]string, comment *ast.Comment, structName string) (schemaext.Facets, error) {
	partitioning, err := ydbpartition.ParseTableDeclaration(kv)
	if declaration, ok := errors.AsType[*ydbpartition.DeclarationError](err); ok {
		return schemaext.Facets{}, &ptaherr.ParseError{
			File: s.filename, Line: s.annotationContext(comment, "//ptah:schema:table", structName).line,
			Directive: "ptah:schema:table", Attribute: declaration.Attribute, Err: ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("%s on //ptah:schema:table at %s", declaration.Error(), structName),
		}
	}
	if err != nil || partitioning == nil {
		return schemaext.Facets{}, err
	}
	return schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: *partitioning})
}

func tableDirectiveName(rawSchema, rawName string) (schemaName, tableName string) {
	return annotation.QualifiedName(rawSchema, rawName)
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
	// annotations are the owners the caller selected, and catalog and kv
	// read their directives beside the frontend's own.
	annotations           annotation.Set
	catalog               annotationmeta.Catalog
	kv                    parseutils.KeyValueParser
	featureLimits         ydbsource.Limits
	featureObjects        schemaext.Objects
	featureCoverage       schemaext.Coverage
	filename              string
	fset                  *token.FileSet
	tableNameToStructName map[string]string
	globalEnumsMap        map[string]schemamodel.Enum
	embeddedFields        []schemamodel.EmbeddedField
	schemaFields          []schemamodel.Field
	schemaIndexes         []schemamodel.Index
	schemaConstraints     []schemamodel.Constraint
	tableDirectives       []schemamodel.Table
	extensions            []schemamodel.Extension
	functions             []schemamodel.Function
	sequences             []schemamodel.Sequence
	domains               []schemamodel.Domain
	compositeTypes        []schemamodel.CompositeType
	ranges                []schemamodel.Range
	views                 []schemamodel.View
	synonyms              []schemamodel.Synonym
	materializedViews     []schemamodel.MaterializedView
	triggers              []schemamodel.Trigger
	rlsPolicies           []rlsPolicyDeclaration
	rlsEnabledTables      []rlsSwitchDeclaration
	pendingFacets         []pendingFacet
	roles                 []schemamodel.Role
	grants                []schemamodel.Grant
	revokedGrants         []schemamodel.Grant
	defaultPrivileges     []schemamodel.DefaultPrivilege
	managedData           []schemamodel.ManagedData
	schemas               []schemamodel.Schema
	notDescribed          []coverage.Object
	changefeeds           []pendingChangefeed
	columnFamilies        []pendingColumnFamily
	consumers             []pendingConsumer
	topics                []pendingTopic
	topicConsumers        []pendingTopicConsumer
	asyncReplications     []pendingReplication
	replicationItems      []pendingReplicationItem
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

func newSchemaParseState(filename string, fset *token.FileSet, selection parseSelection) *schemaParseState {
	return &schemaParseState{
		annotations:           selection.annotations,
		catalog:               selection.catalog,
		kv:                    parseutils.NewKeyValueParser(selection.catalog),
		filename:              filename,
		fset:                  fset,
		tableNameToStructName: make(map[string]string),
		globalEnumsMap:        make(map[string]schemamodel.Enum),
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
		catalog:   s.catalog,
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
	directive, ok := s.catalog.MatchCommentDirective(comment.Text)
	if !ok {
		return nil
	}
	if !annotationmeta.AllowsScope(directive, scope) {
		return nil
	}
	if handled, err := s.parsePlacementDirective(comment, directive.Name, target); handled || err != nil {
		return err
	}
	if _, owned := s.annotations.Owner(directive.Name); owned {
		return s.parseOwnerDirective(comment, directive.Name, target.structName)
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
	"ptah:schema:constraint":              (*schemaParseState).parseConstraintComment,
	"ptah:schema:enum":                    ignoringStruct((*schemaParseState).parseEnumComment),
	"ptah:schema:extension":               ignoringStruct((*schemaParseState).parseExtensionComment),
	"ptah:schema:function":                (*schemaParseState).parseFunctionComment,
	"ptah:schema:procedure":               (*schemaParseState).parseProcedureComment,
	"ptah:schema:sequence":                (*schemaParseState).parseSequenceComment,
	"ptah:schema:domain":                  (*schemaParseState).parseDomainComment,
	"ptah:schema:composite":               (*schemaParseState).parseCompositeComment,
	"ptah:schema:range":                   (*schemaParseState).parseRangeComment,
	"ptah:schema:view":                    (*schemaParseState).parseViewComment,
	"ptah:schema:matview":                 (*schemaParseState).parseMaterializedViewComment,
	"ptah:schema:synonym":                 (*schemaParseState).parseSynonymComment,
	"ptah:schema:coordinationnode":        (*schemaParseState).parseCoordinationNodeComment,
	"ptah:schema:trigger":                 (*schemaParseState).parseTriggerComment,
	"ptah:schema:rls:policy":              (*schemaParseState).parseRLSPolicyComment,
	"ptah:schema:rls:enable":              (*schemaParseState).parseRLSEnableComment,
	"ptah:schema:role":                    (*schemaParseState).parseRoleComment,
	"ptah:schema:grant":                   (*schemaParseState).parseGrantComment,
	"ptah:schema:revoke":                  (*schemaParseState).parseRevokeComment,
	"ptah:schema:defaultprivilege":        (*schemaParseState).parseDefaultPrivilegeComment,
	"ptah:schema:data":                    (*schemaParseState).parseManagedDataComment,
	"ptah:schema:notdescribed":            ignoringStruct((*schemaParseState).parseNotDescribedComment),
	"ptah:schema:changefeed":              (*schemaParseState).parseChangefeedComment,
	columnFamilyDirective:                 (*schemaParseState).parseColumnFamilyComment,
	"ptah:schema:changefeed:consumer":     (*schemaParseState).parseChangefeedConsumerComment,
	"ptah:schema:topic":                   (*schemaParseState).parseTopicComment,
	"ptah:schema:topic:consumer":          (*schemaParseState).parseTopicConsumerComment,
	"ptah:schema:resourcepool":            (*schemaParseState).parseResourcePoolComment,
	"ptah:schema:resourcepool:classifier": (*schemaParseState).parseResourcePoolClassifierComment,

	// YDB's async replications, their items and transfers.
	"ptah:schema:async_replication":      (*schemaParseState).parseAsyncReplicationComment,
	"ptah:schema:async_replication:item": (*schemaParseState).parseAsyncReplicationItemComment,
	"ptah:schema:transfer":               (*schemaParseState).parseTransferComment,
	"ptah:schema:secret":                 (*schemaParseState).parseSecretComment,
	"ptah:schema:streamingquery":         (*schemaParseState).parseStreamingQueryComment,
	"ptah:schema:externaldatasource":     (*schemaParseState).parseExternalDataSourceComment,
	"ptah:schema:externaltable":          (*schemaParseState).parseExternalTableComment,
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	s.globalEnumsMap[kv["name"]] = schemamodel.Enum{
		Name:    kv["name"],
		Values:  splitCommaList(kv["values"]),
		Comment: kv["comment"],
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
// annotations selects the feature owners whose directives the parse reads,
// usually the set of the runtime the caller renders and compares with. A parse
// reads a directive of an owner it did not select no more than one it does not
// know, so a caller that wants the frontend's own directives only passes
// [annotation.None]. The zero set is refused with [annotation.ErrUnselected].
//
// A file that does not parse as Go returns a [ptaherr.ParseError] naming it.
func ParseFile(annotations annotation.Set, filename string) (schemamodel.Database, error) {
	selection, err := selectAnnotations(annotations)
	if err != nil {
		return schemamodel.Database{}, err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		return schemamodel.Database{}, &ptaherr.ParseError{
			File:    filename,
			Err:     err,
			Message: fmt.Sprintf("parse Go file %q: %v", filename, err),
		}
	}

	return parseFileAST(filename, fset, f, selection)
}

// ParseSource parses Go source a caller already holds and returns the schema
// it declares. It is [ParseFile] for in-memory source: source can be a
// string, []byte, or io.Reader, and filename is not opened -- it names the
// source in diagnostics and in the File field of a returned
// [ptaherr.ParseError], and its directory becomes the relative
// [schemamodel.ManagedData.SourceDir] of any managed-data annotation the
// source declares.
//
// The result is un-finalized exactly as ParseFile's is -- embedded fields not
// expanded, nothing deduplicated: one file is not a schema. Run it through
// [schemamodel.Merge] or [schemamodel.Finalize] before handing it to a
// renderer or a diff. Source that does not parse as Go, and an annotation the
// parser refuses -- an unknown, retired, or missing required attribute, or an
// invalid value -- both return a [ptaherr.ParseError]; errors.Is against the
// ptaherr sentinels tells the refusals apart. annotations selects the feature
// owners whose directives the parse reads, as it does for ParseFile.
func ParseSource(annotations annotation.Set, filename string, source any) (schemamodel.Database, error) {
	selection, err := selectAnnotations(annotations)
	if err != nil {
		return schemamodel.Database{}, err
	}
	return parseSource(filename, source, selection)
}

func parseSource(filename string, source any, selection parseSelection) (schemamodel.Database, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, source, parser.ParseComments)
	if err != nil {
		slog.Error("Failed to parse file", "error", err)
		return schemamodel.Database{}, &ptaherr.ParseError{
			File:    filename,
			Err:     err,
			Message: fmt.Sprintf("parse Go source %q: %v", filename, err),
		}
	}

	return parseFileAST(filename, fset, f, selection)
}

// parseSelection is the owners one parse reads and the directive catalog that
// joins their directives to the frontend's own.
type parseSelection struct {
	annotations annotation.Set
	catalog     annotationmeta.Catalog
}

func selectAnnotations(annotations annotation.Set) (parseSelection, error) {
	catalog, err := annotationmeta.NewCatalog(annotations)
	if err != nil {
		return parseSelection{}, fmt.Errorf("select Go annotation owners: %w", err)
	}
	return parseSelection{annotations: annotations, catalog: catalog}, nil
}

func parseFileAST(filename string, fset *token.FileSet, f *ast.File, selection parseSelection) (schemamodel.Database, error) {
	state := newSchemaParseState(filename, fset, selection)
	if err := state.processFileAST(f); err != nil {
		return schemamodel.Database{}, err
	}
	if err := state.attachChangefeeds(); err != nil {
		return schemamodel.Database{}, err
	}
	if err := state.attachTopicConsumers(); err != nil {
		return schemamodel.Database{}, err
	}
	if err := state.attachReplicationItems(); err != nil {
		return schemamodel.Database{}, err
	}
	if err := state.attachColumnFamilies(); err != nil {
		return schemamodel.Database{}, err
	}
	if err := state.attachOwnerFacets(); err != nil {
		return schemamodel.Database{}, err
	}
	policies, switches, err := state.attachRowSecurity()
	if err != nil {
		return schemamodel.Database{}, err
	}

	enums := make([]schemamodel.Enum, 0, len(state.globalEnumsMap))
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

	result := schemamodel.Database{
		FeatureObjects:    state.featureObjects,
		FeatureCoverage:   state.featureCoverage,
		Schemas:           state.schemas,
		Tables:            state.tableDirectives,
		Fields:            state.schemaFields,
		Indexes:           state.schemaIndexes,
		Constraints:       state.schemaConstraints,
		Enums:             enums,
		EmbeddedFields:    state.embeddedFields,
		Extensions:        state.extensions,
		Functions:         state.functions,
		Sequences:         state.sequences,
		Domains:           state.domains,
		CompositeTypes:    state.compositeTypes,
		Ranges:            state.ranges,
		Views:             state.views,
		Synonyms:          state.synonyms,
		MaterializedViews: state.materializedViews,
		Triggers:          state.triggers,
		RLSPolicies:       policies,
		RLSEnabledTables:  switches,
		Roles:             state.roles,
		Grants:            state.grants,
		RevokedGrants:     state.revokedGrants,
		DefaultPrivileges: state.defaultPrivileges,
		ManagedData:       state.managedData,
		NotDescribed:      coverage.Set{}.With(state.notDescribed...),
		Dependencies:      make(map[string][]string),
	}
	schemamodel.NormalizeTableScopedNames(&result)
	schemamodel.BuildDependencyGraph(&result)
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
	directive, ok := s.catalog.MatchCommentDirective(comment.Text)
	if !ok || directive.Name != "ptah:schema:table" {
		return
	}
	kv := s.kv.ParseKeyValueComment(comment.Text)
	tableName := kv["name"]
	if tableName == "" {
		return
	}
	s.tableNameToStructName[tableName] = structName
	if schemaName := kv["schema"]; schemaName != "" {
		s.tableNameToStructName[schemamodel.QualifyTableName(schemaName, tableName)] = structName
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
	placements := annotationmeta.CommentPlacements(f)

	for _, commentGroup := range f.Comments {
		for _, comment := range commentGroup.List {
			if placements[comment].Scope != annotationmeta.ScopeFile {
				continue
			}
			if err := s.parseFileScopedRLSComment(comment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *schemaParseState) parseFileScopedRLSComment(comment *ast.Comment) error {
	directive, ok := s.catalog.MatchCommentDirective(comment.Text)
	if !ok {
		return nil
	}
	switch directive.Name {
	case "ptah:schema:rls:policy":
		return s.parseFileScopedRLSPolicyComment(comment)
	case "ptah:schema:rls:enable":
		return s.parseFileScopedRLSEnableComment(comment)
	}
	return nil
}

func (s *schemaParseState) parseFileScopedRLSPolicyComment(comment *ast.Comment) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	structName, exists := s.tableNameToStructName[tableName]
	if !exists {
		return nil
	}

	restrictive, err := rlsPolicyRestrictive(kv, ctx)
	if err != nil {
		return err
	}
	s.rlsPolicies = append(s.rlsPolicies, rlsPolicyDeclaration{ctx: ctx, policy: schemamodel.RLSPolicy{
		StructName:          structName,
		Name:                policyName,
		Table:               tableName,
		PolicyFor:           kv["for"],
		ToRoles:             kv["to"],
		UsingExpression:     kv["using"],
		WithCheckExpression: kv["with_check"],
		Comment:             kv["comment"],
		Restrictive:         restrictive,
		Dialects:            scope,
	}})
	return nil
}

func (s *schemaParseState) parseFileScopedRLSEnableComment(comment *ast.Comment) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	structName, exists := s.tableNameToStructName[tableName]
	if !exists {
		return nil
	}

	s.rlsEnabledTables = append(s.rlsEnabledTables, rlsSwitchDeclaration{ctx: ctx, enabled: schemamodel.RLSEnabledTable{
		StructName: structName,
		Table:      tableName,
		Comment:    kv["comment"],
		Forced:     kv["force"] == "true",
		Dialects:   scope,
	}})
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
// qualifiedObjectName folds a declared schema into an object's name.
//
// Function, view and materialized-view declarations have nowhere else to put
// it: unlike a sequence, domain, composite or range, their IR carries no
// Schema field, so the schema travels inside Name as a qualified identifier.
// That is not a workaround invented here -- it is what the atlas.hcl frontend
// already produces for these same three kinds (internal/atlashcl/objects.go
// builds them with tableref.Canonical), what the renderers split back apart
// quote-aware, and what the comparator keys on. Writing anything else would
// give the two frontends two spellings of one declaration.
//
// A declaration naming no schema keeps its name byte for byte. Canonical would
// otherwise quote a name containing a dot or a quote character, and re-spelling
// names that already work is not what adding an optional attribute may do
// (stokaro/ptah#1270).
func qualifiedObjectName(kv map[string]string) string {
	schema := strings.TrimSpace(kv["schema"])
	if schema == "" {
		return kv["name"]
	}
	return tableref.Canonical(schema, kv["name"])
}

func (s *schemaParseState) parseProcedureComment(comment *ast.Comment, structName string) error {
	// The attributes are validated against the procedure's own directive, so
	// `returns=` is refused by name rather than accepted and then rejected
	// below -- the registry is where an operator reads what a directive takes.
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	routine.Kind = schemamodel.FunctionKindProcedure
	return nil
}

func (s *schemaParseState) parseFunctionComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:function", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}

	parallel, err := routineParallelLevel(kv, ctx)
	if err != nil {
		return err
	}
	fn := schemamodel.Function{
		StructName: structName,
		Name:       qualifiedObjectName(kv),
		Parameters: kv["params"],
		Returns:    kv["returns"],
		Language:   kv["language"],
		Security:   kv["security"],
		Volatility: kv["volatility"],
		Settings:   routinesetting.NormalizeAll(splitRoutineSettings(kv["settings"])),
		Leakproof:  kv["leakproof"] == "true",
		Parallel:   parallel,
		Strict:     kv["strict"] == "true",
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

// routineParallelLevel reads a routine's `parallel`, which names how the
// planner may use it.
//
// An unrecognized level is refused rather than folded into a default. UNSAFE is
// the most restrictive of the three, so folding a misspelled SAFE into it would
// quietly forbid the parallelism the author asked for, and folding the other
// way would quietly permit what they did not (stokaro/ptah#3121).
func routineParallelLevel(kv map[string]string, ctx annotationErrorContext) (string, error) {
	raw, ok := kv["parallel"]
	if !ok {
		return "", nil
	}
	level := strings.ToUpper(strings.TrimSpace(raw))
	switch level {
	case "":
		return "", nil
	case "SAFE", "RESTRICTED", "UNSAFE":
		return level, nil
	default:
		slog.Error("unsupported routine parallel level",
			"directive", ctx.directive,
			"value", raw,
			"location", ctx.location,
		)
		return "", &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "parallel",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("parallel on %s at %s: must be SAFE, RESTRICTED or UNSAFE, got %q",
				ctx.directive, ctx.location, raw),
		}
	}
}

func (s *schemaParseState) parseSequenceComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	seq := schemamodel.Sequence{
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
	if !schemamodel.IsValidSequenceType(seq.AsType) {
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	domain := schemamodel.Domain{
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	composite := schemamodel.CompositeType{
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
func parseCompositeFields(value string) ([]schemamodel.CompositeField, error) {
	var fields []schemamodel.CompositeField
	for _, part := range splitTopLevelCommaList(value) {
		name, typ, ok := strings.Cut(part, ":")
		name = strings.TrimSpace(name)
		typ = strings.TrimSpace(typ)
		if !ok || name == "" || typ == "" {
			return nil, fmt.Errorf("invalid composite field %q", part)
		}
		fields = append(fields, schemamodel.CompositeField{Name: name, Type: typ})
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	rangeType := schemamodel.Range{
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
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	s.views = append(s.views, schemamodel.View{
		StructName: structName,
		Name:       qualifiedObjectName(kv),
		Body:       kv["body"],
		WithCheck:  kv["with_check"] == "true",
		Comment:    kv["comment"],
		DependsOn:  splitDependsOn(kv["depends_on"]),
		Dialects:   scope,
	})
	return nil
}

// parseSynonymComment reads a SQL Server synonym declaration.
//
// There is no dialect scope here, and the omission is deliberate: a synonym is
// a SQL Server object and nothing else, so a scope attribute would let a schema
// claim it belongs to a target that has no such construct.
func (s *schemaParseState) parseSynonymComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:synonym", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	s.synonyms = append(s.synonyms, schemamodel.Synonym{
		StructName: structName,
		Name:       kv["name"],
		Schema:     kv["schema"],
		Target:     kv["target"],
		Comment:    kv["comment"],
	})
	return nil
}

// parseCoordinationNodeComment reads a YDB coordination node declaration.
//
// There is no dialect scope here, for the reason a synonym has none: a
// coordination node is a YDB object and nothing else. The settings are read
// and checked by dialect/ydb/ydbcoordination, which the YAML reader asks too, so
// a value one source accepts is one the other accepts. Ptah's own lock node
// and a name with a segment that starts with a dot are refused where they are
// written.
func (s *schemaParseState) parseCoordinationNodeComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:coordinationnode", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	if err := ydbcoordination.RefuseName(kv["schema"], kv["name"]); err != nil {
		return coordinationNodeError(ctx, "name", err)
	}
	spec, err := ydbcoordination.ParseDeclaration(kv)
	if setting, ok := errors.AsType[*ydbcoordination.SettingError](err); ok {
		return coordinationNodeError(ctx, setting.Setting, err)
	}
	if err != nil {
		return err
	}
	s.featureObjects, err = s.featureObjects.With(ydbcoordination.DesiredObject(kv["schema"], kv["name"], structName, spec))
	return err
}

// coordinationNodeError is the parse error for a coordination node
// attribute whose value the node cannot take.
func coordinationNodeError(ctx annotationErrorContext, attribute string, err error) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: strings.TrimPrefix(ctx.directive, "//"),
		Attribute: attribute,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s on %s at %s", err.Error(), ctx.directive, ctx.location),
	}
}

func (s *schemaParseState) parseMaterializedViewComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	// this line (stokaro/ptah#1625). The attributes owners add to the
	// directive, such as the ClickHouse refresh schedule, are read by their
	// owners into settings of the view (stokaro/ptah#1802).
	owned, err := s.annotations.DecodeAttributes("ptah:schema:matview", kv)
	if err != nil {
		return ownerAttributeError(ctx, err)
	}
	s.materializedViews = append(s.materializedViews, schemamodel.MaterializedView{
		Facets:     owned,
		StructName: structName,
		Name:       qualifiedObjectName(kv),
		Body:       kv["body"],
		Comment:    kv["comment"],
		DependsOn:  splitDependsOn(kv["depends_on"]),
		Dialects:   scope,
	})
	return nil
}

// ownerAttributeError reports an attribute an owner added to the directive
// and refused, in the parse error shape every other refusal uses.
func ownerAttributeError(ctx annotationErrorContext, err error) error {
	slog.Error("invalid owner attribute",
		"directive", ctx.directive,
		"location", ctx.location,
		"error", err,
	)
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: strings.TrimPrefix(ctx.directive, "//"),
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s at %s: %s", ctx.directive, ctx.location, err),
	}
}

func (s *schemaParseState) parseTriggerComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	trigger := schemamodel.Trigger{
		StructName: structName,
		Name:       kv["name"],
		Table:      kv["table"],
		Timing:     kv["timing"],
		Event:      kv["event"],
		ForEach:    kv["for"],
		When:       kv["when"],
		OldTable:   kv["old_table"],
		NewTable:   kv["new_table"],
		Body:       kv["body"],
		Comment:    kv["comment"],
		Dialects:   scope,
	}
	trigger.Canonicalize()
	s.triggers = append(s.triggers, trigger)
	return nil
}

func (s *schemaParseState) parseRLSPolicyComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:rls:policy", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	restrictive, err := rlsPolicyRestrictive(kv, ctx)
	if err != nil {
		return err
	}
	s.rlsPolicies = append(s.rlsPolicies, rlsPolicyDeclaration{ctx: ctx, policy: schemamodel.RLSPolicy{
		StructName:          structName,
		Name:                kv["name"],
		Table:               kv["table"],
		PolicyFor:           kv["for"],
		ToRoles:             kv["to"],
		UsingExpression:     kv["using"],
		WithCheckExpression: kv["with_check"],
		Comment:             kv["comment"],
		Restrictive:         restrictive,
		Dialects:            scope,
	}})
	return nil
}

// rlsPolicyRestrictive reads a policy's `as`, which decides how the policy
// combines with the table's others rather than how it is written.
//
// An unrecognized value is refused rather than treated as the default:
// PERMISSIVE is the weaker of the two, so folding a misspelled RESTRICTIVE
// into it grants the access the policy was written to withhold
// (stokaro/ptah#3121).
func rlsPolicyRestrictive(kv map[string]string, ctx annotationErrorContext) (bool, error) {
	raw, ok := kv["as"]
	if !ok {
		return false, nil
	}
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "PERMISSIVE":
		return false, nil
	case "RESTRICTIVE":
		return true, nil
	default:
		slog.Error("unsupported RLS policy as value",
			"directive", ctx.directive,
			"value", raw,
			"location", ctx.location,
		)
		return false, &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "as",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("as on %s at %s: must be PERMISSIVE or RESTRICTIVE, got %q",
				ctx.directive, ctx.location, raw),
		}
	}
}

func (s *schemaParseState) parseRLSEnableComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:rls:enable", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	s.rlsEnabledTables = append(s.rlsEnabledTables, rlsSwitchDeclaration{ctx: ctx, enabled: schemamodel.RLSEnabledTable{
		StructName: structName,
		Table:      kv["table"],
		Comment:    kv["comment"],
		Forced:     kv["force"] == "true",
		Dialects:   scope,
	}})
	return nil
}

func (s *schemaParseState) parseRoleComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:role", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	scope, err := parseDialectScope(kv, ctx)
	if err != nil {
		return err
	}
	s.roles = append(s.roles, schemamodel.Role{
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
		Group:       kv["group"] == "true",
		MemberOf:    splitCommaList(kv["member_of"]),
		Dialects:    scope,
	})
	return nil
}

func (s *schemaParseState) parseGrantComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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
	grant := schemamodel.Grant{
		StructName: structName,
		Role:       kv["role"],
		Privileges: privileges,
		OnTable:    kv["on_table"],
		OnSchema:   kv["on_schema"],
		OnSequence: kv["on_sequence"],
		OnDatabase: kv["on_database"] == "true",
		WithOption: kv["with_option"] == "true" || kv["grant_option"] == "true",
		Comment:    kv["comment"],
		Dialects:   scope,
	}
	if err := setRoutineTarget(&grant, kv, ctx); err != nil {
		return err
	}
	if err := setGrantColumns(&grant, kv, ctx); err != nil {
		return err
	}
	grant.Canonicalize()
	s.grants = append(s.grants, grant)
	return nil
}

// parseRevokeComment reads one //ptah:schema:revoke directive: privileges the
// role is declared not to hold, one [schemamodel.Grant] per directive in
// [schemamodel.Database.RevokedGrants].
func (s *schemaParseState) parseRevokeComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:revoke", structName)
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
	privileges := splitCommaList(kv["privilege"])
	if len(privileges) == 0 {
		privileges = splitCommaList(kv["privileges"])
	}
	if len(privileges) == 0 {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "privilege",
			Err:       ptaherr.ErrMissingRequiredAttribute,
			Message:   fmt.Sprintf("missing required annotation attribute %q on %s at %s", "privilege", ctx.directive, ctx.location),
		}
	}
	revoked := schemamodel.Grant{
		StructName: structName,
		Role:       kv["role"],
		Privileges: privileges,
		OnTable:    kv["on_table"],
		OnSchema:   kv["on_schema"],
		OnSequence: kv["on_sequence"],
		OnDatabase: kv["on_database"] == "true",
		Comment:    kv["comment"],
		Dialects:   scope,
	}
	if err := setRoutineTarget(&revoked, kv, ctx); err != nil {
		return err
	}
	if err := setGrantColumns(&revoked, kv, ctx); err != nil {
		return err
	}
	revoked.Canonicalize()
	s.revokedGrants = append(s.revokedGrants, revoked)
	return nil
}

// setGrantColumns reads the columns a grant or revoke is limited to, which
// only a table target has.
func setGrantColumns(grant *schemamodel.Grant, kv map[string]string, ctx annotationErrorContext) error {
	columns := splitCommaList(kv["columns"])
	if len(columns) == 0 {
		return nil
	}
	if strings.TrimSpace(grant.OnTable) == "" {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "columns",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("columns on %s at %s needs on_table: column privileges apply to a table",
				ctx.directive, ctx.location),
		}
	}
	grant.Columns = columns
	return nil
}

// setRoutineTarget reads on_function or on_procedure, whose value names the
// routine and its argument types: purge(uuid). The types are required because
// PostgreSQL overloads a name by them, and a directive that named only the
// routine could not say which one it means.
func setRoutineTarget(grant *schemamodel.Grant, kv map[string]string, ctx annotationErrorContext) error {
	for _, attribute := range []struct{ key, kind string }{{"on_function", "FUNCTION"}, {"on_procedure", "PROCEDURE"}} {
		value := strings.TrimSpace(kv[attribute.key])
		if value == "" {
			continue
		}
		name, arguments, ok := routineargs.SplitTarget(value)
		if !ok {
			return &ptaherr.ParseError{
				File:      ctx.file,
				Line:      ctx.line,
				Directive: strings.TrimPrefix(ctx.directive, "//"),
				Attribute: attribute.key,
				Err:       ptaherr.ErrInvalidAttributeValue,
				Message: fmt.Sprintf("%s=%q on %s at %s needs the argument types in parentheses, as in purge(uuid): "+
					"PostgreSQL tells overloaded routines apart by them", attribute.key, value, ctx.directive, ctx.location),
			}
		}
		grant.OnRoutine = name
		grant.RoutineArguments = arguments
		grant.RoutineKind = attribute.kind
	}
	return nil
}

// defaultPrivilegeObjectTypes is the closed set ALTER DEFAULT PRIVILEGES names,
// and the set the catalog reader folds pg_default_acl's defaclobjtype into.
//
// SCHEMAS and LARGE OBJECTS are named only by the global form, without a
// schema: PostgreSQL 18.6 refuses IN SCHEMA for both.
var defaultPrivilegeObjectTypes = []string{"TABLES", "SEQUENCES", "FUNCTIONS", "TYPES", "SCHEMAS", "LARGE OBJECTS"}

// parseDefaultPrivilegeComment reads one //ptah:schema:defaultprivilege
// directive into the schema model.
//
// The object type and the grantable subset are checked here rather than at
// render time. Without the object-type check a misspelled keyword reaches the
// renderer and the author learns about it from the server's syntax error;
// without the subset check the model holds a contradiction -- a privilege
// marked grantable that is not granted at all -- which renders nothing and
// compares as a difference the planner can never resolve.
func (s *schemaParseState) parseDefaultPrivilegeComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:defaultprivilege", structName)
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
	objectType, err := defaultPrivilegeObjectType(kv["object_type"], strings.TrimSpace(kv["schema"]), ctx)
	if err != nil {
		return err
	}
	privileges, err := defaultPrivilegeGrants(kv, ctx)
	if err != nil {
		return err
	}
	revoked := splitCommaList(kv["revoked"])
	if len(privileges) == 0 && len(revoked) == 0 {
		return &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "privileges",
			Err:       ptaherr.ErrMissingRequiredAttribute,
			Message: fmt.Sprintf("missing required annotation attribute %q on %s at %s: "+
				"a default privilege grants privileges, revokes them, or both", "privileges", ctx.directive, ctx.location),
		}
	}
	privilege := schemamodel.DefaultPrivilege{
		StructName: structName,
		Grantor:    kv["for_role"],
		Schema:     kv["schema"],
		ObjectType: objectType,
		Grantee:    kv["grantee"],
		Privileges: privileges,
		Revoked:    revoked,
		Comment:    kv["comment"],
		Dialects:   scope,
	}
	privilege.Canonicalize()
	s.defaultPrivileges = append(s.defaultPrivileges, privilege)
	return nil
}

// defaultPrivilegeObjectType reads object_type, refusing one the schema
// cannot carry: a global-only type beside a schema.
func defaultPrivilegeObjectType(raw, schema string, ctx annotationErrorContext) (string, error) {
	objectType := strings.ToUpper(strings.TrimSpace(raw))
	if schemamodel.GlobalOnlyDefaultPrivilegeObjectType(objectType) && schema != "" {
		return "", &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "object_type",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf(
				"invalid %q value %q on %s at %s: PostgreSQL sets it for the whole database only, "+
					"so the declaration cannot name a schema",
				"object_type", raw, ctx.directive, ctx.location,
			),
		}
	}
	if slices.Contains(defaultPrivilegeObjectTypes, objectType) {
		return objectType, nil
	}
	slog.Error("unsupported default privilege object type",
		"directive", ctx.directive,
		"value", raw,
		"location", ctx.location,
	)
	return "", &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: strings.TrimPrefix(ctx.directive, "//"),
		Attribute: "object_type",
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message: fmt.Sprintf(
			"invalid %q value %q on %s at %s: must be one of %s",
			"object_type", raw, ctx.directive, ctx.location,
			strings.Join(defaultPrivilegeObjectTypes, ", "),
		),
	}
}

// defaultPrivilegeGrants folds the two attribute lists into one list of pairs.
//
// Two parallel lists are what the author writes and what the catalog cannot
// hold: pg_default_acl explodes to one row per privilege, each with its own
// is_grantable. Folding at the parse boundary means nothing downstream has to
// keep the lists in step.
func defaultPrivilegeGrants(
	kv map[string]string,
	ctx annotationErrorContext,
) ([]schemamodel.PrivilegeGrant, error) {
	privileges := splitCommaList(kv["privileges"])
	grantable := make(map[string]bool, len(privileges))
	for _, name := range splitCommaList(kv["grantable"]) {
		grantable[strings.ToUpper(name)] = true
	}
	granted := make(map[string]bool, len(privileges))
	grants := make([]schemamodel.PrivilegeGrant, 0, len(privileges))
	for _, name := range privileges {
		normalized := strings.ToUpper(name)
		granted[normalized] = true
		grants = append(grants, schemamodel.PrivilegeGrant{
			Privilege:  name,
			WithOption: grantable[normalized],
		})
	}
	for _, name := range slices.Sorted(maps.Keys(grantable)) {
		if granted[name] {
			continue
		}
		slog.Error("grantable privilege is not granted",
			"directive", ctx.directive,
			"privilege", name,
			"location", ctx.location,
		)
		return nil, &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: strings.TrimPrefix(ctx.directive, "//"),
			Attribute: "grantable",
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf(
				"invalid %q value %q on %s at %s: %q is not in %q",
				"grantable", kv["grantable"], ctx.directive, ctx.location,
				name, kv["privileges"],
			),
		}
	}
	return grants, nil
}

func (s *schemaParseState) parseManagedDataComment(comment *ast.Comment, structName string) error {
	kv := s.kv.ParseKeyValueComment(comment.Text)
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

	s.managedData = append(s.managedData, schemamodel.ManagedData{
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
func targetNames(kv map[string]string) schemamodel.TargetNames {
	return schemamodel.TargetNames{
		OpenAPI:  kv["openapi_name"],
		GraphQL:  kv["graphql_name"],
		Protobuf: kv["proto_name"],
	}
}

// splitRoutineSettings reads the `settings` attribute, which carries several
// `name=value` entries separated by `;`.
//
// A semicolon rather than a comma, because a value is itself comma-separated:
// `search_path=pg_catalog, pg_temp` is one setting (stokaro/ptah#2356).
// splitDependsOn reads a comma-separated `depends_on` into the object names it
// carries, dropping empty entries so a trailing comma is not an object nobody
// declared.
func splitDependsOn(value string) []string {
	var names []string
	for name := range strings.SplitSeq(value, ",") {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	return names
}

func splitRoutineSettings(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.Split(value, ";")
}

// unsignedAttribute refuses a malformed hint before it can become the default.
func (s *schemaParseState) unsignedAttribute(kv map[string]string, comment *ast.Comment, structName, kind, attribute string) (uint64, error) {
	value, present := kv[attribute]
	if !present {
		return 0, nil
	}
	size, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, &ptaherr.ParseError{
			File: s.filename, Line: s.annotationContext(comment, "//ptah:schema:"+kind, structName).line,
			Directive: "ptah:schema:" + kind, Attribute: attribute, Err: ptaherr.ErrInvalidAttributeValue,
			Message: fmt.Sprintf("invalid %s %q on //ptah:schema:%s at %s (must be a non-negative integer)", attribute, value, kind, structName),
		}
	}
	return size, nil
}
