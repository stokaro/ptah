// Package atlashcl parses HCL schema files into Ptah's schema IR.
package atlashcl

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/tableref"
)

// Options configures HCL schema parsing.
type Options struct {
	// IgnoreUnknownNames accepts and drops HCL names this parser does not
	// model instead of refusing the whole file.
	//
	// Off by default. It is opt-in because three callers depend on the
	// refusal: Ptah's own schema loading and the native `ptah schema`
	// commands, where an unmodeled name is a user error worth naming, and
	// internal/goannotationexport, which re-parses HCL Ptah itself rendered as
	// a round-trip fidelity check -- tolerance there would let a renderer typo
	// pass unnoticed.
	//
	// Turning it on does not make the body of a dropped name unchecked: see
	// tolerateUnknownBlock.
	IgnoreUnknownNames bool

	// RecordIgnored receives every name dropped under IgnoreUnknownNames, in
	// the order the parser reached them. Optional; nil discards them, which is
	// what the community binary does.
	RecordIgnored func(IgnoredName)
}

// ParseFile parses an HCL schema file into the same Database IR used by
// Go annotations and YAML schema files.
func ParseFile(path string) (*goschema.Database, error) {
	return ParseFileWithOptions(path, Options{})
}

// Parse parses HCL schema text into the same Database IR used by Go
// annotations and YAML schema files.
func Parse(data []byte, filename string) (*goschema.Database, error) {
	return ParseWithOptions(data, filename, Options{})
}

// ParseFileWithOptions parses an HCL schema file under the given options.
func ParseFileWithOptions(path string, opts Options) (*goschema.Database, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read HCL schema file: %w", err)
	}

	return ParseWithOptions(data, path, opts)
}

// ParseWithOptions parses HCL schema text under the given options.
func ParseWithOptions(data []byte, filename string, opts Options) (*goschema.Database, error) {
	if filename == "" {
		filename = "schema.hcl"
	}
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse HCL schema: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse HCL schema: unsupported body type %T", file.Body)
	}

	p := parser{
		src:           data,
		sourceDir:     filepath.Dir(filename),
		db:            &goschema.Database{},
		tolerant:      opts.IgnoreUnknownNames,
		recordIgnored: opts.RecordIgnored,
	}
	if err := p.parseBody(body); err != nil {
		return nil, err
	}
	goschema.Finalize(p.db)
	return p.db, nil
}

type parser struct {
	src       []byte
	sourceDir string
	db        *goschema.Database

	// tolerant enables the unknown-name policy described on Options.
	tolerant bool
	// recordIgnored receives the names dropped under that policy. Nil discards
	// them.
	recordIgnored func(IgnoredName)
}

func (p *parser) parseBody(body *hclsyntax.Body) error {
	for _, block := range body.Blocks {
		if err := p.parseTopLevelBlock(block); err != nil {
			return err
		}
	}
	return nil
}

func (p *parser) parseTopLevelBlock(block *hclsyntax.Block) error {
	switch block.Type {
	case "schema":
		return p.parseSchema(block)
	case "enum":
		return p.parseEnum(block)
	case "sequence":
		return p.parseSequence(block)
	case "domain":
		return p.parseDomain(block)
	case "composite":
		return p.parseComposite(block)
	case "range":
		return p.parseRange(block)
	case "table":
		return p.parseTable(block)
	case "extension":
		return p.parseExtension(block)
	case "function":
		return p.parseFunction(block)
	case "view":
		return p.parseView(block)
	case "materialized":
		return p.parseMaterializedView(block)
	case "trigger":
		return p.parseTrigger(block)
	case "policy":
		return p.parsePolicy(block)
	case "role":
		return p.parseRole(block)
	case "permission":
		return p.parsePermission(block)
	case "data":
		return p.parseManagedData(block)
	case "env", "variable":
		// Project-level HCL can carry env/variable blocks next to schema blocks.
		// They do not define schema objects directly.
		return nil
	default:
		return p.rejectUnsupportedBlock(block, "top-level")
	}
}

func (p *parser) parseSchema(block *hclsyntax.Block) error {
	if len(block.Labels) != 1 {
		return p.blockError(block, "schema block requires exactly one label")
	}
	if err := p.rejectUnsupportedSchemaBody(block); err != nil {
		return err
	}
	p.db.Schemas = append(p.db.Schemas, goschema.Schema{
		Name:    block.Labels[0],
		Comment: p.optionalString(block.Body.Attributes["comment"]),
		Charset: p.optionalString(block.Body.Attributes["charset"]),
		Collate: p.optionalString(block.Body.Attributes["collate"]),
	})
	return nil
}

func (p *parser) parseEnum(block *hclsyntax.Block) error {
	if len(block.Labels) != 1 {
		return p.blockError(block, "enum block requires exactly one label")
	}
	if err := p.rejectUnsupportedEnumAttrs(block); err != nil {
		return err
	}
	values, err := p.stringListAttr(block, "values")
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return p.blockError(block, "enum %q requires values", block.Labels[0])
	}
	p.db.Enums = append(p.db.Enums, goschema.Enum{
		Name:   block.Labels[0],
		Values: values,
	})
	return nil
}

func (p *parser) parseTable(block *hclsyntax.Block) error {
	labels, err := p.tableLabels(block)
	if err != nil {
		return err
	}

	checks, err := p.stringListAttr(block, "checks")
	if err != nil {
		return err
	}
	customSQL, err := p.stringAttr(block, "custom", "table")
	if err != nil {
		return err
	}
	overrides, err := p.parsePlatformOverrides(block, "table")
	if err != nil {
		return err
	}
	strict, err := p.optionalTableBool(block, "strict", false)
	if err != nil {
		return err
	}
	withoutRowID, err := p.optionalTableBool(block, "without_rowid", false)
	if err != nil {
		return err
	}

	table := goschema.Table{
		StructName:    hclTableStructName(labels.schema, labels.name),
		Name:          labels.name,
		Schema:        labels.schema,
		Engine:        p.optionalString(block.Body.Attributes["engine"]),
		AutoIncrement: p.optionalString(block.Body.Attributes["auto_increment"]),
		Charset:       p.optionalString(block.Body.Attributes["charset"]),
		Collate:       p.optionalString(block.Body.Attributes["collate"]),
		Strict:        strict,
		WithoutRowID:  withoutRowID,
		Comment:       p.optionalString(block.Body.Attributes["comment"]),
		Checks:        checks,
		CustomSQL:     customSQL,
		Overrides:     overrides,
	}

	fieldsStart := len(p.db.Fields)
	unlabeledCheckOrdinal := 0
	for _, nested := range block.Body.Blocks {
		if nested.Type == "check" && len(nested.Labels) == 0 {
			unlabeledCheckOrdinal++
		}
		if err := p.parseTableBlock(&table, fieldsStart, unlabeledCheckOrdinal, nested); err != nil {
			return err
		}
	}
	markPrimaryFields(p.db.Fields[fieldsStart:], table.PrimaryKey)
	if err := p.rejectUnsupportedTableAttrs(block); err != nil {
		return err
	}
	p.db.Tables = append(p.db.Tables, table)
	return nil
}

func hclTableStructName(schema, name string) string {
	schema = strings.TrimSpace(schema)
	switch strings.ToLower(schema) {
	case "", "main", "public":
		return tableref.Canonical("", name)
	default:
		return tableref.Canonical(schema, name)
	}
}

type tableLabels struct {
	schema string
	name   string
}

func (p *parser) tableLabels(block *hclsyntax.Block) (tableLabels, error) {
	schemaAttr := p.optionalRefName(block.Body.Attributes["schema"])
	switch len(block.Labels) {
	case 1:
		return tableLabels{schema: schemaAttr, name: block.Labels[0]}, nil
	case 2:
		if schemaAttr != "" && schemaAttr != block.Labels[0] {
			return tableLabels{}, p.blockError(block, "table %q schema label conflicts with schema attribute %q", block.Labels[1], schemaAttr)
		}
		return tableLabels{schema: block.Labels[0], name: block.Labels[1]}, nil
	default:
		return tableLabels{}, p.blockError(block, "table block requires one or two labels")
	}
}

func (p *parser) parseTableBlock(table *goschema.Table, fieldsStart, unlabeledCheckOrdinal int, block *hclsyntax.Block) error {
	switch block.Type {
	case "column":
		field, err := p.parseColumn(table.StructName, block)
		if err != nil {
			return err
		}
		p.db.Fields = append(p.db.Fields, field)
	case "primary_key":
		primaryKey, err := p.parsePrimaryKey(block)
		if err != nil {
			return err
		}
		table.PrimaryKey = primaryKey.columns
		table.PrimaryKeyParts = primaryKey.parts
		table.PrimaryKeyInclude = primaryKey.include
	case "index":
		index, err := p.parseIndex(table.StructName, table.Name, block)
		if err != nil {
			return err
		}
		index.TableName = table.QualifiedName()
		p.db.Indexes = append(p.db.Indexes, index)
	case "unique":
		constraint, err := p.parseUnique(table.StructName, table.Name, block)
		if err != nil {
			return err
		}
		constraint.Table = table.QualifiedName()
		p.db.Constraints = append(p.db.Constraints, constraint)
	case "foreign_key":
		spec, err := p.parseForeignKey(block)
		if err != nil {
			return err
		}
		if err := p.applyForeignKey(*table, fieldsStart, block, spec); err != nil {
			return err
		}
	case "check":
		constraint, err := p.parseCheck(table.StructName, table.Name, unlabeledCheckOrdinal, block)
		if err != nil {
			return err
		}
		constraint.Table = table.QualifiedName()
		p.db.Constraints = append(p.db.Constraints, constraint)
	case "partition":
		partition, err := p.parsePartition(block)
		if err != nil {
			return err
		}
		if table.Partition != nil {
			return p.blockError(block, "table %q has multiple partition blocks", table.Name)
		}
		table.Partition = partition
	case "row_security":
		rlsEnabled, err := p.parseRowSecurity(table, block)
		if err != nil {
			return err
		}
		p.db.RLSEnabledTables = append(p.db.RLSEnabledTables, rlsEnabled)
	default:
		return p.parseAdditionalTableBlock(table, block)
	}
	return nil
}

func (p *parser) parseAdditionalTableBlock(table *goschema.Table, block *hclsyntax.Block) error {
	switch block.Type {
	case "constraint":
		constraint, err := p.parseConstraint(table, block)
		if err != nil {
			return err
		}
		p.db.Constraints = append(p.db.Constraints, constraint)
	case "platform":
		// Parsed before the child walk so duplicate dialect/key pairs are
		// detected across all platform blocks on the table.
	default:
		return p.rejectUnsupportedBlock(block, "table")
	}
	return nil
}

func (p *parser) parseColumn(structName string, block *hclsyntax.Block) (goschema.Field, error) {
	if len(block.Labels) != 1 {
		return goschema.Field{}, p.blockError(block, "column block requires exactly one label")
	}
	name := block.Labels[0]
	typeAttr, ok := block.Body.Attributes["type"]
	if !ok {
		return goschema.Field{}, p.blockError(block, "column %q requires type", name)
	}
	if err := p.rejectUnsupportedColumnAttrs(block); err != nil {
		return goschema.Field{}, err
	}
	// Once, here, rather than in each of the two body walks below: they both
	// iterate the same blocks, so leaving the gate in their default arms would
	// report a dropped name twice.
	if err := p.rejectUnsupportedColumnBlocks(block); err != nil {
		return goschema.Field{}, err
	}
	generated, err := p.parseGeneratedColumn(block)
	if err != nil {
		return goschema.Field{}, err
	}
	identity, err := p.parseIdentityColumn(block)
	if err != nil {
		return goschema.Field{}, err
	}
	if generated.expression != "" && identity.generation != "" {
		return goschema.Field{}, p.blockError(block, "column cannot mix as and identity blocks")
	}
	overrides, err := p.parsePlatformOverrides(block, "column")
	if err != nil {
		return goschema.Field{}, err
	}
	enumValues, err := p.stringListAttr(block, "enum")
	if err != nil {
		return goschema.Field{}, err
	}

	field := goschema.Field{
		StructName:          structName,
		FieldName:           name,
		Name:                name,
		Type:                p.columnTypeName(block, typeAttr),
		Nullable:            p.optionalBool(block.Body.Attributes["null"], false),
		AutoInc:             p.optionalBool(block.Body.Attributes["auto_increment"], false) || identity.generation != "",
		IdentityGeneration:  identity.generation,
		IdentityStart:       identity.start,
		IdentityIncrement:   identity.increment,
		Unique:              p.optionalBool(block.Body.Attributes["unique"], false),
		GeneratedExpression: generated.expression,
		GeneratedKind:       generated.kind,
		UpdateExpression:    p.optionalSQLExpression(block.Body.Attributes["on_update"]),
		UniqueExpr:          p.optionalSQLExpression(block.Body.Attributes["unique_expr"]),
		Enum:                enumValues,
		Check:               p.optionalSQLExpression(block.Body.Attributes["check"]),
		CheckName:           p.optionalString(block.Body.Attributes["check_name"]),
		IdentityOptions:     identity.options,
		Charset:             p.optionalString(block.Body.Attributes["charset"]),
		Collate:             p.optionalString(block.Body.Attributes["collate"]),
		Comment:             p.optionalString(block.Body.Attributes["comment"]),
		Overrides:           overrides,
	}
	if attr := block.Body.Attributes["default"]; attr != nil {
		p.setDefault(&field, attr)
	}
	return field, nil
}

type generatedColumnSpec struct {
	expression string
	kind       string
}

func (p *parser) parseGeneratedColumn(block *hclsyntax.Block) (generatedColumnSpec, error) {
	attr := block.Body.Attributes["as"]
	var asBlocks []*hclsyntax.Block
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "as":
			asBlocks = append(asBlocks, nested)
		case "identity", "platform":
			continue
		default:
			// Already gated by rejectUnsupportedColumnBlocks.
			continue
		}
	}
	if attr != nil && len(asBlocks) > 0 {
		return generatedColumnSpec{}, p.blockError(asBlocks[0], "column cannot mix as attribute with as block")
	}
	if len(asBlocks) > 1 {
		return generatedColumnSpec{}, p.blockError(asBlocks[1], "column can contain at most one as block")
	}
	if attr != nil {
		return generatedColumnSpec{expression: p.exprString(attr)}, nil
	}
	if len(asBlocks) == 0 {
		return generatedColumnSpec{}, nil
	}

	asBlock := asBlocks[0]
	if err := p.rejectUnsupportedGeneratedColumnAttrs(asBlock); err != nil {
		return generatedColumnSpec{}, err
	}
	exprAttr := asBlock.Body.Attributes["expr"]
	if exprAttr == nil {
		return generatedColumnSpec{}, p.blockError(asBlock, "column as block requires expr")
	}
	return generatedColumnSpec{
		expression: p.exprString(exprAttr),
		kind:       strings.ToUpper(p.optionalString(asBlock.Body.Attributes["type"])),
	}, nil
}

type identityColumnSpec struct {
	generation string
	start      string
	increment  string
	options    string
}

func (p *parser) parseIdentityColumn(block *hclsyntax.Block) (identityColumnSpec, error) {
	var identityBlocks []*hclsyntax.Block
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "identity":
			identityBlocks = append(identityBlocks, nested)
		case "as", "platform":
			continue
		default:
			// Already gated by rejectUnsupportedColumnBlocks.
			continue
		}
	}
	if len(identityBlocks) == 0 {
		return identityColumnSpec{}, nil
	}
	if len(identityBlocks) > 1 {
		return identityColumnSpec{}, p.blockError(identityBlocks[1], "column can contain at most one identity block")
	}

	identityBlock := identityBlocks[0]
	if err := p.rejectUnsupportedIdentityColumnAttrs(identityBlock); err != nil {
		return identityColumnSpec{}, err
	}
	generated := p.optionalString(identityBlock.Body.Attributes["generated"])
	generation := normalizeIdentityGeneration(generated)
	if generation == "" {
		if generated != "" {
			return identityColumnSpec{}, p.blockError(identityBlock, "unsupported identity generated value %q", generated)
		}
		generation = "BY_DEFAULT"
	}
	return identityColumnSpec{
		generation: generation,
		start:      p.optionalString(identityBlock.Body.Attributes["start"]),
		increment:  p.optionalString(identityBlock.Body.Attributes["increment"]),
		options:    p.optionalString(identityBlock.Body.Attributes["options"]),
	}, nil
}

func (p *parser) parseIndex(structName, tableName string, block *hclsyntax.Block) (goschema.Index, error) {
	if len(block.Labels) != 1 {
		return goschema.Index{}, p.blockError(block, "index block requires exactly one label")
	}
	// Gated before the mix check so a dropped block name does not read as an
	// `on` block and turn tolerance into "cannot mix columns with on blocks".
	onBlocks, err := p.indexOnBlocks(block)
	if err != nil {
		return goschema.Index{}, err
	}
	if block.Body.Attributes["columns"] != nil && len(onBlocks) > 0 {
		return goschema.Index{}, p.blockError(onBlocks[0], "index cannot mix columns attribute with on blocks")
	}
	columns, err := p.parseColumnsAttr(block, "columns")
	if err != nil {
		return goschema.Index{}, err
	}
	include, err := p.parseColumnsAttr(block, "include")
	if err != nil {
		return goschema.Index{}, err
	}
	var parts []goschema.IndexPart
	if len(columns) == 0 {
		columns, parts, err = p.parseIndexParts(onBlocks)
		if err != nil {
			return goschema.Index{}, err
		}
	}
	if len(columns) == 0 {
		return goschema.Index{}, p.blockError(block, "index %q requires columns or parts", block.Labels[0])
	}
	if err := p.rejectUnsupportedIndexAttrs(block); err != nil {
		return goschema.Index{}, err
	}
	storageParams, err := p.parseIndexStorageParams(block)
	if err != nil {
		return goschema.Index{}, err
	}
	indexType := p.optionalString(block.Body.Attributes["type"])
	operator, err := p.stringAttr(block, "ops", "index")
	if err != nil {
		return goschema.Index{}, err
	}
	nullsDistinct, err := p.optionalBlockBoolPtr(block, "nulls_distinct", "index")
	if err != nil {
		return goschema.Index{}, err
	}
	parserName := p.optionalString(block.Body.Attributes["parser"])
	if parserName != "" && !strings.EqualFold(indexType, "FULLTEXT") {
		return goschema.Index{}, p.blockError(block, "index parser requires FULLTEXT type")
	}
	unique := p.optionalBool(block.Body.Attributes["unique"], false)
	if nullsDistinct != nil && !unique {
		return goschema.Index{}, p.blockError(block, "index nulls_distinct requires unique = true")
	}
	granularity, err := p.optionalGranularity(block)
	if err != nil {
		return goschema.Index{}, err
	}
	return goschema.Index{
		StructName:     structName,
		Name:           block.Labels[0],
		Fields:         columns,
		Parts:          parts,
		Unique:         unique,
		NullsDistinct:  nullsDistinct,
		Type:           indexType,
		Operator:       operator,
		Parser:         parserName,
		Condition:      p.optionalString(block.Body.Attributes["where"]),
		Comment:        p.optionalString(block.Body.Attributes["comment"]),
		IncludeColumns: include,
		StorageParams:  storageParams,
		Granularity:    granularity,
		TableName:      tableName,
	}, nil
}

func (p *parser) parseConstraint(table *goschema.Table, block *hclsyntax.Block) (goschema.Constraint, error) {
	if len(block.Labels) != 1 {
		return goschema.Constraint{}, p.blockError(block, "constraint block requires exactly one label")
	}
	if err := p.rejectNestedBlocks(block, "constraint"); err != nil {
		return goschema.Constraint{}, err
	}
	if err := p.rejectUnsupportedConstraintAttrs(block); err != nil {
		return goschema.Constraint{}, err
	}
	constraintType, err := p.stringAttr(block, "type", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	if constraintType == "" {
		return goschema.Constraint{}, p.blockError(block, "constraint %q requires type", block.Labels[0])
	}
	columns, err := p.stringListAttr(block, "columns")
	if err != nil {
		return goschema.Constraint{}, err
	}
	include, err := p.stringListAttr(block, "include")
	if err != nil {
		return goschema.Constraint{}, err
	}
	foreignColumns, err := p.stringListAttr(block, "foreign_columns")
	if err != nil {
		return goschema.Constraint{}, err
	}
	nullsDistinct, err := p.optionalBlockBoolPtr(block, "nulls_distinct", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	usingMethod, err := p.stringAttr(block, "using", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	excludeElements, err := p.stringAttr(block, "elements", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	whereCondition, err := p.stringAttr(block, "condition", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	checkExpression, err := p.stringAttr(block, "check", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	foreignTable, err := p.stringAttr(block, "foreign_table", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	onDelete, err := p.stringAttr(block, "on_delete", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	onUpdate, err := p.stringAttr(block, "on_update", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	comment, err := p.stringAttr(block, "comment", "constraint")
	if err != nil {
		return goschema.Constraint{}, err
	}
	return goschema.Constraint{
		StructName:      table.StructName,
		Name:            block.Labels[0],
		Type:            constraintType,
		Table:           table.QualifiedName(),
		UsingMethod:     usingMethod,
		ExcludeElements: excludeElements,
		WhereCondition:  whereCondition,
		CheckExpression: checkExpression,
		Columns:         columns,
		IncludeColumns:  include,
		NullsDistinct:   nullsDistinct,
		ForeignTable:    foreignTable,
		ForeignColumns:  foreignColumns,
		OnDelete:        onDelete,
		OnUpdate:        onUpdate,
		Comment:         comment,
	}, nil
}

func (p *parser) parsePlatformOverrides(block *hclsyntax.Block, owner string) (map[string]map[string]string, error) {
	overrides := make(map[string]map[string]string)
	for _, platformBlock := range block.Body.Blocks {
		if platformBlock.Type != "platform" {
			continue
		}
		if err := p.parsePlatformBlock(overrides, platformBlock, owner); err != nil {
			return nil, err
		}
	}
	if len(overrides) == 0 {
		return nil, nil
	}
	return overrides, nil
}

func (p *parser) parsePlatformBlock(
	overrides map[string]map[string]string,
	block *hclsyntax.Block,
	owner string,
) error {
	if len(block.Labels) != 1 || strings.TrimSpace(block.Labels[0]) == "" {
		return p.blockError(block, "%s platform block requires exactly one dialect label", owner)
	}
	for name := range block.Body.Attributes {
		return p.blockError(block, "unsupported %s platform attribute %q", owner, name)
	}
	dialect := block.Labels[0]
	if overrides[dialect] == nil {
		overrides[dialect] = make(map[string]string)
	}
	for _, overrideBlock := range block.Body.Blocks {
		if overrideBlock.Type != "override" {
			if err := p.rejectUnsupportedBlock(overrideBlock, owner+" platform"); err != nil {
				return err
			}
			continue
		}
		override, err := p.parsePlatformOverride(overrideBlock, owner)
		if err != nil {
			return err
		}
		if _, exists := overrides[dialect][override.key]; exists {
			return p.blockError(
				overrideBlock,
				"%s platform override %q for dialect %q is duplicated",
				owner,
				override.key,
				dialect,
			)
		}
		overrides[dialect][override.key] = override.value
	}
	return nil
}

type platformOverride struct {
	key   string
	value string
}

// parsePlatformOverride parses one `override` block. The caller has already
// gated any other block type through the unknown-name policy.
func (p *parser) parsePlatformOverride(block *hclsyntax.Block, owner string) (platformOverride, error) {
	if len(block.Labels) != 1 || strings.TrimSpace(block.Labels[0]) == "" {
		return platformOverride{}, p.blockError(block, "%s platform override requires exactly one key label", owner)
	}
	if err := p.rejectNestedBlocks(block, owner+" platform override"); err != nil {
		return platformOverride{}, err
	}
	if err := p.rejectUnsupportedAttrs(
		block,
		map[string]bool{"value": true},
		owner+" platform override",
	); err != nil {
		return platformOverride{}, err
	}
	if block.Body.Attributes["value"] == nil {
		return platformOverride{}, p.blockError(block, "%s platform override %q requires value", owner, block.Labels[0])
	}
	value, err := p.stringAttr(block, "value", owner+" platform override")
	if err != nil {
		return platformOverride{}, err
	}
	return platformOverride{key: block.Labels[0], value: value}, nil
}

// optionalGranularity reads the optional ClickHouse data-skipping index
// GRANULARITY value. An absent attribute yields 0, which the ClickHouse
// renderer treats as "use the dialect default". The value must be a
// non-negative integer within the int64 range, mirroring the Go-annotation
// path (parseIndexComment), which parses it with strconv.Atoi and rejects
// negatives; both frontends therefore accept the same granularity values.
func (p *parser) optionalGranularity(block *hclsyntax.Block) (int, error) {
	value, err := p.optionalInt64(block, "granularity", "index")
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}
	if *value < 0 {
		return 0, p.blockError(block, "index attribute %q must be a non-negative integer", "granularity")
	}
	return int(*value), nil
}

func (p *parser) parseUnique(structName, tableName string, block *hclsyntax.Block) (goschema.Constraint, error) {
	if len(block.Labels) != 1 {
		return goschema.Constraint{}, p.blockError(block, "unique block requires exactly one label")
	}
	if err := p.rejectUnsupportedUniqueAttrs(block); err != nil {
		return goschema.Constraint{}, err
	}
	columns, err := p.parseColumnsAttr(block, "columns")
	if err != nil {
		return goschema.Constraint{}, err
	}
	if len(columns) == 0 {
		return goschema.Constraint{}, p.blockError(block, "unique %q requires columns", block.Labels[0])
	}
	include, err := p.parseColumnsAttr(block, "include")
	if err != nil {
		return goschema.Constraint{}, err
	}
	nullsDistinct, err := p.optionalBlockBoolPtr(block, "nulls_distinct", "unique")
	if err != nil {
		return goschema.Constraint{}, err
	}
	return goschema.Constraint{
		StructName:     structName,
		Name:           block.Labels[0],
		Type:           "UNIQUE",
		Table:          tableName,
		Columns:        columns,
		IncludeColumns: include,
		NullsDistinct:  nullsDistinct,
	}, nil
}

func (p *parser) parseIndexStorageParams(block *hclsyntax.Block) (map[string]string, error) {
	pagePerRange := block.Body.Attributes["page_per_range"]
	pagesPerRange := block.Body.Attributes["pages_per_range"]
	if pagePerRange != nil && pagesPerRange != nil {
		return nil, p.blockError(block, "index cannot set both page_per_range and pages_per_range")
	}
	params := map[string]string{}
	if pagePerRange != nil {
		params["pages_per_range"] = p.exprString(pagePerRange)
	}
	if pagesPerRange != nil {
		params["pages_per_range"] = p.exprString(pagesPerRange)
	}
	if len(params) == 0 {
		return nil, nil
	}
	return params, nil
}

// indexOnBlocks returns the index body's `on` blocks, sending every other
// block type through the unknown-name gate.
func (p *parser) indexOnBlocks(block *hclsyntax.Block) ([]*hclsyntax.Block, error) {
	onBlocks := make([]*hclsyntax.Block, 0, len(block.Body.Blocks))
	for _, nested := range block.Body.Blocks {
		if nested.Type != "on" {
			if err := p.rejectUnsupportedBlock(nested, "index"); err != nil {
				return nil, err
			}
			continue
		}
		onBlocks = append(onBlocks, nested)
	}
	return onBlocks, nil
}

func (p *parser) parseIndexParts(onBlocks []*hclsyntax.Block) ([]string, []goschema.IndexPart, error) {
	var columns []string
	var parts []goschema.IndexPart
	for _, nested := range onBlocks {
		if err := p.rejectUnsupportedIndexOnAttrs(nested); err != nil {
			return nil, nil, err
		}
		columnAttr := nested.Body.Attributes["column"]
		exprAttr := nested.Body.Attributes["expr"]
		if columnAttr == nil && exprAttr == nil {
			return nil, nil, p.blockError(nested, "index on block requires column or expr")
		}
		if columnAttr != nil && exprAttr != nil {
			return nil, nil, p.blockError(nested, "index on block cannot set both column and expr")
		}
		desc, err := p.optionalIndexOnBool(nested, "desc", false)
		if err != nil {
			return nil, nil, err
		}
		operator := p.optionalSQLExpression(nested.Body.Attributes["ops"])
		prefix := p.optionalRawExpr(nested.Body.Attributes["prefix"])
		if columnAttr != nil {
			column := p.columnNameFromExpr(columnAttr)
			columns = append(columns, column)
			parts = append(parts, goschema.IndexPart{Name: column, Operator: operator, Prefix: prefix, Desc: desc})
			continue
		}
		if prefix != "" {
			return nil, nil, p.blockError(nested, "index on prefix requires column")
		}
		expr := p.exprString(exprAttr)
		columns = append(columns, expr)
		parts = append(parts, goschema.IndexPart{Expr: expr, Operator: operator, Desc: desc})
	}
	return columns, parts, nil
}

type primaryKeySpec struct {
	columns []string
	parts   []goschema.PrimaryKeyPart
	include []string
}

func (p *parser) parsePrimaryKey(block *hclsyntax.Block) (primaryKeySpec, error) {
	if err := p.rejectUnsupportedPrimaryKeyAttrs(block); err != nil {
		return primaryKeySpec{}, err
	}
	if err := p.validatePrimaryKeyType(block); err != nil {
		return primaryKeySpec{}, err
	}
	include, err := p.parseColumnsAttr(block, "include")
	if err != nil {
		return primaryKeySpec{}, err
	}
	if block.Body.Attributes["columns"] != nil {
		if len(block.Body.Blocks) > 0 {
			return primaryKeySpec{}, p.blockError(block.Body.Blocks[0], "primary_key cannot mix columns attribute with on blocks")
		}
		columns, err := p.parseColumnsAttr(block, "columns")
		if err != nil {
			return primaryKeySpec{}, err
		}
		return primaryKeySpec{columns: columns, parts: primaryKeyParts(columns), include: include}, nil
	}

	parts, err := p.parsePrimaryKeyParts(block)
	if err != nil {
		return primaryKeySpec{}, err
	}
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		columns = append(columns, part.Name)
	}
	return primaryKeySpec{columns: columns, parts: parts, include: include}, nil
}

func (p *parser) parsePartition(block *hclsyntax.Block) (*goschema.PartitionSpec, error) {
	if len(block.Labels) != 0 {
		return nil, p.blockError(block, "partition block does not accept labels")
	}
	if err := p.rejectUnsupportedPartitionAttrs(block); err != nil {
		return nil, err
	}
	partitionType := strings.ToUpper(p.optionalString(block.Body.Attributes["type"]))
	if partitionType == "" {
		return nil, p.blockError(block, "partition requires type")
	}
	// Gated before the mix check so a dropped block name does not read as a
	// `by` block and turn tolerance into "cannot mix columns with by blocks".
	byBlocks, err := p.partitionByBlocks(block)
	if err != nil {
		return nil, err
	}
	if block.Body.Attributes["columns"] != nil {
		if len(byBlocks) > 0 {
			return nil, p.blockError(byBlocks[0], "partition cannot mix columns attribute with by blocks")
		}
		columns, err := p.parseColumnsAttr(block, "columns")
		if err != nil {
			return nil, err
		}
		if len(columns) == 0 {
			return nil, p.blockError(block, "partition requires at least one column")
		}
		return &goschema.PartitionSpec{Type: partitionType, Parts: partitionColumnParts(columns)}, nil
	}

	parts, err := p.parsePartitionParts(block, byBlocks)
	if err != nil {
		return nil, err
	}
	return &goschema.PartitionSpec{Type: partitionType, Parts: parts}, nil
}

// partitionByBlocks returns the partition body's `by` blocks, sending every
// other block type through the unknown-name gate.
func (p *parser) partitionByBlocks(block *hclsyntax.Block) ([]*hclsyntax.Block, error) {
	byBlocks := make([]*hclsyntax.Block, 0, len(block.Body.Blocks))
	for _, nested := range block.Body.Blocks {
		if nested.Type != "by" {
			if err := p.rejectUnsupportedBlock(nested, "partition"); err != nil {
				return nil, err
			}
			continue
		}
		byBlocks = append(byBlocks, nested)
	}
	return byBlocks, nil
}

func (p *parser) parsePartitionParts(block *hclsyntax.Block, byBlocks []*hclsyntax.Block) ([]goschema.PartitionPart, error) {
	parts := make([]goschema.PartitionPart, 0, len(byBlocks))
	for _, nested := range byBlocks {
		if err := p.rejectUnsupportedPartitionByAttrs(nested); err != nil {
			return nil, err
		}
		columnAttr := nested.Body.Attributes["column"]
		exprAttr := nested.Body.Attributes["expr"]
		if columnAttr == nil && exprAttr == nil {
			return nil, p.blockError(nested, "partition by block requires column or expr")
		}
		if columnAttr != nil && exprAttr != nil {
			return nil, p.blockError(nested, "partition by block cannot set both column and expr")
		}
		if columnAttr != nil {
			parts = append(parts, goschema.PartitionPart{Name: p.columnNameFromExpr(columnAttr)})
			continue
		}
		parts = append(parts, goschema.PartitionPart{Expr: p.exprString(exprAttr)})
	}
	// Checked after the walk, not before it: under the unknown-name policy a
	// body holding nothing but dropped block names leaves no parts behind, and
	// that is the same structural gap as an empty body.
	if len(parts) == 0 {
		return nil, p.blockError(block, "partition requires columns attribute or by blocks")
	}
	return parts, nil
}

func partitionColumnParts(columns []string) []goschema.PartitionPart {
	parts := make([]goschema.PartitionPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, goschema.PartitionPart{Name: column})
	}
	return parts
}

func (p *parser) validatePrimaryKeyType(block *hclsyntax.Block) error {
	primaryKeyType := strings.ToUpper(p.optionalString(block.Body.Attributes["type"]))
	switch primaryKeyType {
	case "", "BTREE", "HASH":
		return nil
	default:
		return p.blockError(block, "unsupported primary_key type %q", primaryKeyType)
	}
}

func (p *parser) parsePrimaryKeyParts(block *hclsyntax.Block) ([]goschema.PrimaryKeyPart, error) {
	parts := make([]goschema.PrimaryKeyPart, 0, len(block.Body.Blocks))
	for _, nested := range block.Body.Blocks {
		if nested.Type != "on" {
			if err := p.rejectUnsupportedBlock(nested, "primary_key"); err != nil {
				return nil, err
			}
			continue
		}
		if err := p.rejectUnsupportedPrimaryKeyOnAttrs(nested); err != nil {
			return nil, err
		}
		attr := nested.Body.Attributes["column"]
		if attr == nil {
			return nil, p.blockError(nested, "primary_key on block requires column")
		}
		parts = append(parts, goschema.PrimaryKeyPart{
			Name:   p.columnNameFromExpr(attr),
			Prefix: p.optionalRawExpr(nested.Body.Attributes["prefix"]),
			Desc:   p.optionalBool(nested.Body.Attributes["desc"], false),
		})
	}
	// See parsePartitionParts: dropped block names leave no parts behind.
	if len(parts) == 0 {
		return nil, p.blockError(block, "primary_key requires columns attribute or on blocks")
	}
	return parts, nil
}

func primaryKeyParts(columns []string) []goschema.PrimaryKeyPart {
	parts := make([]goschema.PrimaryKeyPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, goschema.PrimaryKeyPart{Name: column})
	}
	return parts
}

type foreignKeySpec struct {
	name           string
	columns        []string
	foreignTable   string
	foreignColumns []string
	onDelete       string
	onUpdate       string
}

func (p *parser) parseForeignKey(block *hclsyntax.Block) (foreignKeySpec, error) {
	if len(block.Labels) != 1 {
		return foreignKeySpec{}, p.blockError(block, "foreign_key block requires exactly one label")
	}
	if err := p.rejectUnsupportedForeignKeyAttrs(block); err != nil {
		return foreignKeySpec{}, err
	}
	columns, err := p.parseColumnRefsAttr(block, "columns")
	if err != nil {
		return foreignKeySpec{}, err
	}
	refColumns, err := p.parseColumnRefsAttr(block, "ref_columns")
	if err != nil {
		return foreignKeySpec{}, err
	}
	if len(columns) == 0 || len(refColumns) == 0 {
		return foreignKeySpec{}, p.blockError(block, "foreign_key %q requires columns and ref_columns", block.Labels[0])
	}
	if len(columns) != len(refColumns) {
		return foreignKeySpec{}, p.blockError(block, "foreign_key %q requires matching columns and ref_columns counts", block.Labels[0])
	}

	localColumns := make([]string, 0, len(columns))
	foreignColumns := make([]string, 0, len(refColumns))
	foreignTable := refColumns[0].table
	for _, refColumn := range refColumns {
		if refColumn.table == "" || refColumn.column == "" {
			return foreignKeySpec{}, p.blockError(block, "foreign_key %q requires table-qualified ref_columns", block.Labels[0])
		}
		if refColumn.table != foreignTable {
			return foreignKeySpec{}, p.blockError(block, "foreign_key %q ref_columns must target one table", block.Labels[0])
		}
		foreignColumns = append(foreignColumns, refColumn.column)
	}
	for _, column := range columns {
		if column.column == "" {
			return foreignKeySpec{}, p.blockError(block, "foreign_key %q requires column refs", block.Labels[0])
		}
		localColumns = append(localColumns, column.column)
	}

	return foreignKeySpec{
		name:           block.Labels[0],
		columns:        localColumns,
		foreignTable:   foreignTable,
		foreignColumns: foreignColumns,
		onDelete:       p.optionalString(block.Body.Attributes["on_delete"]),
		onUpdate:       p.optionalString(block.Body.Attributes["on_update"]),
	}, nil
}

func (p *parser) applyForeignKey(table goschema.Table, fieldsStart int, block *hclsyntax.Block, spec foreignKeySpec) error {
	if err := p.requireForeignKeyLocalColumns(fieldsStart, block, spec); err != nil {
		return err
	}
	if len(spec.columns) > 1 {
		p.db.Constraints = append(p.db.Constraints, goschema.Constraint{
			StructName:     table.StructName,
			Name:           spec.name,
			Type:           "FOREIGN KEY",
			Table:          table.QualifiedName(),
			Columns:        spec.columns,
			ForeignTable:   spec.foreignTable,
			ForeignColumn:  spec.foreignColumns[0],
			ForeignColumns: spec.foreignColumns,
			OnDelete:       spec.onDelete,
			OnUpdate:       spec.onUpdate,
		})
		return nil
	}

	for i := fieldsStart; i < len(p.db.Fields); i++ {
		field := &p.db.Fields[i]
		if field.Name != spec.columns[0] {
			continue
		}
		field.Foreign = spec.foreignTable + "(" + spec.foreignColumns[0] + ")"
		field.ForeignKeyName = spec.name
		field.OnDelete = spec.onDelete
		field.OnUpdate = spec.onUpdate
		return nil
	}
	return nil
}

func (p *parser) requireForeignKeyLocalColumns(fieldsStart int, block *hclsyntax.Block, spec foreignKeySpec) error {
	seen := make(map[string]bool, len(spec.columns))
	for i := fieldsStart; i < len(p.db.Fields); i++ {
		seen[p.db.Fields[i].Name] = true
	}
	for _, column := range spec.columns {
		if !seen[column] {
			return p.blockError(block, "foreign_key %q references unknown local column %q", spec.name, column)
		}
	}
	return nil
}

func (p *parser) parseCheck(
	structName, tableName string,
	unlabeledOrdinal int,
	block *hclsyntax.Block,
) (goschema.Constraint, error) {
	if len(block.Labels) > 1 {
		return goschema.Constraint{}, p.blockError(block, "check block accepts at most one label")
	}
	if err := p.rejectUnsupportedCheckAttrs(block); err != nil {
		return goschema.Constraint{}, err
	}
	expr := p.optionalString(block.Body.Attributes["expr"])
	if expr == "" {
		return goschema.Constraint{}, p.blockError(block, "check requires expr")
	}
	name := tableName + "_check"
	if len(block.Labels) == 1 {
		name = block.Labels[0]
	} else if unlabeledOrdinal > 1 {
		name = fmt.Sprintf("%s_check_%d", tableName, unlabeledOrdinal)
	}
	return goschema.Constraint{
		StructName:      structName,
		Name:            name,
		Type:            "CHECK",
		Table:           tableName,
		CheckExpression: expr,
	}, nil
}

func (p *parser) parseColumnsAttr(block *hclsyntax.Block, attrName string) ([]string, error) {
	refs, err := p.parseColumnRefsAttr(block, attrName)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(refs))
	for i, ref := range refs {
		columns[i] = ref.column
	}
	return columns, nil
}

type columnRef struct {
	table  string
	column string
}

func (p *parser) parseColumnRefsAttr(block *hclsyntax.Block, attrName string) ([]columnRef, error) {
	attr := block.Body.Attributes[attrName]
	if attr == nil {
		return nil, nil
	}
	exprs := []hclsyntax.Expression{attr.Expr}
	if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok {
		exprs = tuple.Exprs
	}

	var refs []columnRef
	for _, expr := range exprs {
		item := p.rawExprNode(expr)
		table, column := tableColumnFromRef(item)
		if column == "" {
			return nil, p.blockError(block, "%s contains unsupported reference %q", attrName, item)
		}
		refs = append(refs, columnRef{table: table, column: column})
	}
	return refs, nil
}

func (p *parser) rejectUnsupportedSchemaBody(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "schema"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"comment": true,
		"charset": true,
		"collate": true,
	}, "schema")
}

func (p *parser) rejectUnsupportedEnumAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "enum"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema": true,
		"values": true,
	}, "enum")
}

func (p *parser) rejectUnsupportedTableAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":         true,
		"engine":         true,
		"auto_increment": true,
		"charset":        true,
		"collate":        true,
		"strict":         true,
		"without_rowid":  true,
		"comment":        true,
		"checks":         true,
		"custom":         true,
	}, "table")
}

func (p *parser) rejectUnsupportedPrimaryKeyAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"columns": true,
		"include": true,
		"type":    true,
	}, "primary_key")
}

func (p *parser) rejectUnsupportedPrimaryKeyOnAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"column": true,
		"prefix": true,
		"desc":   true,
	}, "primary_key on")
}

func (p *parser) rejectUnsupportedPartitionAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"type":    true,
		"columns": true,
	}, "partition")
}

func (p *parser) rejectUnsupportedPartitionByAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"column": true,
		"expr":   true,
	}, "partition by")
}

func (p *parser) rejectUnsupportedColumnAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"type":           true,
		"null":           true,
		"auto_increment": true,
		"unique":         true,
		"unique_expr":    true,
		"default":        true,
		"on_update":      true,
		"check":          true,
		"check_name":     true,
		"enum":           true,
		"as":             true,
		"charset":        true,
		"collate":        true,
		"comment":        true,
		"unsigned":       true,
	}, "column")
}

func (p *parser) rejectUnsupportedGeneratedColumnAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "column as"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"expr": true,
		"type": true,
	}, "column as")
}

func (p *parser) rejectUnsupportedIdentityColumnAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "column identity"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"generated": true,
		"start":     true,
		"increment": true,
		"options":   true,
	}, "column identity")
}

func (p *parser) rejectUnsupportedIndexAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"columns":         true,
		"include":         true,
		"parser":          true,
		"page_per_range":  true,
		"pages_per_range": true,
		"nulls_distinct":  true,
		"unique":          true,
		"type":            true,
		"where":           true,
		"comment":         true,
		"granularity":     true,
		"ops":             true,
	}, "index")
}

func (p *parser) rejectUnsupportedConstraintAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"type":            true,
		"using":           true,
		"elements":        true,
		"condition":       true,
		"check":           true,
		"columns":         true,
		"include":         true,
		"nulls_distinct":  true,
		"foreign_table":   true,
		"foreign_columns": true,
		"on_delete":       true,
		"on_update":       true,
		"comment":         true,
	}, "constraint")
}

func (p *parser) rejectUnsupportedUniqueAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"columns":        true,
		"include":        true,
		"nulls_distinct": true,
	}, "unique")
}

func (p *parser) rejectUnsupportedIndexOnAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"column": true,
		"expr":   true,
		"ops":    true,
		"prefix": true,
		"desc":   true,
	}, "index on")
}

func (p *parser) rejectUnsupportedForeignKeyAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"columns":     true,
		"ref_columns": true,
		"on_delete":   true,
		"on_update":   true,
	}, "foreign_key")
}

func (p *parser) rejectUnsupportedCheckAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"expr": true,
	}, "check")
}

// rejectUnsupportedAttrs is the single gate every attribute allow-list in this
// package goes through, so the unknown-name policy is one branch rather than a
// copy per construct.
//
// The tolerance covers every scope routed through here, not a shortlist. The
// community binary drops an unmodeled attribute in each of them -- measured
// with a nonsense control in the column, table, index, schema, primary_key,
// foreign_key, check, enum and view positions, each time comparing the emitted
// DDL against the same schema with the attribute deleted and getting an exact
// match at exit 0. A shortlist would leave a real ent- or gqlgen-authored file
// refused in the positions that were left out, which is the defect
// stokaro/ptah#1016 describes.
func (p *parser) rejectUnsupportedAttrs(block *hclsyntax.Block, supported map[string]bool, label string) error {
	// Sorted so both the tolerated and the refused path pick the same
	// attribute out of a body carrying several unknown names; map order would
	// otherwise make the reported error non-deterministic.
	for _, name := range slices.Sorted(maps.Keys(block.Body.Attributes)) {
		if supported[name] {
			continue
		}
		if !p.tolerant {
			return p.blockError(block, "unsupported %s attribute %q", label, name)
		}
		if err := p.tolerateUnknownAttr(label, block.Body.Attributes[name]); err != nil {
			return err
		}
	}
	return nil
}

// rejectUnsupportedColumnBlocks gates the nested blocks a column body may
// carry. `as`, `identity` and `platform` are modeled; anything else goes
// through the unknown-name gate.
func (p *parser) rejectUnsupportedColumnBlocks(block *hclsyntax.Block) error {
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "as", "identity", "platform":
			continue
		default:
			if err := p.rejectUnsupportedBlock(nested, "column"); err != nil {
				return err
			}
		}
	}
	return nil
}

// rejectUnsupportedBlock is the block-side counterpart of
// rejectUnsupportedAttrs: one gate, so a construct that grows a nested-block
// allow-list cannot quietly get its own policy.
func (p *parser) rejectUnsupportedBlock(block *hclsyntax.Block, label string) error {
	if p.tolerant {
		return p.tolerateUnknownBlock(label, block)
	}
	return p.blockError(block, "unsupported %s block %q", label, block.Type)
}

// rejectNestedBlocks applies that gate to every block nested in a construct
// that models no nested blocks at all.
//
// Every one of them is checked, not just the first: under the tolerance the
// first is dropped rather than fatal, so stopping there would leave the rest
// of the subtree unevaluated.
func (p *parser) rejectNestedBlocks(block *hclsyntax.Block, label string) error {
	for _, nested := range block.Body.Blocks {
		if err := p.rejectUnsupportedBlock(nested, label); err != nil {
			return err
		}
	}
	return nil
}

func markPrimaryFields(fields []goschema.Field, columns []string) {
	if len(columns) != 1 {
		return
	}
	for i := range fields {
		if fields[i].Name == columns[0] {
			fields[i].Primary = true
			fields[i].Nullable = false
			return
		}
	}
}

func (p *parser) setDefault(field *goschema.Field, attr *hclsyntax.Attribute) {
	if value, ok := p.sqlExpression(attr); ok {
		field.DefaultExpr = value
		return
	}
	field.Default = p.exprString(attr)
	field.DefaultSet = true
}

func (p *parser) optionalRefName(attr *hclsyntax.Attribute) string {
	if attr == nil {
		return ""
	}
	return refName(p.rawExpr(attr))
}

func (p *parser) optionalString(attr *hclsyntax.Attribute) string {
	if attr == nil {
		return ""
	}
	return p.exprString(attr)
}

func (p *parser) stringAttr(block *hclsyntax.Block, name, label string) (string, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return "", nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() || value.Type() != cty.String {
		return "", p.blockError(block, "%s attribute %q must be a string", label, name)
	}
	return value.AsString(), nil
}

func (p *parser) optionalBool(attr *hclsyntax.Attribute, fallback bool) bool {
	if attr == nil {
		return fallback
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return fallback
	}
	return value.True()
}

func (p *parser) optionalTableBool(block *hclsyntax.Block, name string, fallback bool) (bool, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return fallback, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return false, p.blockError(block, "table attribute %q must be a bool", name)
	}
	return value.True(), nil
}

func (p *parser) optionalIndexOnBool(block *hclsyntax.Block, name string, fallback bool) (bool, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return fallback, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return false, p.blockError(block, "index on attribute %q must be a bool", name)
	}
	return value.True(), nil
}

func (p *parser) optionalBlockBoolPtr(block *hclsyntax.Block, name, label string) (*bool, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return nil, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return nil, p.blockError(block, "%s attribute %q must be a bool", label, name)
	}
	result := value.True()
	return &result, nil
}

func (p *parser) optionalRawExpr(attr *hclsyntax.Attribute) string {
	if attr == nil {
		return ""
	}
	return p.rawExpr(attr)
}

func (p *parser) optionalSQLExpression(attr *hclsyntax.Attribute) string {
	if attr == nil {
		return ""
	}
	if value, ok := p.sqlExpression(attr); ok {
		return value
	}
	return p.exprString(attr)
}

func (p *parser) sqlExpression(attr *hclsyntax.Attribute) (string, bool) {
	raw := p.rawExpr(attr)
	if value, ok := strings.CutPrefix(raw, "sql("); ok && strings.HasSuffix(value, ")") {
		value = strings.TrimSuffix(value, ")")
		if unquoted, err := strconv.Unquote(value); err == nil {
			return unquoted, true
		}
		return value, true
	}
	return "", false
}

func (p *parser) exprString(attr *hclsyntax.Attribute) string {
	value, diags := attr.Expr.Value(nil)
	if !diags.HasErrors() && value.Type() == cty.String {
		return value.AsString()
	}
	return p.rawExpr(attr)
}

func (p *parser) columnTypeName(block *hclsyntax.Block, attr *hclsyntax.Attribute) string {
	rawType := p.rawExpr(attr)
	if enumName, ok := strings.CutPrefix(rawType, "enum."); ok {
		return enumName
	}
	typ := p.exprString(attr)
	if p.optionalBool(block.Body.Attributes["unsigned"], false) && !strings.Contains(strings.ToLower(typ), "unsigned") {
		return typ + " unsigned"
	}
	return typ
}

func (p *parser) stringListAttr(block *hclsyntax.Block, attrName string) ([]string, error) {
	attr := block.Body.Attributes[attrName]
	if attr == nil {
		return nil, nil
	}
	value, diags := attr.Expr.Value(nil)
	valueType := value.Type()
	if diags.HasErrors() || !valueType.IsTupleType() && !valueType.IsListType() {
		return nil, p.blockError(block, "%s must be a list of strings", attrName)
	}
	values := make([]string, 0, value.LengthInt())
	it := value.ElementIterator()
	for it.Next() {
		_, item := it.Element()
		if !item.IsKnown() || item.IsNull() || item.Type() != cty.String {
			return nil, p.blockError(block, "%s must be a list of strings", attrName)
		}
		values = append(values, item.AsString())
	}
	return values, nil
}

func (p *parser) columnNameFromExpr(attr *hclsyntax.Attribute) string {
	return columnNameFromRef(p.rawExpr(attr))
}

func (p *parser) rawExpr(attr *hclsyntax.Attribute) string {
	return strings.TrimSpace(string(attr.Expr.Range().SliceBytes(p.src)))
}

func (p *parser) rawExprNode(expr hclsyntax.Expression) string {
	return strings.TrimSpace(string(expr.Range().SliceBytes(p.src)))
}

func (p *parser) blockError(block *hclsyntax.Block, format string, args ...any) error {
	return fmt.Errorf("parse HCL schema at %s: %s", block.TypeRange.String(), fmt.Sprintf(format, args...))
}

func refName(raw string) string {
	raw = strings.TrimSpace(raw)
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return unquoted
	}
	if name, ok := traversalObjectRefName(raw, "schema"); ok {
		if ref, parsed := tableref.Parse(name); parsed && !ref.Qualified {
			return ref.Name
		}
	}
	if suffix, ok := strings.CutPrefix(raw, "schema."); ok {
		return suffix
	}
	return raw
}

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

func columnNameFromRef(raw string) string {
	_, column := tableColumnFromRef(raw)
	return column
}

func tableColumnFromRef(raw string) (table string, column string) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimSuffix(raw, ",")
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return "", unquoted
	}
	expr, diags := hclsyntax.ParseExpression([]byte(raw), "column-reference.hcl", hcl.InitialPos)
	if diags.HasErrors() {
		return "", ""
	}
	traversal, diags := hcl.AbsTraversalForExpr(expr)
	if diags.HasErrors() || len(traversal) < 2 {
		return "", ""
	}
	root, ok := traversal[0].(hcl.TraverseRoot)
	if !ok {
		return "", ""
	}
	parts := make([]string, 0, len(traversal)-1)
	for _, step := range traversal[1:] {
		part, ok := traversalPart(step)
		if !ok {
			return "", ""
		}
		parts = append(parts, part)
	}
	if root.Name == "column" && len(parts) == 1 {
		return "", parts[0]
	}
	if root.Name != "table" || len(parts) < 3 || parts[len(parts)-2] != "column" {
		return "", ""
	}
	tableParts := parts[:len(parts)-2]
	if len(tableParts) == 1 {
		return tableref.Canonical("", tableParts[0]), parts[len(parts)-1]
	}
	if len(tableParts) == 2 {
		return tableref.Canonical(tableParts[0], tableParts[1]), parts[len(parts)-1]
	}
	return "", ""
}
