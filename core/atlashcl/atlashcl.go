// Package atlashcl parses Atlas schema HCL into Ptah's schema IR.
package atlashcl

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"github.com/stokaro/ptah/core/goschema"
)

// ParseFile parses an Atlas schema HCL file into the same Database IR used by
// Go annotations and YAML schema files.
func ParseFile(path string) (*goschema.Database, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Atlas HCL schema file: %w", err)
	}

	return Parse(data, path)
}

// Parse parses Atlas schema HCL into the same Database IR used by Go
// annotations and YAML schema files.
func Parse(data []byte, filename string) (*goschema.Database, error) {
	if filename == "" {
		filename = "schema.hcl"
	}
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse Atlas HCL schema: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse Atlas HCL schema: unsupported body type %T", file.Body)
	}

	p := parser{src: data, db: &goschema.Database{}}
	if err := p.parseBody(body); err != nil {
		return nil, err
	}
	goschema.Finalize(p.db)
	return p.db, nil
}

type parser struct {
	src []byte
	db  *goschema.Database
}

func (p *parser) parseBody(body *hclsyntax.Body) error {
	for _, block := range body.Blocks {
		switch block.Type {
		case "schema":
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
		case "enum":
			if err := p.parseEnum(block); err != nil {
				return err
			}
		case "table":
			if err := p.parseTable(block); err != nil {
				return err
			}
		case "env", "variable":
			// Project-level Atlas HCL can carry env/variable blocks next to schema
			// blocks. They do not define schema objects directly.
		default:
			return p.blockError(block, "unsupported top-level block %q", block.Type)
		}
	}
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
	if len(block.Labels) != 1 {
		return p.blockError(block, "table block requires exactly one label")
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
		StructName:    block.Labels[0],
		Name:          block.Labels[0],
		Schema:        p.optionalRefName(block.Body.Attributes["schema"]),
		Engine:        p.optionalString(block.Body.Attributes["engine"]),
		AutoIncrement: p.optionalString(block.Body.Attributes["auto_increment"]),
		Charset:       p.optionalString(block.Body.Attributes["charset"]),
		Collate:       p.optionalString(block.Body.Attributes["collate"]),
		Strict:        strict,
		WithoutRowID:  withoutRowID,
		Comment:       p.optionalString(block.Body.Attributes["comment"]),
	}

	fieldsStart := len(p.db.Fields)
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "column":
			field, err := p.parseColumn(table.StructName, nested)
			if err != nil {
				return err
			}
			p.db.Fields = append(p.db.Fields, field)
		case "primary_key":
			primaryKey, err := p.parsePrimaryKey(nested)
			if err != nil {
				return err
			}
			table.PrimaryKey = primaryKey.columns
			table.PrimaryKeyParts = primaryKey.parts
			table.PrimaryKeyInclude = primaryKey.include
		case "index":
			index, err := p.parseIndex(table.StructName, table.Name, nested)
			if err != nil {
				return err
			}
			p.db.Indexes = append(p.db.Indexes, index)
		case "unique":
			constraint, err := p.parseUnique(table.StructName, table.Name, nested)
			if err != nil {
				return err
			}
			p.db.Constraints = append(p.db.Constraints, constraint)
		case "foreign_key":
			spec, err := p.parseForeignKey(nested)
			if err != nil {
				return err
			}
			if err := p.applyForeignKey(table, fieldsStart, nested, spec); err != nil {
				return err
			}
		case "check":
			constraint, err := p.parseCheck(table.StructName, table.Name, nested)
			if err != nil {
				return err
			}
			p.db.Constraints = append(p.db.Constraints, constraint)
		default:
			return p.blockError(nested, "unsupported table block %q", nested.Type)
		}
	}
	markPrimaryFields(p.db.Fields[fieldsStart:], table.PrimaryKey)
	if err := p.rejectUnsupportedTableAttrs(block); err != nil {
		return err
	}
	p.db.Tables = append(p.db.Tables, table)
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

	field := goschema.Field{
		StructName:          structName,
		FieldName:           name,
		Name:                name,
		Type:                p.columnTypeName(typeAttr),
		Nullable:            p.optionalBool(block.Body.Attributes["null"], false),
		AutoInc:             p.optionalBool(block.Body.Attributes["auto_increment"], false) || identity.generation != "",
		IdentityGeneration:  identity.generation,
		IdentityStart:       identity.start,
		IdentityIncrement:   identity.increment,
		Unique:              p.optionalBool(block.Body.Attributes["unique"], false),
		GeneratedExpression: generated.expression,
		GeneratedKind:       generated.kind,
		UpdateExpression:    p.optionalSQLExpression(block.Body.Attributes["on_update"]),
		Charset:             p.optionalString(block.Body.Attributes["charset"]),
		Collate:             p.optionalString(block.Body.Attributes["collate"]),
		Comment:             p.optionalString(block.Body.Attributes["comment"]),
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
		case "identity":
			continue
		default:
			return generatedColumnSpec{}, p.blockError(nested, "unsupported column block %q", nested.Type)
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
}

func (p *parser) parseIdentityColumn(block *hclsyntax.Block) (identityColumnSpec, error) {
	var identityBlocks []*hclsyntax.Block
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "identity":
			identityBlocks = append(identityBlocks, nested)
		case "as":
			continue
		default:
			return identityColumnSpec{}, p.blockError(nested, "unsupported column block %q", nested.Type)
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
	}, nil
}

func (p *parser) parseIndex(structName, tableName string, block *hclsyntax.Block) (goschema.Index, error) {
	if len(block.Labels) != 1 {
		return goschema.Index{}, p.blockError(block, "index block requires exactly one label")
	}
	if block.Body.Attributes["columns"] != nil && len(block.Body.Blocks) > 0 {
		return goschema.Index{}, p.blockError(block.Body.Blocks[0], "index cannot mix columns attribute with on blocks")
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
		columns, parts, err = p.parseIndexParts(block)
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
	return goschema.Index{
		StructName:     structName,
		Name:           block.Labels[0],
		Fields:         columns,
		Parts:          parts,
		Unique:         unique,
		NullsDistinct:  nullsDistinct,
		Type:           indexType,
		Parser:         parserName,
		Condition:      p.optionalString(block.Body.Attributes["where"]),
		IncludeColumns: include,
		StorageParams:  storageParams,
		TableName:      tableName,
	}, nil
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
	nullsDistinct, err := p.optionalBlockBoolPtr(block, "nulls_distinct", "unique")
	if err != nil {
		return goschema.Constraint{}, err
	}
	return goschema.Constraint{
		StructName:    structName,
		Name:          block.Labels[0],
		Type:          "UNIQUE",
		Table:         tableName,
		Columns:       columns,
		NullsDistinct: nullsDistinct,
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

func (p *parser) parseIndexParts(block *hclsyntax.Block) ([]string, []goschema.IndexPart, error) {
	var columns []string
	var parts []goschema.IndexPart
	for _, nested := range block.Body.Blocks {
		if nested.Type != "on" {
			return nil, nil, p.blockError(nested, "unsupported index block %q", nested.Type)
		}
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
	if len(block.Body.Blocks) == 0 {
		return nil, p.blockError(block, "primary_key requires columns attribute or on blocks")
	}
	parts := make([]goschema.PrimaryKeyPart, 0, len(block.Body.Blocks))
	for _, nested := range block.Body.Blocks {
		if nested.Type != "on" {
			return nil, p.blockError(nested, "unsupported primary_key block %q", nested.Type)
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
			Table:          table.Name,
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

func (p *parser) parseCheck(structName, tableName string, block *hclsyntax.Block) (goschema.Constraint, error) {
	if len(block.Labels) != 1 {
		return goschema.Constraint{}, p.blockError(block, "check block requires exactly one label")
	}
	if err := p.rejectUnsupportedCheckAttrs(block); err != nil {
		return goschema.Constraint{}, err
	}
	expr := p.optionalString(block.Body.Attributes["expr"])
	if expr == "" {
		return goschema.Constraint{}, p.blockError(block, "check %q requires expr", block.Labels[0])
	}
	return goschema.Constraint{
		StructName:      structName,
		Name:            block.Labels[0],
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
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported schema block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"comment": true,
		"charset": true,
		"collate": true,
	}, "schema")
}

func (p *parser) rejectUnsupportedEnumAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported enum block %q", block.Body.Blocks[0].Type)
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

func (p *parser) rejectUnsupportedColumnAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"type":           true,
		"null":           true,
		"auto_increment": true,
		"unique":         true,
		"default":        true,
		"on_update":      true,
		"as":             true,
		"charset":        true,
		"collate":        true,
		"comment":        true,
	}, "column")
}

func (p *parser) rejectUnsupportedGeneratedColumnAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported column as block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"expr": true,
		"type": true,
	}, "column as")
}

func (p *parser) rejectUnsupportedIdentityColumnAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported column identity block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"generated": true,
		"start":     true,
		"increment": true,
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
	}, "index")
}

func (p *parser) rejectUnsupportedUniqueAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"columns":        true,
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

func (p *parser) rejectUnsupportedAttrs(block *hclsyntax.Block, supported map[string]bool, label string) error {
	for name := range block.Body.Attributes {
		if !supported[name] {
			return p.blockError(block, "unsupported %s attribute %q", label, name)
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

func (p *parser) columnTypeName(attr *hclsyntax.Attribute) string {
	typ := p.rawExpr(attr)
	if enumName, ok := strings.CutPrefix(typ, "enum."); ok {
		return enumName
	}
	return typ
}

func (p *parser) stringListAttr(block *hclsyntax.Block, attrName string) ([]string, error) {
	attr := block.Body.Attributes[attrName]
	if attr == nil {
		return nil, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || !value.CanIterateElements() {
		return nil, p.blockError(block, "%s must be a list of strings", attrName)
	}
	values := make([]string, 0, value.LengthInt())
	it := value.ElementIterator()
	for it.Next() {
		_, item := it.Element()
		if item.Type() != cty.String {
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
	return fmt.Errorf("parse Atlas HCL schema at %s: %s", block.TypeRange.String(), fmt.Sprintf(format, args...))
}

func refName(raw string) string {
	raw = strings.TrimSpace(raw)
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return unquoted
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
	if strings.Contains(raw, "[") {
		return bracketRefParts(raw)
	}
	parts := strings.Split(raw, ".")
	if len(parts) == 0 {
		return "", ""
	}
	column = parts[len(parts)-1]
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] == "table" && parts[i+2] == "column" {
			table = parts[i+1]
			break
		}
	}
	return table, column
}

func bracketRefParts(raw string) (table string, column string) {
	if start := strings.LastIndex(raw, "["); start >= 0 {
		if end := strings.LastIndex(raw, "]"); end > start {
			value := raw[start+1 : end]
			column, _ = strconv.Unquote(value)
		}
	}
	prefix, _, _ := strings.Cut(raw, ".column[")
	parts := strings.Split(prefix, ".")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "table" {
			table = parts[i+1]
			break
		}
	}
	return table, column
}
