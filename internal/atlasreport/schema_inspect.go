package atlasreport

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/template"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/schemaviz"
)

type SchemaInspectReport struct {
	db          *goschema.Database
	info        dbschematypes.DBInfo
	diagnostics io.Writer
	// omitAtlasRefusedBlocks renders HCL for the Atlas-compatible surface,
	// which leaves out the block types the pinned Atlas community binary
	// refuses to read where nothing in the document names them; see
	// [go.5x5.cz/ptah/internal/atlashclrender.RenderInspectedForAtlasCLI].
	omitAtlasRefusedBlocks bool
	Realm                  atlasSchemaInspectJSONRealm `json:"-"`
	Schema                 atlasSchemaInspectJSONRealm `json:"-"`
}

type atlasSchemaInspectJSONRealm struct {
	Schemas []atlasSchemaInspectJSONSchema `json:"schemas,omitempty"`
}

// atlasSchemaInspectJSONSchema and atlasSchemaInspectJSONTable both carry their
// attributes AFTER their children, because that is where the pinned community
// binary v1.3.0 puts them. Measured on PostgreSQL 17 and MySQL 9.7:
//
//	{"name":"public","tables":[…],"comment":"standard public schema"}
//	{"name":"t","columns":[…],"comment":"table comment"}
//	{"name":"adv_dev","tables":[…],"charset":"utf8mb4","collate":"utf8mb4_0900_ai_ci"}
//
// Go emits object keys in field order, embedded fields included, so the field
// order here is the byte order of the document a consumer diffs.
type atlasSchemaInspectJSONSchema struct {
	Name   string                        `json:"name"`
	Tables []atlasSchemaInspectJSONTable `json:"tables,omitempty"`
	atlasSchemaInspectJSONAttrs
}

type atlasSchemaInspectJSONTable struct {
	Name        string                             `json:"name"`
	Columns     []atlasSchemaInspectJSONColumn     `json:"columns,omitempty"`
	Indexes     []atlasSchemaInspectJSONIndex      `json:"indexes,omitempty"`
	PrimaryKey  *atlasSchemaInspectJSONIndex       `json:"primary_key,omitempty"`
	ForeignKeys []atlasSchemaInspectJSONForeignKey `json:"foreign_keys,omitempty"`
	atlasSchemaInspectJSONAttrs
}

type atlasSchemaInspectJSONAttrs struct {
	Comment string `json:"comment,omitempty"`
	Charset string `json:"charset,omitempty"`
	Collate string `json:"collate,omitempty"`
}

type atlasSchemaInspectJSONColumn struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
	Null bool   `json:"null,omitempty"`
	atlasSchemaInspectJSONAttrs
}

type atlasSchemaInspectJSONIndex struct {
	Name   string                            `json:"name,omitempty"`
	Unique bool                              `json:"unique,omitempty"`
	Parts  []atlasSchemaInspectJSONIndexPart `json:"parts,omitempty"`
}

type atlasSchemaInspectJSONIndexPart struct {
	Desc   bool   `json:"desc,omitempty"`
	Column string `json:"column,omitempty"`
	Expr   string `json:"expr,omitempty"`
}

type atlasSchemaInspectJSONForeignKey struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns,omitempty"`
	References struct {
		Table   string   `json:"table"`
		Columns []string `json:"columns,omitempty"`
	} `json:"references"`
}

// SchemaInspectFile is one output file planned by a split/write --format
// template. Path is slash-separated and relative to Dir; Dir is the output
// directory exactly as the template's write call named it.
type SchemaInspectFile struct {
	Dir  string
	Path string
	Data string
}

// SchemaInspectOutput is the explicit result of rendering one schema inspect
// format. Every inspect format — HCL, SQL, JSON, custom templates, Mermaid
// helpers, and split/write exports — flows through this one model: Text is
// what the CLI prints, Files are the write calls the template planned.
// Rendering is pure; the caller decides whether and how to apply Files.
type SchemaInspectOutput struct {
	Text  string
	Files []SchemaInspectFile
}

// RenderSchemaInspect executes the Atlas schema inspect --format template and
// returns the rendered text together with the planned file writes. It never
// touches the filesystem.
func RenderSchemaInspect(format string, report *SchemaInspectReport) (SchemaInspectOutput, error) {
	var files []SchemaInspectFile
	tmpl, err := newAtlasSchemaInspectTemplate("atlas-schema-inspect-format", format, report, &files)
	if err != nil {
		return SchemaInspectOutput{}, err
	}
	var out strings.Builder
	if err := tmpl.Execute(&out, report); err != nil {
		return SchemaInspectOutput{}, fmt.Errorf("execute --format template: %w", err)
	}
	return SchemaInspectOutput{Text: out.String(), Files: files}, nil
}

// NewSchemaInspectReport builds the report one schema inspect renders from.
//
// omitAtlasRefusedBlocks selects the Atlas-compatible HCL rendering, which
// leaves out the block types the pinned Atlas community binary refuses where
// nothing else in the document names them, and reports every decision on
// diagnostics. It is false on the native surface, which describes every
// construct Ptah models.
func NewSchemaInspectReport(
	db *goschema.Database,
	schema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	diagnostics io.Writer,
	omitAtlasRefusedBlocks bool,
) *SchemaInspectReport {
	realm := atlasSchemaInspectJSON(schema, info)
	return &SchemaInspectReport{
		db:                     db,
		info:                   info,
		diagnostics:            diagnostics,
		omitAtlasRefusedBlocks: omitAtlasRefusedBlocks,
		Realm:                  realm,
		Schema:                 realm,
	}
}

func ValidateSchemaInspectTemplate(format string) error {
	var files []SchemaInspectFile
	_, err := newAtlasSchemaInspectTemplate("atlas-schema-inspect-format", format, nil, &files)
	return err
}

func NormalizeSchemaInspectFormat(format string) (string, error) {
	trimmed := strings.TrimSpace(format)
	if trimmed == "" || trimmed == "hcl" {
		return "{{ $.MarshalHCL }}", nil
	}
	if trimmed == "sql" {
		return "{{ sql . }}", nil
	}
	if trimmed == "json" {
		return "{{ json . }}", nil
	}
	return format, nil
}

// newAtlasSchemaInspectTemplate parses the --format template with split/write
// bound to this render: split groups against the report's default schema and
// write appends planned files to files instead of touching the filesystem.
// report may be nil for parse-only validation.
func newAtlasSchemaInspectTemplate(
	name string,
	format string,
	report *SchemaInspectReport,
	files *[]SchemaInspectFile,
) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(template.FuncMap{
		"base64url": atlasSchemaInspectBase64URL,
		"hcl":       atlasSchemaInspectHCL,
		"json":      atlasTemplateJSON,
		"mermaid":   atlasSchemaInspectMermaid,
		"sql":       atlasSchemaInspectSQL,
		"split": func(args ...any) (schemaInspectArchive, error) {
			return atlasSchemaInspectSplit(report.defaultSchemaName(), args...)
		},
		"write": func(args ...any) (string, error) {
			return atlasSchemaInspectWrite(files, args...)
		},
	}).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
}

// defaultSchemaName resolves the schema that owns unqualified objects for
// split grouping. It is nil-safe so template validation can parse formats
// without a report.
func (r *SchemaInspectReport) defaultSchemaName() string {
	if r == nil {
		return ""
	}
	return atlasSchemaInspectSchemaName("", r.info)
}

func (r *SchemaInspectReport) MarshalHCL() (string, error) {
	rendered, err := r.renderHCL()
	if err != nil {
		return "", fmt.Errorf("render HCL schema: %w", err)
	}
	if r.diagnostics != nil {
		for _, diagnostic := range rendered.Diagnostics {
			fmt.Fprintf(r.diagnostics, "%s: %s: %s\n", diagnostic.Severity, diagnostic.Path, diagnostic.Message)
		}
	}
	return string(rendered.Data), nil
}

// renderHCL picks the rendering the surface asked for. Only the HCL output is
// split this way: what the compatibility surface omits, it omits because the
// binary it stands in for cannot PARSE the block, and that is a question only
// the HCL document raises. SQL output is read by a database.
func (r *SchemaInspectReport) renderHCL() (atlashclrender.Result, error) {
	if r.omitAtlasRefusedBlocks {
		return atlashclrender.RenderInspectedForAtlasCLI(r.db, r.info.Dialect, r.defaultSchemaName())
	}
	return atlashclrender.RenderInspected(r.db, r.info.Dialect, r.defaultSchemaName())
}

func (r *SchemaInspectReport) MarshalSQL(indent ...string) (string, error) {
	if len(indent) > 1 {
		return "", fmt.Errorf("unexpected number of arguments: %d", len(indent))
	}
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		r.db,
		r.info.Dialect,
		r.info.Capabilities,
	)
	if err != nil {
		return "", fmt.Errorf("render SQL: %w", err)
	}
	sql := strings.Join(statements, ";\n") + ";\n"
	if len(indent) == 0 || indent[0] == "" {
		return sql, nil
	}
	return indent[0] + strings.ReplaceAll(sql, "\n", "\n"+indent[0]), nil
}

func (r *SchemaInspectReport) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Realm)
}

func atlasSchemaInspectHCL(report *SchemaInspectReport) (string, error) {
	return report.MarshalHCL()
}

func atlasSchemaInspectSQL(report *SchemaInspectReport, indent ...string) (string, error) {
	return report.MarshalSQL(indent...)
}

func atlasSchemaInspectMermaid(report *SchemaInspectReport, _ ...string) (string, error) {
	out, err := schemaviz.Render(report.db, schemaviz.Options{
		Format:         schemaviz.FormatMermaid,
		IncludeColumns: true,
	})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func atlasSchemaInspectBase64URL(value any) string {
	return strings.NewReplacer("+", "-", "/", "_", "=", "").Replace(atlasTemplateString(value))
}

// atlasSchemaInspectJSON builds the realm document.
//
// The schema list comes from the schemas the reader described, NOT from the
// tables it found. Deriving it from tables made a schema disappear the moment
// it held none: an empty database rendered as `{}` where the pinned community
// binary v1.3.0 renders `{"schemas":[{"name":"public","comment":"standard
// public schema"}]}` — measured on PostgreSQL 17, and the same on SQLite with
// `main` and on MySQL 9.7 with the connected database (stokaro/ptah#1264).
//
// Tables still contribute their schema, so a reader that describes no schemas
// keeps rendering what it did before rather than losing its tables to a
// missing row.
func atlasSchemaInspectJSON(schema *dbschematypes.DBSchema, info dbschematypes.DBInfo) atlasSchemaInspectJSONRealm {
	schemasByName := make(map[string]*atlasSchemaInspectJSONSchema)
	indexesByTable := atlasSchemaInspectIndexesByTable(schema.Indexes)
	constraintsByTable := atlasSchemaInspectConstraintsByTable(schema.Constraints)
	for _, described := range schema.Schemas {
		jsonSchema := atlasSchemaInspectSchemaForName(schemasByName, described.Name)
		jsonSchema.atlasSchemaInspectJSONAttrs = atlasSchemaInspectJSONAttrs{
			Comment: described.Comment,
			Charset: described.Charset,
			Collate: described.Collate,
		}
	}
	for _, table := range schema.Tables {
		schemaName := atlasSchemaInspectSchemaName(table.Schema, info)
		jsonSchema := atlasSchemaInspectSchemaForName(schemasByName, schemaName)
		jsonSchema.Tables = append(jsonSchema.Tables, atlasSchemaInspectTable(table, indexesByTable, constraintsByTable))
	}
	names := make([]string, 0, len(schemasByName))
	for name := range schemasByName {
		names = append(names, name)
	}
	slices.Sort(names)

	realm := atlasSchemaInspectJSONRealm{Schemas: make([]atlasSchemaInspectJSONSchema, 0, len(names))}
	for _, name := range names {
		jsonSchema := schemasByName[name]
		slices.SortFunc(jsonSchema.Tables, func(a, b atlasSchemaInspectJSONTable) int {
			return strings.Compare(a.Name, b.Name)
		})
		realm.Schemas = append(realm.Schemas, *jsonSchema)
	}
	return realm
}

func atlasSchemaInspectSchemaForName(
	schemas map[string]*atlasSchemaInspectJSONSchema,
	name string,
) *atlasSchemaInspectJSONSchema {
	if schema, ok := schemas[name]; ok {
		return schema
	}
	schema := &atlasSchemaInspectJSONSchema{Name: name}
	schemas[name] = schema
	return schema
}

func atlasSchemaInspectTable(
	table dbschematypes.DBTable,
	indexesByTable map[string][]dbschematypes.DBIndex,
	constraintsByTable map[string][]dbschematypes.DBConstraint,
) atlasSchemaInspectJSONTable {
	jsonTable := atlasSchemaInspectJSONTable{
		Name: table.Name,
		atlasSchemaInspectJSONAttrs: atlasSchemaInspectJSONAttrs{
			Comment: table.Comment,
		},
	}
	for _, column := range table.Columns {
		jsonTable.Columns = append(jsonTable.Columns, atlasSchemaInspectColumn(column))
	}
	for _, index := range indexesByTable[table.QualifiedName()] {
		jsonIndex := atlasSchemaInspectIndex(index)
		if index.IsPrimary {
			jsonTable.PrimaryKey = atlasSchemaInspectPrimaryKey(jsonIndex.Parts)
			continue
		}
		jsonTable.Indexes = append(jsonTable.Indexes, jsonIndex)
	}
	for _, constraint := range constraintsByTable[table.QualifiedName()] {
		switch strings.ToUpper(constraint.Type) {
		case "PRIMARY KEY":
			if jsonTable.PrimaryKey == nil {
				jsonTable.PrimaryKey = atlasSchemaInspectPrimaryKey(atlasSchemaInspectConstraintIndexParts(constraint))
			}
		case "UNIQUE":
			jsonTable.Indexes = append(jsonTable.Indexes, atlasSchemaInspectUniqueConstraintIndex(constraint))
		case "FOREIGN KEY":
			jsonTable.ForeignKeys = append(jsonTable.ForeignKeys, atlasSchemaInspectForeignKey(constraint))
		}
	}
	return jsonTable
}

func atlasSchemaInspectColumn(column dbschematypes.DBColumn) atlasSchemaInspectJSONColumn {
	columnType := column.ColumnType
	if columnType == "" {
		columnType = column.DataType
	}
	return atlasSchemaInspectJSONColumn{
		Name: column.Name,
		Type: columnType,
		Null: strings.EqualFold(column.IsNullable, "YES"),
		atlasSchemaInspectJSONAttrs: atlasSchemaInspectJSONAttrs{
			Charset: column.Charset,
			Collate: column.Collate,
		},
	}
}

func atlasSchemaInspectIndex(index dbschematypes.DBIndex) atlasSchemaInspectJSONIndex {
	parts := atlasSchemaInspectIndexParts(index)
	if index.Expression != "" && len(parts) == 0 {
		parts = append(parts, atlasSchemaInspectJSONIndexPart{Expr: index.Expression})
	}
	return atlasSchemaInspectJSONIndex{
		Name:   index.Name,
		Unique: index.IsUnique,
		Parts:  parts,
	}
}

// atlasSchemaInspectIndexParts prefers the reader's structured key parts over
// the flat Columns list.
//
// Columns is a list of names and cannot say that a key is descending, so this
// output dropped the direction of every index. Measured against the pinned
// community binary v1.3.0 on PostgreSQL 17.10, for
// CREATE INDEX i_desc ON t (a DESC) it prints
// `{"desc": true, "column": "a"}` where Ptah printed `{"column": "a"}`.
//
// Falling back to Columns keeps every reader that supplies only the flat form
// -- MySQL, MariaDB -- printing exactly what it printed before.
func atlasSchemaInspectIndexParts(index dbschematypes.DBIndex) []atlasSchemaInspectJSONIndexPart {
	if len(index.Parts) == 0 {
		parts := make([]atlasSchemaInspectJSONIndexPart, 0, len(index.Columns))
		for _, column := range index.Columns {
			parts = append(parts, atlasSchemaInspectIndexPart(column))
		}
		return parts
	}
	parts := make([]atlasSchemaInspectJSONIndexPart, 0, len(index.Parts))
	for _, part := range index.Parts {
		parts = append(parts, atlasSchemaInspectStructuredIndexPart(part))
	}
	return parts
}

// atlasSchemaInspectStructuredIndexPart maps one structured key part. Unlike
// the string form below it never has to guess whether a key is an expression:
// the reader already established that from pg_index.indkey, so a column
// literally named "lower(name)" stays a column here.
func atlasSchemaInspectStructuredIndexPart(
	part dbschematypes.DBIndexPart,
) atlasSchemaInspectJSONIndexPart {
	if part.Expr != "" {
		return atlasSchemaInspectJSONIndexPart{Desc: part.Desc, Expr: part.Expr}
	}
	return atlasSchemaInspectJSONIndexPart{Desc: part.Desc, Column: part.Name}
}

func atlasSchemaInspectUniqueConstraintIndex(constraint dbschematypes.DBConstraint) atlasSchemaInspectJSONIndex {
	return atlasSchemaInspectJSONIndex{
		Name:   constraint.Name,
		Unique: true,
		Parts:  atlasSchemaInspectConstraintIndexParts(constraint),
	}
}

func atlasSchemaInspectPrimaryKey(parts []atlasSchemaInspectJSONIndexPart) *atlasSchemaInspectJSONIndex {
	return &atlasSchemaInspectJSONIndex{Parts: parts}
}

func atlasSchemaInspectConstraintIndexParts(
	constraint dbschematypes.DBConstraint,
) []atlasSchemaInspectJSONIndexPart {
	columns := constraint.ColumnNamesOrDefault()
	parts := make([]atlasSchemaInspectJSONIndexPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, atlasSchemaInspectIndexPart(column))
	}
	return parts
}

func atlasSchemaInspectIndexPart(value string) atlasSchemaInspectJSONIndexPart {
	if strings.ContainsAny(value, "() ") {
		return atlasSchemaInspectJSONIndexPart{Expr: value}
	}
	return atlasSchemaInspectJSONIndexPart{Column: value}
}

func atlasSchemaInspectForeignKey(constraint dbschematypes.DBConstraint) atlasSchemaInspectJSONForeignKey {
	foreignKey := atlasSchemaInspectJSONForeignKey{
		Name:    constraint.Name,
		Columns: constraint.ColumnNamesOrDefault(),
	}
	foreignKey.References.Table = constraint.QualifiedForeignTableName()
	foreignKey.References.Columns = constraint.ForeignColumnsOrDefault()
	return foreignKey
}

func atlasSchemaInspectIndexesByTable(indexes []dbschematypes.DBIndex) map[string][]dbschematypes.DBIndex {
	byTable := make(map[string][]dbschematypes.DBIndex)
	for _, index := range indexes {
		byTable[index.QualifiedTableName()] = append(byTable[index.QualifiedTableName()], index)
	}
	return byTable
}

func atlasSchemaInspectConstraintsByTable(
	constraints []dbschematypes.DBConstraint,
) map[string][]dbschematypes.DBConstraint {
	byTable := make(map[string][]dbschematypes.DBConstraint)
	for _, constraint := range constraints {
		byTable[constraint.QualifiedTableName()] = append(byTable[constraint.QualifiedTableName()], constraint)
	}
	return byTable
}

func atlasSchemaInspectSchemaName(schema string, info dbschematypes.DBInfo) string {
	if schema != "" {
		return schema
	}
	if info.Schema != "" {
		return info.Schema
	}
	if platform.NormalizeDialect(info.Dialect) == platform.SQLite {
		return "main"
	}
	return ""
}
