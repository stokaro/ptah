package atlasreport

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/template"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlashclrender"
	"github.com/stokaro/ptah/internal/schemaviz"
)

type SchemaInspectReport struct {
	db          *goschema.Database
	info        dbschematypes.DBInfo
	diagnostics io.Writer
	Realm       atlasSchemaInspectJSONRealm `json:"-"`
	Schema      atlasSchemaInspectJSONRealm `json:"-"`
}

type atlasSchemaInspectJSONRealm struct {
	Schemas []atlasSchemaInspectJSONSchema `json:"schemas,omitempty"`
}

type atlasSchemaInspectJSONSchema struct {
	Name string `json:"name"`
	atlasSchemaInspectJSONAttrs
	Tables []atlasSchemaInspectJSONTable `json:"tables,omitempty"`
}

type atlasSchemaInspectJSONTable struct {
	Name string `json:"name"`
	atlasSchemaInspectJSONAttrs
	Columns     []atlasSchemaInspectJSONColumn     `json:"columns,omitempty"`
	Indexes     []atlasSchemaInspectJSONIndex      `json:"indexes,omitempty"`
	PrimaryKey  *atlasSchemaInspectJSONIndex       `json:"primary_key,omitempty"`
	ForeignKeys []atlasSchemaInspectJSONForeignKey `json:"foreign_keys,omitempty"`
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

func RenderSchemaInspectFormat(format string, report *SchemaInspectReport) (string, error) {
	var out strings.Builder
	if err := renderAtlasSchemaInspectTemplate(&out, "atlas-schema-inspect-format", format, report); err != nil {
		return "", err
	}
	return out.String(), nil
}

func NewSchemaInspectReport(
	db *goschema.Database,
	schema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	diagnostics io.Writer,
) *SchemaInspectReport {
	realm := atlasSchemaInspectJSON(schema, info)
	return &SchemaInspectReport{
		db:          db,
		info:        info,
		diagnostics: diagnostics,
		Realm:       realm,
		Schema:      realm,
	}
}

func ValidateSchemaInspectTemplate(format string) error {
	_, err := newAtlasSchemaInspectTemplate("atlas-schema-inspect-format", format)
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

func renderAtlasSchemaInspectTemplate(
	out *strings.Builder,
	name string,
	format string,
	report *SchemaInspectReport,
) error {
	tmpl, err := newAtlasSchemaInspectTemplate(name, format)
	if err != nil {
		return err
	}
	if err := tmpl.Execute(out, report); err != nil {
		return fmt.Errorf("execute --format template: %w", err)
	}
	return nil
}

func newAtlasSchemaInspectTemplate(name, format string) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(template.FuncMap{
		"base64url": atlasSchemaInspectBase64URL,
		"json":      atlasTemplateJSON,
		"mermaid":   atlasSchemaInspectMermaid,
		"sql":       atlasSchemaInspectSQL,
		"split":     atlasSchemaInspectUnsupportedFileTemplateFunc,
		"write":     atlasSchemaInspectUnsupportedFileTemplateFunc,
	}).Parse(format)
	if err != nil {
		return nil, fmt.Errorf("parse --format template: %w", err)
	}
	return tmpl, nil
}

func (r *SchemaInspectReport) MarshalHCL() (string, error) {
	rendered, err := atlashclrender.Render(r.db)
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

func atlasSchemaInspectUnsupportedFileTemplateFunc(_ ...any) (string, error) {
	return "", fmt.Errorf("atlas schema inspect accepts split/write templates, but Ptah does not implement their behavior yet")
}

func atlasSchemaInspectBase64URL(value string) string {
	return strings.NewReplacer("+", "-", "/", "_", "=", "").Replace(value)
}

func atlasSchemaInspectJSON(schema *dbschematypes.DBSchema, info dbschematypes.DBInfo) atlasSchemaInspectJSONRealm {
	schemasByName := make(map[string]*atlasSchemaInspectJSONSchema)
	indexesByTable := atlasSchemaInspectIndexesByTable(schema.Indexes)
	constraintsByTable := atlasSchemaInspectConstraintsByTable(schema.Constraints)
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
	parts := make([]atlasSchemaInspectJSONIndexPart, 0, len(index.Columns))
	for _, column := range index.Columns {
		parts = append(parts, atlasSchemaInspectIndexPart(column))
	}
	if index.Expression != "" && len(parts) == 0 {
		parts = append(parts, atlasSchemaInspectJSONIndexPart{Expr: index.Expression})
	}
	return atlasSchemaInspectJSONIndex{
		Name:   index.Name,
		Unique: index.IsUnique,
		Parts:  parts,
	}
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
