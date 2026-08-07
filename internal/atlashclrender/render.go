// Package atlashclrender renders Ptah schema IR as HCL schema text.
package atlashclrender

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/tableref"
)

// Severity is the diagnostic severity emitted by the exporter.
type Severity string

const (
	// SeverityWarning reports a lossy or currently unsupported export path.
	SeverityWarning Severity = "warning"
)

// Diagnostic describes an unsupported or lossy export detail.
type Diagnostic struct {
	Severity Severity
	Path     string
	Message  string
}

// TriggerDiagnosticPath returns an unambiguous diagnostic identity for a
// trigger, whose schema identity is the pair of target table and trigger name.
func TriggerDiagnosticPath(table, name string) string {
	return fmt.Sprintf("triggers[%q][%q]", table, name)
}

// Result is the rendered HCL plus loss diagnostics.
type Result struct {
	Data        []byte
	Diagnostics []Diagnostic
}

// Render renders a finalized Ptah schema IR as deterministic HCL schema text.
//
// It renders every column type as the IR spells it. Use [RenderForDialect] when
// the schema came out of a database rather than out of HCL: an inspected type
// carries no record of how it was written, and some of them have to be written
// as a sql() call to be readable at all.
func Render(db *goschema.Database) (Result, error) {
	return RenderForDialect(db, "")
}

// RenderForDialect renders the IR for a named dialect, writing a column type
// that dialect's Atlas HCL schema does not model as a sql() call.
//
// The dialect matters only for that decision. An empty dialect renders exactly
// what [Render] renders, which is what the parse-and-re-render callers want:
// their input was HCL, so the raw-SQL marker the parser set is already right and
// re-deciding it from the type name would second-guess the author.
func RenderForDialect(db *goschema.Database, dialect string) (Result, error) {
	return render(db, dialect, "")
}

// RenderInspected renders a schema that was read out of a database, declaring
// defaultSchema as the owner of every object the catalog reported without one.
//
// A catalog does not repeat the schema on objects the engine considers
// implicit, so an inspected IR arrives with no schema anywhere. HCL has no such
// notion: a file with no `schema` block and no `schema =` on its tables is not
// an under-specified schema, it is an invalid one. The pinned Atlas community
// binary v1.3.0 refuses it with
//
//	specutil: failed converting to *schema.Realm: cannot extract schema name
//	for table "t": schemahcl: type "schema" was not found in nil
//
// which is what made Ptah's inspect output unreadable to it independently of
// any type or permission question (stokaro/ptah#1234).
//
// The name is a parameter rather than something derived from the dialect
// because it is not derivable: a PostgreSQL connection's search_path can name
// any schema, and the caller is the only one that knows which one was read.
//
// [RenderForDialect] is deliberately left alone. Its callers parsed HCL to get
// here, so their IR already carries whatever the author wrote, and synthesizing
// a schema there would invent one the author did not declare.
func RenderInspected(db *goschema.Database, dialect, defaultSchema string) (Result, error) {
	return render(db, dialect, strings.TrimSpace(defaultSchema))
}

func render(db *goschema.Database, dialect, defaultSchema string) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}

	r := renderer{
		db:            db,
		dialect:       dialect,
		defaultSchema: defaultSchema,
	}
	r.render()
	return Result{
		Data:        []byte(r.builder.String()),
		Diagnostics: r.diagnostics,
	}, nil
}

type renderer struct {
	db      *goschema.Database
	dialect string
	// defaultSchema owns every object that arrived without one. Empty means the
	// IR is taken as written, which is what every parse-and-re-render caller
	// wants.
	defaultSchema string
	builder       strings.Builder
	diagnostics   []Diagnostic
}

// schemaFor returns the schema name to write for an object, which is the one it
// carries or, failing that, the schema the whole read belongs to.
func (r *renderer) schemaFor(schema string) string {
	if strings.TrimSpace(schema) != "" {
		return schema
	}
	return r.defaultSchema
}

// referencedSchemas lists, in a stable order, every schema this render will
// write a `schema.<name>` reference to. It is empty outside an inspected
// render, because nothing is synthesized there.
func (r *renderer) referencedSchemas() []string {
	if r.defaultSchema == "" {
		return nil
	}
	// Nothing to attach a schema to means nothing to declare. An inspect whose
	// selection matched no object renders an empty document, and a lone schema
	// block there would be a declaration of nothing -- which is what the
	// empty-include-selection contract already pins.
	//
	// Enums count as something to attach. They now carry a schema reference of
	// their own (see renderEnums), so a read that matched enums but no table
	// still has to declare the block that reference points at.
	if len(r.db.Tables) == 0 && len(r.db.Enums) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		names = append(names, name)
	}
	for _, table := range r.db.Tables {
		add(r.schemaFor(table.Schema))
	}
	if len(r.db.Enums) > 0 {
		add(r.defaultSchema)
	}
	slices.Sort(names)
	return names
}

func (r *renderer) render() {
	r.builder.WriteString("// Code generated by ptah; DO NOT EDIT.\n\n")
	r.renderSchemas()
	r.renderExtensions()
	r.renderSequences()
	r.renderUserTypes()
	r.renderEnums()
	r.renderRoles()
	r.renderTables()
	r.renderFunctions()
	r.renderViews()
	r.renderMaterializedViews()
	r.renderTriggers()
	r.renderRLSPolicies()
	r.renderGrants()
	r.renderManagedData()
}

func (r *renderer) renderSchemas() {
	schemas := append([]goschema.Schema(nil), r.db.Schemas...)
	// An inspected read reports no schema block at all, so every schema the
	// file is about to reference has to be declared here or the reference
	// resolves to nothing. A read that DID report one keeps what it reported,
	// comment, charset and collation included.
	//
	// It is not enough to declare the default. A reader looking at more than
	// one schema reports each table's own, so a table in `reporting` emits
	// `schema = schema.reporting` and needs that block too -- the first version
	// of this declared only the default, and a test row with a table outside it
	// is what caught the dangling reference.
	for _, name := range r.referencedSchemas() {
		if !slices.ContainsFunc(schemas, func(s goschema.Schema) bool { return s.Name == name }) {
			schemas = append(schemas, goschema.Schema{Name: name})
		}
	}
	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].Name < schemas[j].Name
	})
	for _, schema := range schemas {
		r.linef(`schema %s {`, quote(schema.Name))
		r.stringAttr(1, "comment", schema.Comment)
		r.stringAttr(1, "charset", schema.Charset)
		r.stringAttr(1, "collate", schema.Collate)
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderEnums() {
	enums := append([]goschema.Enum(nil), r.db.Enums...)
	sort.Slice(enums, func(i, j int) bool {
		return enums[i].Name < enums[j].Name
	})
	for _, enum := range enums {
		r.linef(`enum %s {`, quote(enum.Name))
		// An enum block with no schema is not under-specified, it is invalid.
		// The pinned Atlas community binary v1.3.0 refuses the whole file with
		//
		//	extract schema name from enum reference: schemahcl: type "schema"
		//	was not found in nil reference
		//
		// and accepts the identical file once `schema = schema.public` is added
		// -- one operand, measured both ways. That binary's own inspect writes
		// the attribute too (stokaro/ptah#1251).
		//
		// goschema.Enum carries no schema of its own, so the schema the whole
		// read belongs to is the only name available. It is also the right one:
		// a catalog omits the schema exactly where the engine treats the read's
		// own schema as implicit, which is the same reason renderTable falls
		// back to it.
		if schema := r.schemaFor(""); schema != "" {
			r.rawAttr(1, "schema", schemaRef(schema))
		}
		r.rawAttr(1, "values", stringList(enum.Values))
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderTables() {
	tables := append([]goschema.Table(nil), r.db.Tables...)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].QualifiedName() < tables[j].QualifiedName()
	})
	fieldsByStruct := groupFieldsByStruct(r.db.Fields)
	indexesByTable, orphanIndexes := groupIndexesByTable(r.db.Indexes, tables)
	constraintsByTable, orphanConstraints := groupConstraintsByTable(r.db.Constraints, tables)
	rlsEnabledByTable, orphanRLSEnabledTables := groupRLSEnabledByTable(r.db.RLSEnabledTables, tables)

	for _, table := range tables {
		r.renderTable(
			table,
			fieldsByStruct[table.StructName],
			indexesByTable[table.QualifiedName()],
			constraintsByTable[table.QualifiedName()],
			rlsEnabledByTable[table.QualifiedName()],
		)
	}
	for _, index := range orphanIndexes {
		r.warn("index "+index.Name, "index cannot be rendered because the target table is absent from the exported schema")
	}
	for _, constraint := range orphanConstraints {
		r.warn("constraint "+constraint.Name, "constraint cannot be rendered because the target table is absent from the exported schema")
	}
	for _, rlsEnabled := range orphanRLSEnabledTables {
		r.warn("rls_enabled_tables."+rlsEnabled.Table, "RLS enablement cannot be rendered because the target table is absent from the exported schema")
	}
}

func (r *renderer) renderTable(
	table goschema.Table,
	fields []goschema.Field,
	indexes []goschema.Index,
	constraints []goschema.Constraint,
	rlsEnabled *goschema.RLSEnabledTable,
) {
	r.linef(`table %s {`, quote(table.Name))
	if schema := r.schemaFor(table.Schema); schema != "" {
		r.rawAttr(1, "schema", schemaRef(schema))
	}
	r.stringAttr(1, "engine", table.Engine)
	r.stringAttr(1, "charset", table.Charset)
	r.stringAttr(1, "collate", table.Collate)
	r.stringAttr(1, "comment", table.Comment)
	if table.AutoIncrement != "" {
		r.rawAttr(1, "auto_increment", table.AutoIncrement)
	}
	if table.Strict {
		r.rawAttr(1, "strict", "true")
	}
	if table.WithoutRowID {
		r.rawAttr(1, "without_rowid", "true")
	}
	if len(table.Checks) > 0 {
		r.rawAttr(1, "checks", stringList(table.Checks))
	}
	r.stringAttr(1, "custom", table.CustomSQL)
	r.renderPlatformOverrides(1, table.Overrides)
	r.renderRowSecurity(rlsEnabled)

	sort.SliceStable(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	for _, field := range fields {
		if fieldIsPrimary(table, field) {
			field.Nullable = false
		}
		r.renderColumn(field)
	}
	r.renderPrimaryKey(table, fields)
	r.renderPartition(table.Partition)
	r.renderFieldForeignKeys(table, fields, constraints)
	for _, constraint := range constraints {
		r.renderConstraint(constraint)
	}
	for _, index := range indexes {
		r.renderIndex(index)
	}
	r.line("}")
	r.line("")
}

func (r *renderer) renderColumn(field goschema.Field) {
	r.linef(`  column %s {`, quote(field.Name))
	r.rawAttr(2, "type", r.columnTypeExpr(field))
	if field.Nullable {
		r.rawAttr(2, "null", "true")
	}
	if field.AutoInc {
		r.rawAttr(2, "auto_increment", "true")
	}
	if field.Unique {
		r.rawAttr(2, "unique", "true")
	}
	if field.DefaultExpr != "" {
		r.rawAttr(2, "default", sqlCall(field.DefaultExpr))
	} else if field.DefaultSet || field.Default != "" {
		r.rawAttr(2, "default", quote(field.Default))
	}
	if field.UpdateExpression != "" {
		r.rawAttr(2, "on_update", sqlCall(field.UpdateExpression))
	}
	if field.GeneratedExpression != "" {
		r.line("    as {")
		r.stringAttr(3, "expr", field.GeneratedExpression)
		r.stringAttr(3, "type", field.GeneratedKind)
		r.line("    }")
	}
	if field.IdentityGeneration != "" || field.IdentityStart != "" || field.IdentityIncrement != "" || field.IdentityOptions != "" {
		r.line("    identity {")
		r.stringAttr(3, "generated", field.IdentityGeneration)
		r.stringAttr(3, "start", field.IdentityStart)
		r.stringAttr(3, "increment", field.IdentityIncrement)
		r.stringAttr(3, "options", field.IdentityOptions)
		r.line("    }")
	}
	if field.UniqueExpr != "" {
		r.stringAttr(2, "unique_expr", field.UniqueExpr)
	}
	if len(field.Enum) > 0 {
		r.rawAttr(2, "enum", stringList(field.Enum))
	}
	r.stringAttr(2, "check", field.Check)
	r.stringAttr(2, "check_name", field.CheckName)
	r.stringAttr(2, "charset", field.Charset)
	r.stringAttr(2, "collate", field.Collate)
	r.stringAttr(2, "comment", field.Comment)
	r.renderPlatformOverrides(2, field.Overrides)
	r.line("  }")
}

func fieldIsPrimary(table goschema.Table, field goschema.Field) bool {
	return field.Primary ||
		slices.Contains(table.PrimaryKey, field.Name) ||
		slices.ContainsFunc(table.PrimaryKeyParts, func(part goschema.PrimaryKeyPart) bool {
			return part.Name == field.Name
		})
}

func (r *renderer) renderFieldForeignKeys(
	table goschema.Table,
	fields []goschema.Field,
	constraints []goschema.Constraint,
) {
	constraintNames := foreignKeyConstraintNames(constraints)
	for _, field := range fields {
		if field.Foreign == "" {
			continue
		}
		name := field.ForeignKeyName
		if name == "" {
			name = "fk_" + table.Name + "_" + field.Name
		}
		if constraintNames[name] {
			continue
		}
		foreignTable, foreignColumns := parseForeignReference(field.Foreign)
		r.renderForeignKey(name, []string{field.Name}, foreignTable, foreignColumns, field.OnDelete, field.OnUpdate)
	}
}

func foreignKeyConstraintNames(constraints []goschema.Constraint) map[string]bool {
	result := make(map[string]bool)
	for _, constraint := range constraints {
		if strings.EqualFold(constraint.Type, "FOREIGN KEY") && constraint.Name != "" {
			result[constraint.Name] = true
		}
	}
	return result
}

func (r *renderer) renderPrimaryKey(table goschema.Table, fields []goschema.Field) {
	columns := table.PrimaryKey
	if len(columns) == 0 {
		for _, field := range fields {
			if field.Primary {
				columns = append(columns, field.Name)
			}
		}
	}
	if len(columns) == 0 && len(table.PrimaryKeyParts) == 0 {
		return
	}
	r.line("  primary_key {")
	if len(table.PrimaryKeyParts) > 0 && !simplePrimaryKeyParts(table.PrimaryKeyParts) {
		for _, part := range table.PrimaryKeyParts {
			r.line("    on {")
			if part.Name != "" {
				r.rawAttr(3, "column", columnRef(part.Name))
			}
			r.stringAttr(3, "prefix", part.Prefix)
			if part.Desc {
				r.rawAttr(3, "desc", "true")
			}
			r.line("    }")
		}
	} else {
		r.rawAttr(2, "columns", columnRefs(columns))
	}
	if len(table.PrimaryKeyInclude) > 0 {
		r.rawAttr(2, "include", columnRefs(table.PrimaryKeyInclude))
	}
	r.line("  }")
}

func (r *renderer) renderPartition(partition *goschema.PartitionSpec) {
	if partition == nil {
		return
	}
	r.line("  partition {")
	r.stringAttr(2, "type", partition.Type)
	for _, part := range partition.Parts {
		r.line("    by {")
		if part.Name != "" {
			r.rawAttr(3, "column", columnRef(part.Name))
		}
		r.stringAttr(3, "expr", part.Expr)
		r.line("    }")
	}
	r.line("  }")
}

func (r *renderer) renderConstraint(constraint goschema.Constraint) {
	if r.renderAtlasConstraint(constraint) {
		return
	}
	r.renderPtahConstraint(constraint)
}

func (r *renderer) renderAtlasConstraint(constraint goschema.Constraint) bool {
	switch strings.ToUpper(constraint.Type) {
	case "CHECK":
		if !atlasCheckConstraint(constraint) {
			return false
		}
		if constraint.Name == "" {
			r.line("  check {")
		} else {
			r.linef(`  check %s {`, quote(constraint.Name))
		}
		r.stringAttr(2, "expr", constraint.CheckExpression)
		r.line("  }")
		return true
	case "UNIQUE":
		if !atlasUniqueConstraint(constraint) {
			return false
		}
		r.linef(`  unique %s {`, quote(constraint.Name))
		r.rawAttr(2, "columns", columnRefs(constraint.Columns))
		if len(constraint.IncludeColumns) > 0 {
			r.rawAttr(2, "include", columnRefs(constraint.IncludeColumns))
		}
		r.boolPtrAttr(2, "nulls_distinct", constraint.NullsDistinct)
		r.line("  }")
		return true
	case "FOREIGN KEY":
		if !atlasForeignKeyConstraint(constraint) {
			return false
		}
		r.renderForeignKey(
			constraint.Name,
			constraint.Columns,
			constraint.ForeignTable,
			constraint.ForeignColumnsOrDefault(),
			constraint.OnDelete,
			constraint.OnUpdate,
		)
		return true
	case "PRIMARY KEY":
		if !atlasPrimaryKeyConstraint(constraint) {
			return false
		}
		r.line("  primary_key {")
		r.rawAttr(2, "columns", columnRefs(constraint.Columns))
		if len(constraint.IncludeColumns) > 0 {
			r.rawAttr(2, "include", columnRefs(constraint.IncludeColumns))
		}
		r.line("  }")
		return true
	default:
		return false
	}
}

func (r *renderer) renderPtahConstraint(constraint goschema.Constraint) {
	r.linef(`  constraint %s {`, quote(constraint.Name))
	r.stringAttr(2, "type", constraint.Type)
	r.stringAttr(2, "using", constraint.UsingMethod)
	r.stringAttr(2, "elements", constraint.ExcludeElements)
	r.stringAttr(2, "condition", constraint.WhereCondition)
	r.stringAttr(2, "check", constraint.CheckExpression)
	if len(constraint.Columns) > 0 {
		r.rawAttr(2, "columns", stringList(constraint.Columns))
	}
	if len(constraint.IncludeColumns) > 0 {
		r.rawAttr(2, "include", stringList(constraint.IncludeColumns))
	}
	r.boolPtrAttr(2, "nulls_distinct", constraint.NullsDistinct)
	r.stringAttr(2, "foreign_table", constraint.ForeignTable)
	if foreignColumns := constraint.ForeignColumnsOrDefault(); len(foreignColumns) > 0 {
		r.rawAttr(2, "foreign_columns", stringList(foreignColumns))
	}
	r.stringAttr(2, "on_delete", constraint.OnDelete)
	r.stringAttr(2, "on_update", constraint.OnUpdate)
	r.stringAttr(2, "comment", constraint.Comment)
	r.line("  }")
}

func atlasCheckConstraint(constraint goschema.Constraint) bool {
	return constraint.CheckExpression != "" &&
		constraint.UsingMethod == "" &&
		constraint.ExcludeElements == "" &&
		constraint.WhereCondition == "" &&
		len(constraint.Columns) == 0 &&
		len(constraint.IncludeColumns) == 0 &&
		constraint.NullsDistinct == nil &&
		constraint.ForeignTable == "" &&
		len(constraint.ForeignColumnsOrDefault()) == 0 &&
		constraint.OnDelete == "" &&
		constraint.OnUpdate == "" &&
		constraint.Comment == ""
}

func atlasUniqueConstraint(constraint goschema.Constraint) bool {
	return constraint.Name != "" &&
		len(constraint.Columns) > 0 &&
		constraint.UsingMethod == "" &&
		constraint.ExcludeElements == "" &&
		constraint.WhereCondition == "" &&
		constraint.CheckExpression == "" &&
		constraint.ForeignTable == "" &&
		len(constraint.ForeignColumnsOrDefault()) == 0 &&
		constraint.OnDelete == "" &&
		constraint.OnUpdate == "" &&
		constraint.Comment == ""
}

func atlasForeignKeyConstraint(constraint goschema.Constraint) bool {
	return len(constraint.Columns) > 0 &&
		constraint.ForeignTable != "" &&
		len(constraint.ForeignColumnsOrDefault()) == len(constraint.Columns) &&
		constraint.UsingMethod == "" &&
		constraint.ExcludeElements == "" &&
		constraint.WhereCondition == "" &&
		constraint.CheckExpression == "" &&
		len(constraint.IncludeColumns) == 0 &&
		constraint.NullsDistinct == nil &&
		constraint.Comment == ""
}

func atlasPrimaryKeyConstraint(constraint goschema.Constraint) bool {
	return constraint.Name == "" &&
		len(constraint.Columns) > 0 &&
		constraint.UsingMethod == "" &&
		constraint.ExcludeElements == "" &&
		constraint.WhereCondition == "" &&
		constraint.CheckExpression == "" &&
		constraint.NullsDistinct == nil &&
		constraint.ForeignTable == "" &&
		len(constraint.ForeignColumnsOrDefault()) == 0 &&
		constraint.OnDelete == "" &&
		constraint.OnUpdate == "" &&
		constraint.Comment == ""
}

func (r *renderer) renderForeignKey(name string, columns []string, foreignTable string, foreignColumns []string, onDelete string, onUpdate string) {
	r.linef(`  foreign_key %s {`, quote(name))
	r.rawAttr(2, "columns", columnRefs(columns))
	r.rawAttr(2, "ref_columns", tableColumnRefs(foreignTable, foreignColumns))
	r.stringAttr(2, "on_delete", onDelete)
	r.stringAttr(2, "on_update", onUpdate)
	r.line("  }")
}

func (r *renderer) renderIndex(index goschema.Index) {
	r.linef(`  index %s {`, quote(index.Name))
	if index.Unique {
		r.rawAttr(2, "unique", "true")
	}
	r.stringAttr(2, "type", index.Type)
	r.stringAttr(2, "parser", index.Parser)
	r.stringAttr(2, "where", index.Condition)
	r.stringAttr(2, "comment", index.Comment)
	r.stringAttr(2, "ops", index.Operator)
	r.boolPtrAttr(2, "nulls_distinct", index.NullsDistinct)
	// Granularity is always non-negative (the parser rejects negatives) and 0 is
	// the implicit dialect default, so emit only a positive value; the parser
	// defaults an absent attribute back to 0, keeping the render/parse pair
	// symmetric.
	if index.Granularity != 0 {
		r.rawAttr(2, "granularity", strconv.Itoa(index.Granularity))
	}
	if len(index.IncludeColumns) > 0 {
		r.rawAttr(2, "include", columnRefs(index.IncludeColumns))
	}
	if pages, ok := index.StorageParams["pages_per_range"]; ok {
		r.rawAttr(2, "pages_per_range", pages)
	}
	if len(index.Parts) > 0 && !simpleIndexParts(index.Parts) {
		for _, part := range index.Parts {
			r.line("    on {")
			if part.Name != "" {
				r.rawAttr(3, "column", columnRef(part.Name))
			}
			r.stringAttr(3, "expr", part.Expr)
			r.stringAttr(3, "ops", part.Operator)
			r.stringAttr(3, "prefix", part.Prefix)
			if part.Desc {
				r.rawAttr(3, "desc", "true")
			}
			r.line("    }")
		}
	} else {
		r.rawAttr(2, "columns", columnRefs(firstNonEmptySlice(index.Fields, indexPartNames(index.Parts))))
	}
	r.line("  }")
}

func (r *renderer) renderPlatformOverrides(indent int, overrides map[string]map[string]string) {
	for _, dialect := range sortedMapKeys(overrides) {
		values := overrides[dialect]
		if len(values) == 0 {
			continue
		}
		r.linef("%splatform %s {", strings.Repeat("  ", indent), quote(dialect))
		for _, key := range sortedMapKeys(values) {
			r.linef("%soverride %s {", strings.Repeat("  ", indent+1), quote(key))
			r.rawAttr(indent+2, "value", quote(values[key]))
			r.linef("%s}", strings.Repeat("  ", indent+1))
		}
		r.linef("%s}", strings.Repeat("  ", indent))
	}
}

func (r *renderer) warn(path, message string) {
	r.diagnostics = append(r.diagnostics, Diagnostic{
		Severity: SeverityWarning,
		Path:     path,
		Message:  message,
	})
}

func (r *renderer) line(value string) {
	r.builder.WriteString(value)
	r.builder.WriteByte('\n')
}

func (r *renderer) linef(format string, args ...any) {
	r.line(fmt.Sprintf(format, args...))
}

func (r *renderer) rawAttr(indent int, name, value string) {
	if value == "" {
		return
	}
	r.linef("%s%s = %s", strings.Repeat("  ", indent), name, value)
}

func (r *renderer) stringAttr(indent int, name, value string) {
	if value == "" {
		return
	}
	r.rawAttr(indent, name, quote(value))
}

func (r *renderer) boolPtrAttr(indent int, name string, value *bool) {
	if value == nil {
		return
	}
	r.rawAttr(indent, name, strconv.FormatBool(*value))
}

func groupFieldsByStruct(fields []goschema.Field) map[string][]goschema.Field {
	result := make(map[string][]goschema.Field)
	for _, field := range fields {
		result[field.StructName] = append(result[field.StructName], field)
	}
	return result
}

func groupIndexesByTable(
	indexes []goschema.Index,
	tables []goschema.Table,
) (map[string][]goschema.Index, []goschema.Index) {
	return groupTableObjects(
		indexes,
		tables,
		func(index goschema.Index) (string, string) {
			return index.StructName, index.TableName
		},
		func(a, b goschema.Index) int {
			return cmp.Compare(a.Name, b.Name)
		},
	)
}

func groupConstraintsByTable(
	constraints []goschema.Constraint,
	tables []goschema.Table,
) (map[string][]goschema.Constraint, []goschema.Constraint) {
	return groupTableObjects(
		constraints,
		tables,
		func(constraint goschema.Constraint) (string, string) {
			return constraint.StructName, constraint.Table
		},
		func(a, b goschema.Constraint) int {
			return cmp.Compare(a.Name, b.Name)
		},
	)
}

func groupTableObjects[T any](
	objects []T,
	tables []goschema.Table,
	tableReference func(T) (string, string),
	compare func(T, T) int,
) (map[string][]T, []T) {
	result := make(map[string][]T)
	var orphan []T
	for _, object := range objects {
		structName, tableName := tableReference(object)
		table := resolveTable(tables, structName, tableName)
		if table == nil {
			orphan = append(orphan, object)
			continue
		}
		result[table.QualifiedName()] = append(result[table.QualifiedName()], object)
	}
	for key := range result {
		slices.SortFunc(result[key], compare)
	}
	slices.SortFunc(orphan, compare)
	return result, orphan
}

func resolveTable(tables []goschema.Table, structName, tableName string) *goschema.Table {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		for i := range tables {
			if tables[i].StructName == structName {
				return &tables[i]
			}
		}
		return nil
	}
	for i := range tables {
		if tables[i].QualifiedName() == tableName {
			return &tables[i]
		}
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return nil
	}
	for i := range tables {
		if tables[i].StructName == structName && tables[i].Name == ref.Name {
			return &tables[i]
		}
	}
	var match *goschema.Table
	for i := range tables {
		if tables[i].Name != ref.Name {
			continue
		}
		if match != nil {
			return nil
		}
		match = &tables[i]
	}
	return match
}

func simplePrimaryKeyParts(parts []goschema.PrimaryKeyPart) bool {
	for _, part := range parts {
		if part.Prefix != "" || part.Desc {
			return false
		}
	}
	return true
}

func simpleIndexParts(parts []goschema.IndexPart) bool {
	for _, part := range parts {
		if part.Expr != "" || part.Operator != "" || part.Prefix != "" || part.Desc {
			return false
		}
	}
	return true
}

func indexPartNames(parts []goschema.IndexPart) []string {
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		if part.Name != "" {
			names = append(names, part.Name)
		}
	}
	return names
}

func firstNonEmptySlice(first, second []string) []string {
	if len(first) > 0 {
		return first
	}
	return second
}

func firstNonEmpty(first, second string) string {
	if first != "" {
		return first
	}
	return second
}

func parseForeignReference(value string) (string, []string) {
	value = strings.TrimSpace(value)
	open := strings.LastIndex(value, "(")
	closeParen := strings.LastIndex(value, ")")
	if open < 0 || closeParen < open {
		return value, nil
	}
	table := strings.TrimSpace(value[:open])
	rawColumns := strings.Split(value[open+1:closeParen], ",")
	columns := make([]string, 0, len(rawColumns))
	for _, rawColumn := range rawColumns {
		column := strings.TrimSpace(rawColumn)
		if column != "" {
			columns = append(columns, column)
		}
	}
	return table, columns
}

// columnTypeExpr renders a column's `type` attribute.
//
// A type the schema author reached the sql() escape hatch for is written back
// through it. The reduction issue #1106 introduced is a READ-side reduction:
// Ptah's IR carries the SQL text so the DDL it renders is valid. Writing that
// text back bare loses the only spelling of an engine type Atlas does not model
// that the pinned Atlas community binary v1.3.0 accepts -- it refuses
// `type = USER_DEFINED` with `Unknown column.type; There is no type named
// "USER_DEFINED"` and accepts `type = sql("USER_DEFINED")`. Measured on that
// binary, an HCL file it plans must survive a Ptah round trip still planning.
func (r *renderer) columnTypeExpr(field goschema.Field) string {
	if strings.TrimSpace(field.Type) == "" {
		return typeExpr(field.Type)
	}
	// The IR remembers the call when the schema came from HCL. When it came
	// from a database it remembers nothing, so the dialect's own list of
	// modeled types decides -- see modeled_types.go for why that list is
	// trustworthy rather than a copied table.
	//
	// The raw-SQL check stays ahead of the enum check on purpose. A sql() call
	// is the author's own escape hatch, and issue #1106's contract is to write
	// it back exactly as written rather than re-decide it from the type name; a
	// name that happens to match an enum block could just as well mean a domain
	// or composite the author is reaching past Atlas's model for. A database
	// read never sets the flag -- only the HCL parser does -- so the inspect
	// path this fixes is not affected by the ordering.
	if field.TypeRawSQL {
		return sqlCall(field.Type)
	}
	if ref, ok := r.enumTypeRef(field.Type); ok {
		return ref
	}
	modeled, ok := modeledColumnType(r.dialect, field.Type)
	if !ok {
		// Wrapped verbatim: an engine type Atlas does not model is only
		// readable as the text the database itself reports.
		return sqlCall(field.Type)
	}
	return typeExpr(modeled)
}

// enumTypeRef returns the expression for a column whose type is an enum this
// same document declares, and reports whether the type is one.
//
// The reference is not one spelling among several -- it is the only one that
// works. Measured on the pinned Atlas community binary v1.3.0, a table with an
// enum-typed column and the matching `enum` block, one operand varied:
//
//	type = enum.status     exit 0
//	type = sql("status")   exit 1  pq: type "status" does not exist
//	type = status          exit 1  Unknown column.type; There is no type named "status"
//	type = "status"        exit 1  set field "type": unexpected type string
//
// sql() is the interesting failure and the one Ptah used to emit: the type name
// is real in the database that was inspected but not in the dev database the
// file gets replayed into, and only the enum block creates it there. That
// binary's own inspect writes `type = enum.status` as well.
//
// The reference is unqualified because that binary keys enum blocks by name
// alone: two `enum "status"` blocks in different schemas are refused as
// `duplicate enum "status"`, and an `enum.status` reference resolves to a block
// declared in a non-default schema (measured with table and enum both in
// `reporting`).
//
// A name that also names a domain, composite or range is left alone. Those are
// separate declarations in the same document, and a reference into the enum
// block would point at the wrong one.
func (r *renderer) enumTypeRef(typeName string) (string, bool) {
	name := strings.TrimSpace(typeName)
	if name == "" {
		return "", false
	}
	if !slices.ContainsFunc(r.db.Enums, func(e goschema.Enum) bool { return e.Name == name }) {
		return "", false
	}
	if slices.ContainsFunc(r.db.Domains, func(d goschema.Domain) bool { return d.Name == name }) ||
		slices.ContainsFunc(r.db.CompositeTypes, func(ct goschema.CompositeType) bool { return ct.Name == name }) ||
		slices.ContainsFunc(r.db.Ranges, func(rg goschema.Range) bool { return rg.Name == name }) {
		return "", false
	}
	return "enum" + objectRefPart(name), true
}

func typeExpr(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "text"
	}
	if _, err := strconv.Unquote(value); err == nil {
		return quote(value)
	}
	_, diagnostics := hclsyntax.ParseExpression([]byte(value), "<type>", hcl.InitialPos)
	if diagnostics.HasErrors() {
		return quote(value)
	}
	return value
}

func sqlCall(value string) string {
	return "sql(" + quote(value) + ")"
}

func quote(value string) string {
	return string(hclwrite.TokensForValue(cty.StringVal(value)).Bytes())
}

func stringList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = quote(value)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func columnRefs(columns []string) string {
	refs := make([]string, 0, len(columns))
	for _, column := range columns {
		if column == "" {
			continue
		}
		refs = append(refs, columnRef(column))
	}
	return "[" + strings.Join(refs, ", ") + "]"
}

func columnRef(name string) string {
	return "column" + objectRefPart(name)
}

func schemaRef(name string) string {
	return "schema" + objectRefPart(name)
}

func tableColumnRefs(table string, columns []string) string {
	tableRef := objectRef("table", table)
	refs := make([]string, 0, len(columns))
	for _, column := range columns {
		if table == "" || column == "" {
			continue
		}
		refs = append(refs, tableRef+".column"+objectRefPart(column))
	}
	return "[" + strings.Join(refs, ", ") + "]"
}
