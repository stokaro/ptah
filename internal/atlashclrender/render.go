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

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/sqlitekey"
	"go.5x5.cz/ptah/internal/tableref"
)

// primaryKeyImpliesNotNull reports whether the dialect makes a primary key
// column NOT NULL on its own.
//
// SQLite does not decide it from the dialect: on a rowid table
// `id INTEGER PRIMARY KEY` is a rowid alias whose `pragma table_info.notnull` is
// 0 and which accepts an explicit NULL insert, while a STRICT or WITHOUT ROWID
// table does enforce NOT NULL on its key columns. This predicate answers for the
// ordinary rowid table, and [renderer.keyColumnIsNotNull] adds the table's shape
// where the dialect is SQLite. See stokaro/ptah#1235.
func primaryKeyImpliesNotNull(dialect string) bool {
	return platform.NormalizeDialect(dialect) != platform.SQLite
}

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
	// NotDescribed is what the rendered document does not claim to describe.
	// It is also written into the document itself, as directive comments in the
	// header, because the process that reads the document back is not this one;
	// see [go.5x5.cz/ptah/core/coverage].
	NotDescribed coverage.Set
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
	return render(db, dialect, "", false)
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
	return render(db, dialect, strings.TrimSpace(defaultSchema), false)
}

// RenderInspectedForAtlasCLI renders an inspected schema for the
// Atlas-compatible surface, leaving out the top-level block types the pinned
// Atlas community binary v1.3.0 refuses as a feature -- but only where nothing
// else in the document names the object -- and reporting every decision as a
// loss diagnostic.
//
// It differs from [RenderInspected] in nothing else. The omission is a property
// of the reader the compatibility binary stands in for, not of the database and
// not of Ptah, so the native surface keeps describing everything Ptah models;
// see [atlasRefusedBlockTypes] for the list and the measurement behind it, and
// [renderer.omitRefusedBlock] for why suppression is reference-aware
// (stokaro/ptah#1251).
//
// The capability is not deleted by the default. Setting
// [KeepAtlasRefusedBlocksEnvVar] restores every block on this same surface.
func RenderInspectedForAtlasCLI(db *goschema.Database, dialect, defaultSchema string) (Result, error) {
	return render(db, dialect, strings.TrimSpace(defaultSchema), true)
}

func render(db *goschema.Database, dialect, defaultSchema string, omitAtlasRefusedBlocks bool) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}

	r := renderer{
		db:                     db,
		dialect:                dialect,
		defaultSchema:          defaultSchema,
		omitAtlasRefusedBlocks: omitAtlasRefusedBlocks,
	}
	r.render()
	return Result{
		Data:         []byte(r.builder.String()),
		Diagnostics:  r.diagnostics,
		NotDescribed: r.notDescribed(),
	}, nil
}

type renderer struct {
	db      *goschema.Database
	dialect string
	// defaultSchema owns every object that arrived without one. Empty means the
	// IR is taken as written, which is what every parse-and-re-render caller
	// wants.
	defaultSchema string
	// omitAtlasRefusedBlocks marks the render as feeding the Atlas-compatible
	// surface, which leaves out the block types the pinned binary refuses
	// unless the document names the object; see [renderer.omitRefusedBlock].
	omitAtlasRefusedBlocks bool
	// references caches the identifiers the surviving document names, built
	// once per render by [collectReferencedNames]. Nil means not yet built,
	// which is distinguishable from "built and empty" because a document with
	// no references at all still answers every lookup false.
	references map[string]bool
	// requiredExtensions caches the extensions the surviving document's objects
	// resolve to without naming, built once per render by
	// [collectRequiredExtensions]. Nil means not yet built, on the same
	// reasoning as references above.
	requiredExtensions map[string]bool
	// tableSchemas caches, per table block label this render will write, the
	// schema each block carries. Nil means not yet built, which is
	// distinguishable from "built and empty" because a document with no tables
	// still answers every lookup. See [renderer.documentResolvesTableRef].
	tableSchemas map[string][]string
	// roleBlocks caches the `role` block labels this render will write. Nil
	// means not yet built, which is distinguishable from "built and empty"
	// because a document with no roles still answers every lookup. See
	// [renderer.documentDeclaresRole].
	roleBlocks map[string]bool
	// relationBlocks caches, per block label this render will write, every
	// relation block carrying it. Nil means not yet built, which is
	// distinguishable from "built and empty" because a document with no
	// relations still answers every lookup. See [renderer.documentDeclares].
	relationBlocks map[string][]declaredBlock
	// schemaRefs collects the name of every `schema.<name>` reference the body
	// wrote, filled by [renderer.schemaRef] as the body is rendered and read
	// afterwards by [renderer.referencedSchemas].
	schemaRefs  map[string]bool
	builder     strings.Builder
	diagnostics []Diagnostic
}

// documentDeclaresRole reports whether this document writes a `role` block
// labeled with this name.
//
// A `role.<name>` traversal names a BLOCK, so without one it is an HCL variable
// reference with nothing behind it and the pinned Atlas community binary v1.3.0
// refuses the whole file with `There is no variable named "role"`. Every render
// writes a block for every role in the IR, so the IR is the list of labels.
//
// Matching is exact rather than case-insensitive: [renderer.renderRoles] writes
// the label as a quoted string, so `role "App"` and `role "app"` are two
// different blocks and a traversal resolves to neither of the other's.
func (r *renderer) documentDeclaresRole(name string) bool {
	if r.roleBlocks == nil {
		blocks := make(map[string]bool, len(r.db.Roles))
		for _, role := range r.db.Roles {
			blocks[role.Name] = true
		}
		r.roleBlocks = blocks
	}
	return r.roleBlocks[name]
}

// declaredBlock is one relation block this render writes, described the way a
// reference to it has to be spelled: the block KIND the traversal names, and
// the schema the block belongs to.
type declaredBlock struct {
	kind   string
	schema string
}

// documentDeclares reports the single relation block this document writes under
// this label, when there is exactly one.
//
// A reference in HCL names a BLOCK, and the block TYPE is the first word of the
// traversal, so `table.v` is "the v attribute of the table object" and resolves
// to nothing when the document declares `view "v"`. The pinned Atlas community
// binary v1.3.0 refuses the whole file over it. Measured on PostgreSQL 17
// against the document `ptah-compat schema inspect` writes for
//
//	CREATE TABLE t (id integer PRIMARY KEY);
//	CREATE VIEW v AS SELECT id FROM t;
//
// with one operand varied and nothing else touched:
//
//	for = table.v   exit 1  Unsupported attribute; This object does not have
//	                        an attribute named "v"
//	for = view.v    exit 0
//	for = "v"       exit 0
//
// PostgreSQL reports the owner's implicit grants on a view exactly as it does
// on a table, so that is the DEFAULT invocation on any database carrying a
// view, with no selection involved (stokaro/ptah#1234).
//
// The set is what this render WRITES, not what the IR holds: a view with no
// body is warned about and skipped, and a sequence the Atlas-compatible surface
// omits is not there to be named either -- both asked through the same
// predicate the render itself uses, so the answer cannot drift from the
// document.
//
// "Exactly one" is what makes the kind knowable. Relations share one namespace
// per schema in every engine Ptah renders for, so two blocks under one label
// are two schemas' worth, and which of them an unqualified reference means is
// exactly the question [renderer.documentResolvesTableRef] refuses to guess.
// Saying no here is therefore routine rather than exotic -- two schemas each
// holding a view named `v` is all it takes, and that is a DEFAULT inspect of a
// realm-scoped URL -- so what the caller writes instead has to be a spelling
// the pinned binary reads. For a relation that is a quoted name; see
// [renderer.relationRef] for the measurement and for why the short form is not
// it.
func (r *renderer) documentDeclares(label string) (declaredBlock, bool) {
	if r.relationBlocks == nil {
		r.relationBlocks = r.declaredRelationBlocks()
	}
	blocks := r.relationBlocks[label]
	if len(blocks) != 1 {
		return declaredBlock{}, false
	}
	return blocks[0], true
}

func (r *renderer) declaredRelationBlocks() map[string][]declaredBlock {
	blocks := make(map[string][]declaredBlock, len(r.db.Tables)+len(r.db.Views)+len(r.db.MaterializedViews))
	declare := func(kind, label, schema string) {
		blocks[label] = append(blocks[label], declaredBlock{kind: kind, schema: r.schemaFor(schema)})
	}
	for _, table := range r.db.Tables {
		declare(blockTable, table.Name, table.Schema)
	}
	for _, view := range r.db.Views {
		if view.Body == "" {
			continue
		}
		declare(blockView, objectNameFromQualified(view.Name), schemaNameFromQualified(view.Name))
	}
	for _, view := range r.db.MaterializedViews {
		if view.Body == "" {
			continue
		}
		declare(blockMaterialized, objectNameFromQualified(view.Name), schemaNameFromQualified(view.Name))
	}
	for _, sequence := range r.db.Sequences {
		if r.omitsRefusedBlock(blockSequence, sequence.Name) {
			continue
		}
		// The label [renderer.renderSequences] writes is the canonical name, so
		// that is the label a reference has to match.
		sequence.Canonicalize()
		declare(blockSequence, sequence.Name, sequence.Schema)
	}
	return blocks
}

// documentResolvesTableRef reports whether an unqualified `table.<name>`
// reference in this document resolves to exactly the table the qualified name
// meant.
//
// It is true only when the document writes ONE table block with that label and
// that block carries this schema. Both halves are load-bearing:
//
//   - One block, because two tables of one name in different schemas are legal.
//     Measured on the pinned Atlas community binary v1.3.0 with `public.users`
//     and `other.users` both declared and an unqualified `ref_columns`, it
//     refuses the document with `specutil: failed converting to *schema.Realm:
//     multiple reference tables found for "users"`. Ptah's own reader is no
//     happier: goschema's resolveTableReference gives up on an ambiguous name
//     and leaves the reference unqualified, so dropping the schema there would
//     lose it silently -- worse than a refusal.
//   - That block's schema, because a lone `public.users` does not make
//     `table.users` mean `other.users`. Without this half a reference to a table
//     the document does not contain would resolve, quietly, to a different one.
func (r *renderer) documentResolvesTableRef(schema, name string) bool {
	if r.tableSchemas == nil {
		schemas := make(map[string][]string, len(r.db.Tables))
		for _, table := range r.db.Tables {
			schemas[table.Name] = append(schemas[table.Name], r.schemaFor(table.Schema))
		}
		r.tableSchemas = schemas
	}
	blocks := r.tableSchemas[name]
	return len(blocks) == 1 && blocks[0] == schema
}

// schemaFor returns the schema name to write for an object, which is the one it
// carries or, failing that, the schema the whole read belongs to.
//
// Every schema-scoped block an inspected render writes goes through it, and the
// reason is that the alternative to the attribute is not "no schema", it is
// "whichever schema replays it". A PostgreSQL catalog blanks the schema on
// exactly the objects the read's own search_path made implicit, so an object of
// the schema being inspected arrives with an empty Schema; a block written
// without the attribute is then created wherever the replaying connection's
// search_path points.
//
// This renderer writes nine schema-scoped block types -- sequence, domain,
// composite, range, enum, table, function, view, materialized. Measured on
// PostgreSQL 17.10 with one schema `wf1138s` holding one object of each,
// inspected and planned back into a fresh database, seven of the nine landed in
// `public`; only `enum` and `table` were already reaching this helper. See
// stokaro/ptah#1138, and stokaro/ptah#1276 for the two that were not.
//
// An empty default is the parse-and-re-render contract and is deliberately
// preserved: those callers got their IR from HCL, where an absent schema is
// what the author wrote, and synthesizing one would invent a declaration.
func (r *renderer) schemaFor(schema string) string {
	if strings.TrimSpace(schema) != "" {
		return schema
	}
	return r.defaultSchema
}

// referencedSchemas lists, in a stable order, every schema the body of this
// render actually wrote a `schema.<name>` reference to. It is empty outside an
// inspected render, because nothing is synthesized there.
//
// The set is COLLECTED from the rendered body rather than predicted from the
// IR, and that is the whole point. Predicting it has now been wrong three
// times, each time in the same direction and each time reaching the pinned
// Atlas community binary v1.3.0 as `There is no variable named "schema"`: the
// first version declared only the default and missed a table outside it, the
// second missed the enum block's own reference, and the third missed the
// `permission { for = schema.public }` that every PostgreSQL database has --
// visible on an EMPTY database, where there is no table to predict from at all
// (stokaro/ptah#1234). A reference and its declaration cannot disagree when one
// is derived from the other.
//
// Collecting also gets the suppressed blocks right for free. A block the
// Atlas-compatible surface leaves out never reaches [renderer.schemaRef], so an
// omitted sequence no longer conjures a schema block declaring nothing.
func (r *renderer) referencedSchemas() []string {
	if r.defaultSchema == "" {
		return nil
	}
	return sortedMapKeys(r.schemaRefs)
}

// render assembles the document in two passes. The body is rendered first and
// held aside, then the header and the schema blocks that body turned out to
// reference are written, then the body goes back on the end.
//
// Two things force that. Every schema a reference names has to be declared
// somewhere in the file, and Ptah writes the declarations at the top -- so the
// only way to declare exactly the set the body references is to have rendered
// the body before writing them.
func (r *renderer) render() {
	r.renderBody()
	body := r.builder.String()
	r.builder.Reset()

	// The coverage header must be written AFTER the body snapshot is taken and
	// the builder reset, or it lands inside body and is re-emitted below the
	// schema blocks instead of at the top of the document.
	r.builder.WriteString("// Code generated by ptah; DO NOT EDIT.\n")
	r.renderCoverageHeader()
	r.builder.WriteString("\n")
	r.renderSchemas()
	r.builder.WriteString(body)
}

func (r *renderer) renderBody() {
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
	// file references has to be declared here or the reference resolves to
	// nothing. A read that DID report one keeps what it reported, comment,
	// charset and collation included.
	//
	// Which schemas those are is answered by the body that was already
	// rendered, not guessed from the IR; see [renderer.referencedSchemas] for
	// the three guesses that were wrong.
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
		return enums[i].QualifiedName() < enums[j].QualifiedName()
	})
	for _, enum := range enums {
		r.linef(`enum %s {`, r.enumLabels(enum))
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
		// The schema is the ENUM'S, falling back to the render's default only
		// where the enum names none -- the rule renderTable follows, for the
		// same reason: a catalog blanks the schema exactly where the engine
		// treats the read's own schema as implicit.
		//
		// It used to be the render's default unconditionally, because
		// goschema.Enum carried no schema. That is right for every read of one
		// schema and wrong for every read of more than one: `schema inspect`
		// against a URL that pins no schema describes the whole realm, so
		// `extra.mood` was written as `schema = schema.public` -- an enum
		// attributed to a schema that does not hold it. Applying that document
		// created the type in `public` and typed `extra.b.feeling` against it,
		// so the round trip produced a database whose column type pointed at
		// the wrong schema (stokaro/ptah#1276).
		if schema := r.schemaFor(enum.Schema); schema != "" {
			r.rawAttr(1, "schema", r.schemaRef(schema))
		}
		r.rawAttr(1, "values", stringList(enum.Values))
		r.line("}")
		r.line("")
	}
}

// enumLabels writes an enum block's label list: `"mood"` normally, and
// `"other" "mood"` when the bare name is ambiguous in this document.
//
// Two objects must not be written with one label. Before this, a realm holding
// public.mood and other.mood was described by two blocks both headed
// `enum "mood"`, and reading that document back produced ONE enum -- Ptah's own
// inspect output described a database that does not exist, and no reader,
// including Ptah, could recover the second type from it (stokaro/ptah#1360).
//
// The two-label spelling is the pinned Atlas community binary v1.3.0's own.
// Measured on PostgreSQL 17.10, `schema inspect` of a database holding both
// emits `enum "other" "mood"` and `enum "public" "mood"`, and for a database
// holding one enum it emits the one-label `enum "mood"` -- so qualifying only
// when the name is ambiguous is that binary's rule, not a Ptah convention. Its
// loader then refuses the two-block document it just wrote (`duplicate enum
// "mood"`, exit 1, measured on its own output); Ptah's reads it under
// [go.5x5.cz/ptah/internal/atlashcl.SchemaScopedEnumsEnvVar], which is the
// round trip that binary does not have.
func (r *renderer) enumLabels(enum goschema.Enum) string {
	schema := r.schemaFor(enum.Schema)
	if schema == "" || !r.enumNameIsAmbiguous(enum.Name) {
		return quote(enum.Name)
	}
	return quote(schema) + " " + quote(enum.Name)
}

// enumNameIsAmbiguous reports whether more than one enum in this document
// carries the given bare name.
//
// It counts DISTINCT qualified names rather than blocks, so a document that
// somehow holds the same enum twice is still written with one label: this
// function decides how to spell an identity, and inventing a schema label for a
// repeat would hide the repeat behind two different-looking blocks.
func (r *renderer) enumNameIsAmbiguous(name string) bool {
	qualified := make(map[string]struct{}, 2)
	for _, enum := range r.db.Enums {
		if enum.Name != name {
			continue
		}
		qualified[r.schemaFor(enum.Schema)+"."+enum.Name] = struct{}{}
	}
	return len(qualified) > 1
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
		r.rawAttr(1, "schema", r.schemaRef(schema))
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
		// A key column is written NOT NULL only where the engine makes it so.
		// SQLite does not on a rowid table, and writing `null = false` there
		// made this document disagree with the same schema's `{{ json . }}`,
		// which reports the catalog's answer. It does on a STRICT or
		// WITHOUT ROWID table, so the decision needs the table and not only the
		// dialect. See stokaro/ptah#1235.
		if fieldIsPrimary(table, field) && r.keyColumnIsNotNull(table, fields, field) {
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

// keyColumnIsNotNull reports whether the engine writes this key column NOT NULL
// on its own.
//
// For SQLite that is a property of the table rather than of the dialect, so the
// answer comes from the table's shape; every other dialect answers from the
// dialect. An empty dialect keeps answering yes, which is what the
// parse-and-re-render callers of [Render] want: their input was HCL and its
// nullability is already the author's.
func (r *renderer) keyColumnIsNotNull(
	table goschema.Table,
	fields []goschema.Field,
	field goschema.Field,
) bool {
	if platform.NormalizeDialect(r.dialect) != platform.SQLite {
		return primaryKeyImpliesNotNull(r.dialect)
	}
	return sqlitekey.ImpliesNotNull(table, sqlitekey.KeyColumns(table, fields), field)
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
	r.rawAttr(2, "ref_columns", r.tableColumnRefs(foreignTable, foreignColumns))
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
		// `page_per_range`, singular, is the spelling the pinned Atlas community
		// binary v1.3.0 both emits and honors. Measured on PostgreSQL 17.10
		// against an otherwise identical HCL document: `page_per_range = 32`
		// planned CREATE INDEX ... USING brin ("ts") WITH (pages_per_range = 32),
		// while `pages_per_range = 32` was accepted at exit 0 with no diagnostic
		// and planned CREATE INDEX ... USING brin ("ts") -- the parameter simply
		// gone. Emitting the plural therefore hands the pinned binary a document
		// that silently loses the parameter. Ptah's own parser accepts both, so
		// no document that used to load stops loading. See #1242.
		r.rawAttr(2, "page_per_range", pages)
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
			r.renderIndexPartNullsOrder(part.NullsOrder)
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

// renderIndexPartNullsOrder writes the NULLS ordering of one index key.
//
// The rendered value is whatever the part carries. Deciding that an ordering
// is redundant belongs to whoever produced the part -- the live-database reader
// records only an ordering that deviates from the direction's default -- and
// an author who spelled one out gets it back. The two attribute names are the
// spelling the community binary's own inspect output uses, so a rendered file
// stays readable by both (issue #1272).
func (r *renderer) renderIndexPartNullsOrder(order string) {
	switch strings.ToUpper(strings.TrimSpace(order)) {
	case goschema.NullsOrderFirst:
		r.rawAttr(3, "nulls_first", "true")
	case goschema.NullsOrderLast:
		r.rawAttr(3, "nulls_last", "true")
	}
}

// simpleIndexParts reports whether the parts carry nothing the compact
// `columns = [...]` spelling would lose. Every field the `on` block can express
// has to be listed: a part carrying only a NULLS ordering is not simple, and
// omitting that check is how the ordering used to disappear from rendered HCL
// even after #1271 taught the reader to preserve it.
func simpleIndexParts(parts []goschema.IndexPart) bool {
	for _, part := range parts {
		if part.Expr != "" || part.Operator != "" || part.Prefix != "" ||
			part.Desc || part.NullsOrder != "" {
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
// The reference is unqualified while the name is unambiguous, because that
// binary keys enum blocks by name alone: an `enum.status` reference resolves to
// a block declared in a non-default schema (measured with table and enum both in
// `reporting`). It gains the schema exactly where the BLOCK gains it, so the
// reference and the label it points at always have the same number of parts --
// see [renderer.enumLabels]. That is also the pinned binary's own rule:
// measured on PostgreSQL 17.10, its inspect of a realm holding public.mood and
// other.mood writes `type = enum.public.mood` and `type = enum.other.mood`
// beside the two two-label blocks.
//
// A name that also names a domain, composite or range is left alone. Those are
// separate declarations in the same document, and a reference into the enum
// block would point at the wrong one.
func (r *renderer) enumTypeRef(typeName string) (string, bool) {
	name := strings.TrimSpace(typeName)
	if name == "" {
		return "", false
	}
	enum, found := r.referencedEnum(name)
	if !found {
		return "", false
	}
	if slices.ContainsFunc(r.db.Domains, func(d goschema.Domain) bool { return d.Name == enum.Name }) ||
		slices.ContainsFunc(r.db.CompositeTypes, func(ct goschema.CompositeType) bool { return ct.Name == enum.Name }) ||
		slices.ContainsFunc(r.db.Ranges, func(rg goschema.Range) bool { return rg.Name == enum.Name }) {
		return "", false
	}
	if schema := r.schemaFor(enum.Schema); schema != "" && r.enumNameIsAmbiguous(enum.Name) {
		return "enum" + objectRefPart(schema) + objectRefPart(enum.Name), true
	}
	return "enum" + objectRefPart(enum.Name), true
}

// referencedEnum finds the enum a column's type names.
//
// A catalog read reports an ambiguous enum type qualified (`other.mood`) and an
// unambiguous one bare (`mood`), and a hand-written document can spell either,
// so both are looked up. The qualified match is tried first: a bare match would
// otherwise pick whichever same-named enum came first in the slice, which is the
// silent wrong-schema attribution stokaro/ptah#1276 is about.
func (r *renderer) referencedEnum(name string) (goschema.Enum, bool) {
	for _, enum := range r.db.Enums {
		if enum.QualifiedName() == name {
			return enum, true
		}
	}
	for _, enum := range r.db.Enums {
		if enum.Name == name && !r.enumNameIsAmbiguous(name) {
			return enum, true
		}
	}
	return goschema.Enum{}, false
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

// userTypeExpr renders the type a user-defined type is built on: a domain's
// base type, a composite field's type, a range's subtype.
//
// These are not column type positions and do not follow the column rules. The
// pinned Atlas community binary v1.3.0 reads only a sql() call in the first two,
// measured with everything else held fixed:
//
//	domain    type = text          refused, There is no variable named "text"
//	domain    type = "text"        refused, schemahcl: failed reading spec
//	domain    type = sql("text")   accepted
//	composite type = text          refused
//	composite type = "text"        refused
//	composite type = sql("text")   accepted
//
// A range is the odd one, and it is why this was measured rather than inferred
// from the other two:
//
//	range     subtype = int4          refused
//	range     subtype = "int4"        ACCEPTED
//	range     subtype = sql("int4")   accepted
//
// It takes the quoted string the other two refuse. sql() is used for all three
// because it is the one spelling every position accepts, and one rule beats
// three. Ptah's own parser reads it back to the bare name in each -- text, text,
// int4 -- so the round trip is unaffected (stokaro/ptah#1260).
//
// An empty value keeps typeExpr's behavior rather than becoming sql(""), which
// would round trip as a type named nothing.
func userTypeExpr(value string) string {
	if strings.TrimSpace(value) == "" {
		return typeExpr(value)
	}
	return sqlCall(value)
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

// schemaRef renders a reference to a schema block and records that this
// document now needs one, which is what [renderer.renderSchemas] declares.
//
// Every position that writes `schema.<name>` goes through here so that no
// position can be added without its declaration following. That is the property
// the previous shape lacked: a reference was written in one place and predicted
// in another, and the two drifted apart every time a new position was added.
func (r *renderer) schemaRef(name string) string {
	if name != "" {
		if r.schemaRefs == nil {
			r.schemaRefs = map[string]bool{}
		}
		r.schemaRefs[name] = true
	}
	return "schema" + objectRefPart(name)
}

func (r *renderer) tableColumnRefs(table string, columns []string) string {
	tableRef := r.tableRef(table)
	refs := make([]string, 0, len(columns))
	for _, column := range columns {
		if table == "" || column == "" {
			continue
		}
		refs = append(refs, tableRef+".column"+objectRefPart(column))
	}
	return "[" + strings.Join(refs, ", ") + "]"
}
