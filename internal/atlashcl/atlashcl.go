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
	"github.com/zclconf/go-cty/cty/convert"

	"go.5x5.cz/ptah/core/coverage"
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

	// RecordSchemaBlock receives every TOP-LEVEL `schema` block the document
	// declares, in file order, one call per block. Optional; nil discards them.
	//
	// It reports blocks rather than schemas because those are two different
	// numbers and the caller needs the first: `goschema.Finalize` folds two
	// `schema "main"` blocks into one schema, and the pinned Atlas community
	// binary v1.3.0 counts that document as two. See [declaredSchemaBlocks].
	//
	// The recorder fires for a document that parses, whatever the caller then
	// does with the count. Deciding what "too many" means needs facts this
	// parser does not have -- which URL the run is limited to -- so the rule
	// lives with the caller that knows them
	// (go.5x5.cz/ptah/internal/schemafile.Options.SchemaScope).
	RecordSchemaBlock func(SchemaBlock)

	// Vars supplies values for the file's `variable` blocks, spelled the way
	// `--var` spells them: one entry per flag occurrence, each entry a
	// comma-separated list of name=value assignments.
	//
	// A name with no matching variable block is ignored, matching the pinned
	// Atlas community binary v1.3.0: `--var nosuch=1` against a file declaring
	// `variable "v"` still fails with `missing value for required variable
	// "v"` rather than complaining about the unused override.
	Vars []string

	// VarValues supplies the same overrides already decoded, for a caller that
	// holds structured values rather than flag text.
	//
	// It exists because [Options.Vars] carries the `--var` GRAMMAR, not just its
	// data: one entry is read as a CSV record, so a value containing a comma
	// comes back split in two. The atlas.hcl `data "hcl_schema" { vars }` map is
	// already decoded when Ptah reaches it, and re-spelling it as flag text to
	// have this package parse it again would corrupt exactly those values.
	//
	// An entry here wins over the same name in Vars. Nothing in Ptah sets both
	// today -- a schema file selected by a data source takes that block's vars
	// and no `--var` at all, which is what the pinned community binary v1.3.0
	// does -- but the rule has to be stated for the field to mean anything.
	VarValues map[string]string
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

	schemaBlocks := declaredSchemaBlocks(body)
	p := parser{
		src:             data,
		filename:        filename,
		sourceDir:       filepath.Dir(filename),
		db:              &goschema.Database{},
		tolerant:        opts.IgnoreUnknownNames,
		recordIgnored:   opts.RecordIgnored,
		refContext:      columnRefContext(body),
		declaredSchemas: schemaBlockNames(schemaBlocks),
	}
	// Classify before validating the schema body. A file carrying a project-file
	// marker is the wrong kind of file, and that verdict must not depend on
	// whether some unrelated block later in the file also happens to be invalid.
	// Measured on a fixture holding both an env block and a bogus table block:
	// the pinned Atlas community binary reports the project-file error, not the
	// block error, so the classification has to run ahead of the body walk.
	if err := classifyProjectFile(body, filename); err != nil {
		return nil, err
	}
	// After the project-file verdict, so a document that is not a schema file at
	// all never contributes a schema block to a caller's count.
	if opts.RecordSchemaBlock != nil {
		for _, block := range schemaBlocks {
			opts.RecordSchemaBlock(block)
		}
	}
	// The evaluation context is built from the file's own variable and locals
	// blocks, so it has to exist before any attribute is read -- including the
	// sql() arguments the next guard evaluates.
	ctx, err := newEvalContext(body, opts.Vars, opts.VarValues, p.printLine)
	if err != nil {
		return nil, err
	}
	p.ctx = ctx
	// Refuse malformed sql() calls before the body walk, not during it. Every
	// value helper below falls back to an attribute's source text when the
	// expression will not evaluate, so a call this guard let through would be
	// rendered into DDL verbatim -- issue #1106.
	if err := p.rejectMalformedSQLRawExprs(body); err != nil {
		return nil, err
	}
	// Same reason, for the expressions the context above was built to resolve:
	// an unresolved var. reference would otherwise reach DDL as its own source
	// text -- issue #926.
	if err := p.rejectUnresolvedExprs(body); err != nil {
		return nil, err
	}
	// The walk is the one pass whose evaluations are the document's own. The two
	// guards above evaluate every expression as well, to refuse a malformed
	// sql() call and an unresolved var. reference before either can reach DDL,
	// and a `print` call would otherwise fire once per pass -- three lines for
	// one call. That binary prints one (stokaro/ptah#1627).
	p.emitting = true
	if err := p.parseBody(body); err != nil {
		return nil, err
	}
	p.emitting = false
	// After the walk, because a reference may name a block declared further
	// down the file, and before Finalize, which reads schemas off the same
	// blocks for the positions it covers.
	p.resolveDocumentTableRefs()
	// Before Finalize, which folds a repeated declaration into the first one and
	// is what turned a document the pinned binary refuses into exit 0.
	if err := p.rejectRedeclarations(); err != nil {
		return nil, err
	}
	goschema.Finalize(p.db)
	// A document's own account of its limits is part of the document. It rides
	// in the leading comment header rather than in a block, because it has to
	// survive being read by tools that are not Ptah -- the pinned Atlas
	// community binary v1.3.0 reads a document carrying it at exit 0 -- and
	// because a block would need a name that binary refuses.
	notDescribed, err := coverage.DecodeHeader(string(data))
	if err != nil {
		return nil, fmt.Errorf("parse HCL schema %s: %w", filename, err)
	}
	p.db.NotDescribed = notDescribed
	return p.db, nil
}

// projectFileBlocks names the top-level blocks that mark an HCL file as an Atlas
// project file (atlas.hcl) rather than a schema file. Handing a project file to a
// schema source is a wrong-kind-of-file mistake: it has no schema objects, so
// parsing it as a schema silently yields an EMPTY desired state, and the caller
// plans to drop everything the real schema contains.
//
// The set is deliberately exactly one name. It comes from a measurement, not a
// guess: each of ten top-level block names was prepended to a valid schema file
// and run through `schema inspect` on the pinned Atlas community binary (v1.3.0).
// Only `env` made it refuse the file. `atlas`, `lint`, `diff`, `format`,
// `docker`, `run` and `locals` are accepted and silently dropped there; Ptah
// already rejects those as unsupported top-level blocks, which is stricter and
// safe, so they are not added here. `variable` is a legitimate schema-file
// construct -- see the case arm in parseTopLevelBlock.
//
// Extend this set only from a new measurement against the pinned binary.
var projectFileBlocks = map[string]bool{"env": true}

// classifyProjectFile refuses a schema file that carries a project-file marker.
//
// It inspects TOP-LEVEL blocks only, which is what makes the three neighboring
// shapes behave correctly: a nested `env` block inside a table keeps its existing
// "unsupported table block" error, an `env = "x"` ATTRIBUTE is not a block at all
// and never triggers, and only a file-scope `env` block is classified. Contents
// and labels are irrelevant -- an unlabeled env, an empty body and a body full of
// nonsense all mark the file, matching the measured trigger.
func classifyProjectFile(body *hclsyntax.Body, filename string) error {
	for _, block := range body.Blocks {
		if projectFileBlocks[block.Type] {
			return projectFileError(filename, block)
		}
	}
	return nil
}

// projectFileError builds the refusal. The leading sentence is the one the Atlas
// community binary emits, kept verbatim so existing scripts that match on it keep
// working; the clause after the colon is Ptah going past that binary, which names
// the file but never the construct, leaving a user who pasted a fragment with
// nothing to act on. Naming the offending block and its position is additive.
//
// The quoted path is spelled the way the caller handed it to Parse. Ptah's
// schema-file loader resolves paths before parsing, so in practice this prints an
// absolute path where the community binary prints the path as typed.
func projectFileError(filename string, block *hclsyntax.Block) error {
	return fmt.Errorf(
		"cannot parse project file %q as a schema file: top-level %q block at %s is a project-file construct",
		filename, block.Type, block.TypeRange.String(),
	)
}

type parser struct {
	src       []byte
	filename  string
	sourceDir string
	db        *goschema.Database

	// ctx carries the var. and local. namespaces and the function set every
	// attribute is evaluated against. It is never nil once Parse has built it.
	ctx *hcl.EvalContext

	// tolerant enables the unknown-name policy described on Options.
	tolerant bool
	// recordIgnored receives the names dropped under that policy. Nil discards
	// them.
	recordIgnored func(IgnoredName)
	// refContext decides a conditional inside a column reference. Built once
	// from the file's own `variable` blocks -- see columnRefContext.
	refContext *hcl.EvalContext
	// declaredSchemas holds the labels of the file's top-level `schema`
	// blocks, collected before the walk so a dropped body can name one that is
	// declared further down -- see droppedSchemaRoot.
	declaredSchemas []string
	// pendingForeignRefs holds the single-column foreign key targets that can
	// only be read once every table block is known; see
	// [parser.resolveDocumentTableRefs].
	pendingForeignRefs []pendingForeignRef
	// emitting is true only during the body walk, which is the pass whose
	// evaluations produce the document. See [parser.printLine].
	emitting bool

	// dynamicIterators names the iteration roots bound by the dynamic blocks
	// currently being expanded, outermost first. See [parser.expandDynamic].
	dynamicIterators []string

	// declaredForeignKeys holds one entry per `foreign_key` block the document
	// declared, recorded at the block because a single-column key leaves no
	// distinguishable trace in the IR; see [parser.recordForeignKey].
	declaredForeignKeys []declaration
}

func (p *parser) parseBody(body *hclsyntax.Body) error {
	for _, block := range body.Blocks {
		if block.Type == dynamicBlockType {
			if err := p.expandDynamic(block, p.parseTopLevelBlock); err != nil {
				return err
			}
			continue
		}
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
	case "env":
		// Parse classifies a file carrying a top-level env block before the body
		// walk begins, so this arm is a guard rather than the usual path. It
		// returns the identical error so the message cannot drift between the
		// two routes.
		return projectFileError(p.filename, block)
	case variableBlockType, localsBlockType:
		// Consumed already; do not fold either name into the env arm.
		//
		// Both are genuine schema-file constructs in Atlas: the community
		// binary accepts them, EVALUATES var.X and local.X references against
		// them, and fails with `missing value for required variable %q` only
		// when a typed variable has neither a default nor a --var override.
		// Measured on the pinned binary, a schema file whose column default is
		// var.status:
		//
		//   variable "status" { type = string, default = "active" }
		//     -> exit 0, renders `default = "active"`
		//
		// The `type` argument carries the example. Drop it and the same file
		// exits 1 with `The argument "type" is required`, which would argue for
		// refusing `variable` -- the exact conclusion this comment exists to
		// prevent. Quote the typed spelling or the measurement says the opposite
		// of what it is cited for.
		//
		// newEvalContext read both block kinds before this walk started, which
		// is where their validation and their `missing value` refusal live.
		// Reaching them again here would parse a type constraint as a value.
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

// enumLabels reads an `enum` block's labels the way [parser.tableLabels] reads a
// table's: one label names the enum, two name its schema and then the enum.
//
// The two-label spelling is not a Ptah invention. It is what the pinned Atlas
// community binary v1.3.0 WRITES when one bare enum name is ambiguous in a
// realm: measured on PostgreSQL 17.10, its `schema inspect` of a database
// holding public.mood and other.mood emits `enum "other" "mood"` and
// `enum "public" "mood"`, and it reads a document holding a single two-label
// block at exit 0. Ptah refused that document with "enum block requires exactly
// one label", so a file that binary wrote was unreadable here.
//
// A schema label that contradicts an explicit `schema =` attribute is an error
// rather than a precedence question, which is the rule `table` already applies.
func (p *parser) enumLabels(block *hclsyntax.Block) (tableLabels, error) {
	schemaAttr := p.optionalRefName(block.Body.Attributes["schema"])
	switch len(block.Labels) {
	case 1:
		return tableLabels{schema: schemaAttr, name: block.Labels[0]}, nil
	case 2:
		if schemaAttr != "" && schemaAttr != block.Labels[0] {
			return tableLabels{}, p.blockError(
				block,
				"enum %q schema label conflicts with schema attribute %q",
				block.Labels[1], schemaAttr,
			)
		}
		return tableLabels{schema: block.Labels[0], name: block.Labels[1]}, nil
	default:
		return tableLabels{}, p.blockError(block, "enum block requires one or two labels")
	}
}

func (p *parser) parseEnum(block *hclsyntax.Block) error {
	labels, err := p.enumLabels(block)
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedEnumAttrs(block); err != nil {
		return err
	}
	values, err := p.stringListAttr(block, "values")
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return p.blockError(block, "enum %q requires values", labels.name)
	}
	// The `schema` attribute is read rather than merely tolerated. It was
	// accepted by rejectUnsupportedEnumAttrs and then discarded, so a document
	// declaring `enum "mood" { schema = schema.extra }` was read back as an
	// enum belonging to nothing, and applying it created the type in whatever
	// schema the connection defaulted to (stokaro/ptah#1276). A `function`
	// block's schema has always been read here; an enum's is the same fact.
	p.db.Enums = append(p.db.Enums, goschema.Enum{
		Name:   labels.name,
		Schema: labels.schema,
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
		if nested.Type == dynamicBlockType {
			if err := p.expandDynamic(nested, func(generated *hclsyntax.Block) error {
				if generated.Type == "check" && len(generated.Labels) == 0 {
					unlabeledCheckOrdinal++
				}
				return p.parseTableBlock(&table, fieldsStart, unlabeledCheckOrdinal, generated)
			}); err != nil {
				return err
			}
			continue
		}
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
		// Recorded before it is applied, because applying a single-column key
		// writes it onto a field where a second application is invisible.
		p.recordForeignKey(*table, spec.name)
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

	columnType, typeRawSQL := p.columnTypeName(block, typeAttr)

	field := goschema.Field{
		StructName:          structName,
		FieldName:           name,
		Name:                name,
		Type:                columnType,
		TypeRawSQL:          typeRawSQL,
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
		nullsOrder, err := p.indexPartNullsOrder(nested)
		if err != nil {
			return nil, nil, err
		}
		operator := p.optionalSQLExpression(nested.Body.Attributes["ops"])
		prefix := p.optionalRawExpr(nested.Body.Attributes["prefix"])
		if columnAttr != nil {
			column, err := p.columnNameFromExpr(nested, "index on", columnAttr)
			if err != nil {
				return nil, nil, err
			}
			columns = append(columns, column)
			parts = append(parts, goschema.IndexPart{
				Name:       column,
				Operator:   operator,
				Prefix:     prefix,
				Desc:       desc,
				NullsOrder: nullsOrder,
			})
			continue
		}
		if prefix != "" {
			return nil, nil, p.blockError(nested, "index on prefix requires column")
		}
		expr := p.exprString(exprAttr)
		columns = append(columns, expr)
		parts = append(parts, goschema.IndexPart{
			Expr:       expr,
			Operator:   operator,
			Desc:       desc,
			NullsOrder: nullsOrder,
		})
	}
	return columns, parts, nil
}

// indexPartNullsOrder reads the NULLS ordering of one index key.
//
// The two attributes are the spelling the community binary's own
// `schema inspect` emits for a PostgreSQL index, so a file it produced was
// reaching Ptah with the ordering silently dropped by the unknown-attribute
// tolerance -- the property was accepted and then ignored. Only an ordering
// that deviates from the direction's default is recorded, matching what
// #1271's reader does with pg_index.indoption, so an explicit
// `nulls_last = true` on an ascending key stays equal to an omitted one
// instead of planning a rebuild (issue #1272).
//
// Setting both to true is refused rather than resolved: a key has one NULLS
// ordering, and picking one of the two for the author would be a guess.
func (p *parser) indexPartNullsOrder(block *hclsyntax.Block) (string, error) {
	first, err := p.optionalIndexOnBool(block, "nulls_first", false)
	if err != nil {
		return "", err
	}
	last, err := p.optionalIndexOnBool(block, "nulls_last", false)
	if err != nil {
		return "", err
	}
	if first && last {
		return "", p.blockError(block, "index on cannot set both nulls_first and nulls_last")
	}
	if first {
		return goschema.NullsOrderFirst, nil
	}
	if last {
		return goschema.NullsOrderLast, nil
	}
	return "", nil
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
			name, err := p.columnNameFromExpr(nested, "partition by", columnAttr)
			if err != nil {
				return nil, err
			}
			parts = append(parts, goschema.PartitionPart{Name: name})
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
		name, err := p.columnNameFromExpr(nested, "primary_key on", attr)
		if err != nil {
			return nil, err
		}
		parts = append(parts, goschema.PrimaryKeyPart{
			Name:   name,
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
		p.pendingForeignRefs = append(p.pendingForeignRefs, pendingForeignRef{
			field:  i,
			owner:  table,
			table:  spec.foreignTable,
			column: spec.foreignColumns[0],
		})
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
		table, column := p.tableColumnFromExpr(expr)
		if column == "" {
			return nil, p.blockError(block, "%s contains unsupported reference %q", attrName, p.rawExprNode(expr))
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
		"column":      true,
		"expr":        true,
		"ops":         true,
		"prefix":      true,
		"desc":        true,
		"nulls_first": true,
		"nulls_last":  true,
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

// markPrimaryFields moves a single-column table-level primary key onto the
// column so it renders inline. The column's `null` attribute is left as the
// document wrote it: measured on the pinned Atlas community v1.3.0 binary,
// `schema apply` from a table whose key column says `null = true` builds a
// SQLite table whose `pragma table_info.notnull` is 0 for that column, so
// clearing the flag here would apply a stricter table than the document asked
// for. Where the flag is absent the HCL parser already defaults it to NOT NULL.
// See stokaro/ptah#1235.
func markPrimaryFields(fields []goschema.Field, columns []string) {
	if len(columns) != 1 {
		return
	}
	for i := range fields {
		if fields[i].Name == columns[0] {
			fields[i].Primary = true
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
	if value, ok := p.sqlRawExprValue(attr.Expr); ok {
		return value
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
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() || value.Type() != cty.String {
		return "", p.blockError(block, "%s attribute %q must be a string", label, name)
	}
	return value.AsString(), nil
}

func (p *parser) optionalBool(attr *hclsyntax.Attribute, fallback bool) bool {
	if attr == nil {
		return fallback
	}
	value, diags := attr.Expr.Value(p.ctx)
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
	value, diags := attr.Expr.Value(p.ctx)
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
	value, diags := attr.Expr.Value(p.ctx)
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
	value, diags := attr.Expr.Value(p.ctx)
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
	if value, ok := p.sqlRawExprValue(attr.Expr); ok {
		return value
	}
	// A var. or local. reference resolves here too. Everything else this helper
	// reads -- `on = table.users`, `to = [role.app]`, a bare argument type --
	// is a reference spelled as source text and stays one.
	if mustEvaluate(attr.Expr) {
		if text, ok := p.evaluatedText(attr.Expr); ok {
			return text
		}
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

// sqlExpression reports whether the attribute is Atlas's sql() raw expression,
// and reduces it to the SQL it carries. Callers that branch on ok distinguish a
// SQL expression from a literal value -- a column default, for instance.
//
// The match is structural, on the parsed call. It used to be textual, on the
// attribute's source: a `sql(` prefix and a `)` suffix. That accepted anything
// those two bytes bracketed, so `default = sql("1") + sql("2")` was read as the
// SQL text `"1") + sql("2` and planned as `DEFAULT "1") + sql("2"`.
func (p *parser) sqlExpression(attr *hclsyntax.Attribute) (string, bool) {
	return p.sqlRawExprValue(attr.Expr)
}

func (p *parser) exprString(attr *hclsyntax.Attribute) string {
	// sql() lands here from every attribute Ptah reads as a plain string, which
	// is most of the grammar. Reduce it: the fallback below hands the
	// attribute's SOURCE TEXT to the renderer, and that is what put
	// `CHECK (sql("n > 0"))` into a plan -- issue #1106.
	if value, ok := p.sqlRawExprValue(attr.Expr); ok {
		return value
	}
	if text, ok := p.evaluatedText(attr.Expr); ok {
		return text
	}
	return p.rawExpr(attr)
}

// evaluatedText returns an expression's evaluated value as the text Ptah stores
// in the schema IR.
//
// A string result is taken whatever the expression was, which is what the nil
// evaluation context already did for a quoted literal. A number or bool result
// is taken only for an expression [mustEvaluate] claims -- `var.n`, `max(1,2)`
// -- because for a plain numeric literal the source text and the formatted
// value can differ (`1.50` formats as `1.5`) and the source text is what every
// existing schema was written against.
func (p *parser) evaluatedText(expr hclsyntax.Expression) (string, bool) {
	value, diags := expr.Value(p.ctx)
	if diags.HasErrors() || value.IsNull() || !value.IsKnown() {
		return "", false
	}
	if value.Type() == cty.String {
		return value.AsString(), true
	}
	if !mustEvaluate(expr) {
		return "", false
	}
	converted, err := convert.Convert(value, cty.String)
	if err != nil {
		return "", false
	}
	return converted.AsString(), true
}

// columnTypeName returns the column's type and whether it was written with
// Atlas's sql() escape hatch.
//
// The type itself is always the reduced SQL text, so the DDL Ptah renders stays
// valid -- issue #1106. The second result is what a writer needs to put the
// call back: `sql("USER_DEFINED")` is the only spelling of an engine type Atlas
// does not model that the pinned Atlas community binary v1.3.0 accepts, and it
// refuses the bare identifier Ptah would otherwise write back
// ("Unknown column.type; There is no type named \"USER_DEFINED\"").
func (p *parser) columnTypeName(block *hclsyntax.Block, attr *hclsyntax.Attribute) (string, bool) {
	rawType := p.rawExpr(attr)
	if enumName, ok := strings.CutPrefix(rawType, "enum."); ok {
		return enumName, false
	}
	_, rawSQL := p.sqlRawExprValue(attr.Expr)
	typ := p.exprString(attr)
	if p.optionalBool(block.Body.Attributes["unsigned"], false) && !strings.Contains(strings.ToLower(typ), "unsigned") {
		// `unsigned = true` is a Ptah-side edit to the type, so the source call
		// no longer spells what the column holds and must not be written back.
		return typ + " unsigned", false
	}
	return typ, rawSQL
}

func (p *parser) stringListAttr(block *hclsyntax.Block, attrName string) ([]string, error) {
	attr := block.Body.Attributes[attrName]
	if attr == nil {
		return nil, nil
	}
	value, diags := attr.Expr.Value(p.ctx)
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

// columnNameFromExpr reads a `column` attribute as a column reference and
// refuses a value that cannot name a column at all.
//
// This is the apply-time half of issue #1106. Reducing Atlas HCL's sql() escape
// hatch to the SQL text it carries fixed every position that reads a plain
// STRING, but a `column` attribute reads a REFERENCE, and SQL text is not one.
// Ptah swallowed the mismatch: columnNameFromRef yields "" for any value it
// cannot resolve, and the empty name went straight into DDL. Measured before
// this refusal existed, `index "u" { unique = true, on { column = sql("n") } }`
// planned AND applied `CREATE UNIQUE INDEX "u" ON "t" ("")` at exit 0, leaving a
// table that accepts exactly one row -- sqlite indexes every row on the same
// constant, so the second INSERT fails on a uniqueness rule the schema never
// asked for. The primary_key form dropped the key without a word. The pinned
// Atlas community binary v1.3.0 refuses all of it: `expected value to be a
// *Ref, got: *schemahcl.RawExpr`.
//
// The refusal is narrower than the blanket one rejected when sql() reduction
// landed. It fires only where Ptah has no column name to render, so every value
// Ptah could already name is untouched -- a reference, or the quoted-string
// spelling Ptah accepts and that binary does not. Raw SQL in an index part has
// its own attribute, `expr`, which takes it and renders it.
//
// What "has no column name" means is tableColumnFromExpr's business, and it is a
// question about the PARSED EXPRESSION. Asking the attribute's source text
// instead was issue #1182: it refused five spellings the pinned binary plans,
// because text that is not a bare traversal reads the same whether it wraps a
// column reference or carries no reference at all.
func (p *parser) columnNameFromExpr(block *hclsyntax.Block, label string, attr *hclsyntax.Attribute) (string, error) {
	_, name := p.tableColumnFromExpr(attr.Expr)
	if name == "" {
		return "", p.blockError(block, "%s column contains unsupported reference %q", label, p.rawExpr(attr))
	}
	return name, nil
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

// printLine is what the `print` function writes through. It drops everything
// outside the body walk, so the guards that evaluate every expression ahead of
// the walk do not each produce their own copy of the line.
func (p *parser) printLine(line string) {
	if !p.emitting {
		return
	}
	fmt.Fprintln(printDestination, line)
}
