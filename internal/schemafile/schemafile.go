// Package schemafile loads local schema definition files into Ptah's schema IR.
package schemafile

import (
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/internal/yamlschema"
)

// Options configures schema file loading.
type Options struct {
	Dialect string
	// IgnoreUnknownHCLNames accepts and drops HCL names Ptah's schema HCL
	// parser does not model instead of refusing the file.
	//
	// The split is by COMMAND TREE, not by file format. It is set only by
	// cmd/atlas -- `schema apply`, `schema inspect`, `schema diff`,
	// `schema plan`, `schema plan validate` and `migrate diff` on the
	// Atlas-compatible binary -- which consumes files written for another tool
	// and must not refuse a construct that tool accepts and ignores.
	//
	// It is left off everywhere else, including the native `ptah schema`
	// commands that share this loader through internal/atlassource: there the
	// file is read by Ptah's own CLI, where an unmodeled name is a user error
	// worth naming rather than dropping. Every shared option struct on that
	// path carries the flag explicitly rather than defaulting it on, because
	// defaulting it on is exactly how the native commands became tolerant
	// without anyone intending it.
	//
	// There is deliberately no companion hook for reporting what was dropped.
	// [atlashcl.Options.RecordIgnored] offers one, but nothing on this path
	// consumes it, and a forwarding field with no producer is a field no test
	// can hold to account.
	IgnoreUnknownHCLNames bool

	// Vars supplies values for the `variable` blocks of an HCL schema file, as
	// `--var` spells them. See [atlashcl.Options.Vars].
	//
	// Other schema file formats ignore it: YAML and SQL have no variables, and
	// silently accepting a value for one there would suggest they do.
	Vars []string

	// SchemaScope names the one schema this run is limited to. Empty means the
	// run is realm-scoped and every schema the source declares is reachable.
	//
	// When it is set, an HCL desired state declaring more than one top-level
	// `schema` block is REFUSED. Ptah otherwise loaded such a document, narrowed
	// it to the scope and reported success, so `schema diff --from one.hcl --to
	// two-schemas.hcl` answered "Schemas are synced" and `migrate diff` wrote a
	// migration covering half the document -- silently, at exit 0. The pinned
	// Atlas community binary v1.3.0 refuses the same run with `cannot use HCL
	// with more than 1 schema when <flag> is limited to schema %q`.
	//
	// Unlike [Options.IgnoreUnknownHCLNames] this is NOT split by command tree.
	// Every caller that knows the URLs its run is scoped by sets it, and the
	// native `ptah schema inspect` and `ptah schema diff` reach this loader
	// through the same shared packages, so they gate too. Measured with the
	// derivation removed and put back, `ptah schema inspect --schema-file
	// two-schemas.hcl --dev-url sqlite://dv?mode=memory` went from exit 0
	// describing one schema to exit 2 naming both blocks.
	//
	// That is deliberate rather than an accident of sharing. Narrowing a desired
	// state to the scope and reporting success is a wrong answer on any surface,
	// and the escape hatch is the run's own URL: a realm-scoped one describes
	// every schema the document declares. Callers with no URL to derive from --
	// a Go-annotation desired state, internal/schemaload, `ptah schema
	// test` -- leave it empty and are unaffected.
	//
	// Use [ScopeFromURLs] to derive it, so every caller picks the same flag.
	SchemaScope string

	// SchemaScopeFlag names the flag that limited the run, spelled the way the
	// community binary's own message spells it -- `dev-url`, `url`, with no
	// leading dashes -- so the leading sentence stays byte-identical to the one
	// a script may be matching on. Ignored when SchemaScope is empty.
	SchemaScopeFlag string

	// collect gathers the top-level `schema` blocks of every HCL file one load
	// reached, so the [Options.SchemaScope] gate counts the whole desired state
	// -- a directory of HCL files, or several --to files -- rather than one file
	// at a time. It is unexported because only the entry points below may own
	// it: a caller-supplied collector would make the gate run twice for a
	// directory, once per entry and once for the whole.
	collect *schemaBlockCollector
}

// schemaBlockCollector accumulates the top-level `schema` blocks of one load.
type schemaBlockCollector struct {
	blocks []atlashcl.SchemaBlock
}

func (c *schemaBlockCollector) record(block atlashcl.SchemaBlock) {
	c.blocks = append(c.blocks, block)
}

// recordSchemaBlock is the recorder handed to the HCL parser, nil when no entry
// point owns a collector (which is every caller that left SchemaScope empty as
// well as the nested loads of a directory's entries).
func (o Options) recordSchemaBlock() func(atlashcl.SchemaBlock) {
	if o.collect == nil {
		return nil
	}
	return o.collect.record
}

// ScopeFromURLs picks the schema an Atlas-compatible run is limited to, and
// names the flag that limited it.
//
// The dev URL is consulted before the target because that is the order the
// pinned Atlas community binary v1.3.0 reports. Measured on PostgreSQL 17.10
// with a two-schema HCL desired state, one throwaway database per side:
//
//	--url realm  --dev-url ?search_path=public  ->  "…when dev-url is limited to schema \"public\""
//	--url ?search_path=public  --dev-url realm  ->  "…when url is limited to schema \"public\""
//
// so a run with both limited reports dev-url, which is what `schema apply` on
// two SQLite URLs prints.
//
// An empty targetURL is the shape `schema inspect` and `migrate diff` have:
// there is no second URL to consult. targetFlag is the flag that URL came from,
// spelled without leading dashes to match the binary's message, so
// `schema plan validate` reports `from` rather than a flag it does not have.
func ScopeFromURLs(devURL, targetURL, targetFlag string) (scope, flag string) {
	if selected, limited := schemaselection.URLScope(devURL); limited {
		return selected, "dev-url"
	}
	if selected, limited := schemaselection.URLScope(targetURL); limited {
		return selected, targetFlag
	}
	return "", ""
}

// gateSchemaScope runs one load with a collector attached and applies the
// [Options.SchemaScope] gate to everything it reached.
//
// A nested load -- a directory entry, or one of several --to files -- finds the
// collector already set and just contributes to it, so the count is the desired
// state's and the refusal is reported once.
func gateSchemaScope(opts Options, load func(Options) (*goschema.Database, error)) (*goschema.Database, error) {
	if opts.collect != nil {
		return load(opts)
	}
	inner := opts
	inner.collect = &schemaBlockCollector{}
	db, err := load(inner)
	if err != nil {
		return nil, err
	}
	if err := opts.checkSchemaScope(inner.collect.blocks); err != nil {
		return nil, err
	}
	return db, nil
}

// checkSchemaScope refuses a desired state that declares more schemas than the
// run can reach.
//
// The leading sentence is the community binary's, kept so a script matching on
// it keeps working. What follows is Ptah going past it: that binary names
// neither the file nor the blocks, which leaves an operator holding a generated
// document with nothing to grep for.
func (o Options) checkSchemaScope(blocks []atlashcl.SchemaBlock) error {
	if o.SchemaScope == "" || len(blocks) <= 1 {
		return nil
	}
	declared := make([]string, 0, len(blocks))
	for _, block := range blocks {
		declared = append(declared, fmt.Sprintf("%q at %s:%d", block.Name, block.Filename, block.Line))
	}
	return fmt.Errorf(
		"cannot use HCL with more than 1 schema when %s is limited to schema %q: %d top-level schema blocks are declared: %s",
		o.SchemaScopeFlag, o.SchemaScope, len(blocks), strings.Join(declared, ", "),
	)
}

// Load reads one local schema file from either a plain path or file:// URL.
func Load(rawURL string, opts Options) (*goschema.Database, error) {
	path, err := LocalFilePath(rawURL)
	if err != nil {
		return nil, err
	}
	return LoadPath(path, opts)
}

// LoadPath reads one local schema source from a resolved filesystem path: a
// single schema file, or a directory of schema files (see [loadSchemaDir]).
func LoadPath(path string, opts Options) (*goschema.Database, error) {
	return gateSchemaScope(opts, func(opts Options) (*goschema.Database, error) {
		resolved, isDir, err := statSchemaPath(path)
		if err != nil {
			return nil, err
		}
		if isDir {
			return loadSchemaDir(resolved, opts)
		}
		return loadSchemaFile(resolved, opts)
	})
}

// loadSchemaDirEntry reads one entry of a schema directory, together with the
// identities that entry declares under a guard (see [guardedObjects]).
//
// It exists so a directory source cannot recurse: an entry that turns out to be
// a directory refuses here instead of being read as another schema directory.
// That covers the case [os.ReadDir] cannot see, a symlink to a directory, which
// it reports as a symlink rather than as a directory.
func loadSchemaDirEntry(path string, opts Options) (*goschema.Database, map[guardKey]struct{}, error) {
	resolved, isDir, err := statSchemaPath(path)
	if err != nil {
		return nil, nil, err
	}
	if isDir {
		return nil, nil, isDirectoryError(resolved)
	}
	if strings.EqualFold(filepath.Ext(resolved), dirSQLExtension) {
		db, statements, err := loadSQLFileWithStatements(resolved, opts)
		if err != nil {
			return nil, nil, err
		}
		return db, guardedObjects(statements), nil
	}
	db, err := loadSchemaFile(resolved, opts)
	if err != nil {
		return nil, nil, err
	}
	return db, nil, nil
}

// statSchemaPath resolves a caller-supplied path through the CLI path guard and
// reports whether it names a directory.
func statSchemaPath(path string) (resolved string, isDir bool, err error) {
	resolved, err = pathguard.ResolveCLIPath(path)
	if err != nil {
		return "", false, fmt.Errorf("resolve schema file path: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, fmt.Errorf("schema file does not exist: %s", resolved)
		}
		return "", false, fmt.Errorf("stat schema file: %w", err)
	}
	return resolved, info.IsDir(), nil
}

// loadSchemaFile reads one schema file, chosen by extension.
func loadSchemaFile(resolved string, opts Options) (*goschema.Database, error) {
	switch strings.ToLower(filepath.Ext(resolved)) {
	case ".hcl":
		return atlashcl.ParseFileWithOptions(resolved, atlashcl.Options{
			IgnoreUnknownNames: opts.IgnoreUnknownHCLNames,
			RecordSchemaBlock:  opts.recordSchemaBlock(),
			Vars:               opts.Vars,
		})
	case ".yaml", ".yml":
		return yamlschema.ParseFile(resolved)
	case ".sql":
		return loadSQLFile(resolved, opts)
	default:
		return nil, fmt.Errorf("unsupported schema file extension %q: only .yaml, .yml, .hcl, and .sql are supported", filepath.Ext(resolved))
	}
}

// dirSQLExtension and dirHCLExtension are the two mutually exclusive formats a
// schema DIRECTORY may hold.
//
// A directory is deliberately narrower than a single file, which also accepts
// .yaml and .yml: the pinned Atlas community binary v1.3.0 reads a directory of
// .sql or of .hcl and reports `contains neither SQL nor HCL files` otherwise,
// and this surface must not accept a directory that binary refuses. A YAML
// schema is still a schema source — one file at a time.
const (
	dirSQLExtension = ".sql"
	dirHCLExtension = ".hcl"
)

// loadSchemaDir reads a directory of schema files as one desired state.
//
// The rules are measured against the pinned Atlas community binary v1.3.0 on
// SQLite, one fixture per rule:
//
//   - a directory of .sql files loads in filename order;
//   - a directory of .hcl files loads the same way;
//   - a directory holding both refuses as ambiguous, naming the first of each;
//   - files with any other extension are ignored, so a README next to the
//     schema costs nothing;
//   - a directory with neither refuses;
//   - a subdirectory refuses — there is no recursion;
//   - a file that declares an object an earlier file already declared refuses,
//     because the files are a script and not a set (see schemadir_order.go).
//
// The subdirectory rule is applied to BOTH formats, which is one deliberate
// divergence: that binary refuses a subdirectory inside a SQL directory and
// silently ignores one inside an HCL directory. Silently ignoring a directory
// that may hold schema files is the behavior worth not copying, and refusing
// can never accept a source that binary rejects.
//
// atlas.sum still wins: a directory carrying one is a migration directory on
// both surfaces, replayed rather than read, and saying so here keeps the two
// spellings from disagreeing when this loader is reached directly.
func loadSchemaDir(dir string, opts Options) (*goschema.Database, error) {
	if _, err := os.Stat(filepath.Join(dir, atlasSumFileName)); err == nil {
		return nil, fmt.Errorf(
			"%q is a migration directory (it contains %s), not a schema directory",
			filepath.Base(dir), atlasSumFileName,
		)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read schema directory: %w", err)
	}
	// os.ReadDir sorts by filename, which is the order the files are merged in.
	var sqlNames, hclNames, subdirNames []string
	for _, entry := range entries {
		if entry.IsDir() {
			subdirNames = append(subdirNames, entry.Name())
			continue
		}
		switch strings.ToLower(filepath.Ext(entry.Name())) {
		case dirSQLExtension:
			sqlNames = append(sqlNames, entry.Name())
		case dirHCLExtension:
			hclNames = append(hclNames, entry.Name())
		}
	}
	if len(sqlNames) > 0 && len(hclNames) > 0 {
		return nil, fmt.Errorf("ambiguous schema: both SQL and HCL files found: %q, %q", sqlNames[0], hclNames[0])
	}
	names := sqlNames
	format := schemaDirFormatSQL
	if len(names) == 0 {
		names = hclNames
		format = schemaDirFormatHCL
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%q contains neither SQL nor HCL files", filepath.Base(dir))
	}
	if len(subdirNames) > 0 {
		return nil, isDirectoryError(filepath.Join(dir, subdirNames[0]))
	}

	merged := &goschema.Database{}
	ledger := newDirDeclarations()
	for _, name := range names {
		db, guarded, err := loadSchemaDirEntry(filepath.Join(dir, name), opts)
		if err != nil {
			return nil, err
		}
		if err := ledger.admit(name, declaredObjects(db, format), guarded); err != nil {
			return nil, err
		}
		appendDatabase(merged, db)
	}
	goschema.Finalize(merged)
	return merged, nil
}

// atlasSumFileName is the marker that makes a directory a migration directory
// rather than a schema directory. It is spelled here rather than imported so
// this package keeps no dependency on the migration-sum package.
const atlasSumFileName = "atlas.sum"

// isDirectoryError reports a path that had to be a file, naming it the way the
// pinned community binary does: the parent directory and the entry, not the
// absolute path.
func isDirectoryError(path string) error {
	return fmt.Errorf("read %s: is a directory", filepath.Join(filepath.Base(filepath.Dir(path)), filepath.Base(path)))
}

// LoadAll reads all schema files and merges them into one database IR.
func LoadAll(rawURLs []string, opts Options) (*goschema.Database, error) {
	if len(rawURLs) == 0 {
		return nil, fmt.Errorf("at least one schema file URL is required")
	}

	return gateSchemaScope(opts, func(opts Options) (*goschema.Database, error) {
		merged := &goschema.Database{}
		for _, rawURL := range rawURLs {
			db, err := Load(rawURL, opts)
			if err != nil {
				return nil, err
			}
			appendDatabase(merged, db)
		}
		goschema.Finalize(merged)
		return merged, nil
	})
}

// ToDBSchema converts Ptah's desired-schema IR into the DB schema shape used by
// schema comparison. It is intended for local file-to-file comparisons where no
// live database reader is involved.
//
// dialect names the target the converted schema will be compared under. It only
// decides how the one goschema field that carries two concepts is unpacked:
// goschema.Index.Type is the PostgreSQL access method on a PostgreSQL-family
// target and the ClickHouse data-skipping-index type on ClickHouse, and the DB
// shape keeps those apart in Method and Type. An empty dialect converts as if
// no target were known and leaves Method unset.
func ToDBSchema(db *goschema.Database, dialect string) *dbschematypes.DBSchema {
	if db == nil {
		return &dbschematypes.DBSchema{}
	}

	tableByStruct := make(map[string]goschema.Table, len(db.Tables))
	for _, table := range db.Tables {
		tableByStruct[table.StructName] = table
	}

	out := &dbschematypes.DBSchema{
		Schemas:     toDBSchemas(db.Schemas),
		Tables:      toDBTables(db.Tables, db.Fields, db.RLSEnabledTables),
		Enums:       toDBEnums(db.Enums),
		Indexes:     toDBIndexes(db.Indexes, tableByStruct, dialect),
		Constraints: toDBConstraints(db.Tables, db.Fields, db.Constraints, tableByStruct),
		Extensions:  toDBExtensions(db.Extensions),
		Functions:   toDBFunctions(db.Functions),
		Sequences:   toDBSequences(db.Sequences),
		Domains:     toDBDomains(db.Domains),
		Composites:  toDBCompositeTypes(db.CompositeTypes),
		Ranges:      toDBRanges(db.Ranges),
		Views:       toDBViews(db.Views),
		MatViews:    toDBMaterializedViews(db.MaterializedViews),
		Triggers:    toDBTriggers(db.Triggers, tableByStruct),
		RLSPolicies: toDBRLSPolicies(db.RLSPolicies),
		Roles:       toDBRoles(db.Roles),
		Grants:      toDBGrants(db.Grants),
		// A file-to-file comparison uses this side as the current state, and a
		// document that declared its own limits declares them here too
		// (stokaro/ptah#1276).
		NotDescribed: db.NotDescribed,
	}
	applyTablePrimaryKeys(out, db.Tables)
	return out
}

// LocalFilePath converts a local schema source URL into a filesystem path.
func LocalFilePath(rawURL string) (string, error) {
	base, rawQuery, _ := strings.Cut(strings.TrimSpace(rawURL), "?")
	if base == "" {
		return "", fmt.Errorf("schema file URL is required")
	}
	if rawQuery != "" {
		if _, err := url.ParseQuery(rawQuery); err != nil {
			return "", fmt.Errorf("parse schema file URL query: %w", err)
		}
		return "", fmt.Errorf("schema file URL query parameters are not supported yet")
	}
	if strings.Contains(base, "://") && !strings.HasPrefix(base, "file://") {
		return "", fmt.Errorf("only local file:// schema files are supported")
	}

	path := strings.TrimPrefix(base, "file://")
	if path == "" {
		path = "."
	}
	path, err := url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("decode schema file URL path: %w", err)
	}
	return filepath.Clean(path), nil
}

func loadSQLFile(path string, opts Options) (*goschema.Database, error) {
	db, _, err := loadSQLFileWithStatements(path, opts)
	return db, err
}

// loadSQLFileWithStatements is [loadSQLFile] with the parsed statements kept.
//
// A schema DIRECTORY needs them: the goschema IR records no IF NOT EXISTS for a
// table, so only the statement can say whether a redeclaration is guarded, and
// that is the difference between an exit 1 the pinned binary also gives and a
// refusal it does not (see schemadir_order.go).
func loadSQLFileWithStatements(path string, opts Options) (*goschema.Database, *ast.StatementList, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read SQL schema file: %w", err)
	}

	statements, err := parser.NewParser(string(data), parser.WithDialect(opts.Dialect)).Parse()
	if err != nil {
		return nil, nil, fmt.Errorf("parse SQL schema file: %w", err)
	}
	db := toschema.ToDatabase(statements)
	goschema.Finalize(&db)
	// The same directive grammar the HCL loader reads, spelled with SQL's
	// comment marker. No Ptah surface writes one into SQL today -- only the HCL
	// rendering omits blocks -- but the contract is the document's, not one
	// format's, and a hand-written SQL desired state must be able to say the
	// same thing (stokaro/ptah#1276).
	notDescribed, err := coverage.DecodeHeader(string(data))
	if err != nil {
		return nil, nil, fmt.Errorf("parse SQL schema file %s: %w", path, err)
	}
	db.NotDescribed = notDescribed
	return &db, statements, nil
}

func appendDatabase(dst, src *goschema.Database) {
	dst.Schemas = append(dst.Schemas, src.Schemas...)
	dst.Tables = append(dst.Tables, src.Tables...)
	dst.Fields = append(dst.Fields, src.Fields...)
	dst.Indexes = append(dst.Indexes, src.Indexes...)
	dst.Constraints = append(dst.Constraints, src.Constraints...)
	dst.Enums = append(dst.Enums, src.Enums...)
	dst.EmbeddedFields = append(dst.EmbeddedFields, src.EmbeddedFields...)
	dst.Extensions = append(dst.Extensions, src.Extensions...)
	dst.Sequences = append(dst.Sequences, src.Sequences...)
	dst.Domains = append(dst.Domains, src.Domains...)
	dst.CompositeTypes = append(dst.CompositeTypes, src.CompositeTypes...)
	dst.Ranges = append(dst.Ranges, src.Ranges...)
	dst.Functions = append(dst.Functions, src.Functions...)
	dst.Views = append(dst.Views, src.Views...)
	dst.MaterializedViews = append(dst.MaterializedViews, src.MaterializedViews...)
	dst.Triggers = append(dst.Triggers, src.Triggers...)
	dst.RLSPolicies = append(dst.RLSPolicies, src.RLSPolicies...)
	dst.RLSEnabledTables = append(dst.RLSEnabledTables, src.RLSEnabledTables...)
	dst.Roles = append(dst.Roles, src.Roles...)
	dst.Grants = append(dst.Grants, src.Grants...)
	dst.ManagedData = append(dst.ManagedData, src.ManagedData...)
	// Several files loaded together are one description, and it describes only
	// what all of them together describe. Union, never intersection: a limit
	// one file declares is a limit of the whole (stokaro/ptah#1276).
	dst.NotDescribed = dst.NotDescribed.Merge(src.NotDescribed)
}

func toDBSchemas(schemas []goschema.Schema) []dbschematypes.DBSchemaInfo {
	out := make([]dbschematypes.DBSchemaInfo, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, dbschematypes.DBSchemaInfo{
			Name:    schema.Name,
			Comment: schema.Comment,
			Charset: schema.Charset,
			Collate: schema.Collate,
		})
	}
	return out
}

func toDBTables(
	tables []goschema.Table,
	fields []goschema.Field,
	rlsEnabledTables []goschema.RLSEnabledTable,
) []dbschematypes.DBTable {
	fieldsByStruct := make(map[string][]goschema.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]dbschematypes.DBTable, 0, len(tables))
	for _, table := range tables {
		out = append(out, dbschematypes.DBTable{
			Name:         table.Name,
			Schema:       table.Schema,
			Type:         "TABLE",
			Comment:      table.Comment,
			Columns:      toDBColumns(fieldsByStruct[table.StructName]),
			RLSEnabled:   tableRLSEnabled(table, rlsEnabledTables),
			Strict:       table.Strict,
			WithoutRowID: table.WithoutRowID,
		})
	}
	return out
}

func tableRLSEnabled(table goschema.Table, enabledTables []goschema.RLSEnabledTable) bool {
	return slices.ContainsFunc(enabledTables, func(enabled goschema.RLSEnabledTable) bool {
		return enabled.StructName != "" && enabled.StructName == table.StructName ||
			enabled.Table == table.QualifiedName() ||
			table.Schema == "" && enabled.Table == table.Name
	})
}

func toDBColumns(fields []goschema.Field) []dbschematypes.DBColumn {
	out := make([]dbschematypes.DBColumn, 0, len(fields))
	for i, field := range fields {
		out = append(out, toDBColumn(field, i+1))
	}
	return out
}

func toDBColumn(field goschema.Field, ordinal int) dbschematypes.DBColumn {
	nullable := "NO"
	if field.Nullable {
		nullable = "YES"
	}
	column := dbschematypes.DBColumn{
		Name:            field.Name,
		DataType:        field.Type,
		ColumnType:      field.Type,
		IsNullable:      nullable,
		OrdinalPosition: ordinal,
		IsAutoIncrement: field.AutoInc || field.IdentityGeneration != "",
		IsPrimaryKey:    field.Primary,
		IsUnique:        field.Unique,
		Charset:         field.Charset,
		Collate:         field.Collate,
		GeneratedKind:   field.GeneratedKind,
	}
	if field.DefaultSet {
		column.ColumnDefault = new(field.Default)
	} else if field.DefaultExpr != "" {
		column.ColumnDefault = new(field.DefaultExpr)
	}
	if field.GeneratedExpression != "" {
		column.GeneratedExpression = new(field.GeneratedExpression)
	}
	return column
}

func applyTablePrimaryKeys(schema *dbschematypes.DBSchema, tables []goschema.Table) {
	primaryByTable := make(map[string]map[string]struct{})
	for _, table := range tables {
		if len(table.PrimaryKey) == 0 {
			continue
		}
		columns := make(map[string]struct{}, len(table.PrimaryKey))
		for _, column := range table.PrimaryKey {
			columns[column] = struct{}{}
		}
		primaryByTable[table.QualifiedName()] = columns
	}
	for tableIdx, table := range schema.Tables {
		columns := primaryByTable[table.QualifiedName()]
		if len(columns) == 0 {
			continue
		}
		for columnIdx, column := range table.Columns {
			if _, ok := columns[column.Name]; ok {
				schema.Tables[tableIdx].Columns[columnIdx].IsPrimaryKey = true
			}
		}
	}
}

func toDBEnums(enums []goschema.Enum) []dbschematypes.DBEnum {
	out := make([]dbschematypes.DBEnum, 0, len(enums))
	for _, enum := range enums {
		out = append(out, dbschematypes.DBEnum{Name: enum.Name, Values: append([]string(nil), enum.Values...)})
	}
	return out
}

// toDBIndexes converts desired-state indexes into the DB shape.
//
// Everything the comparator reads has to survive this hop. Before issue #1272
// the access method, the structured key parts and the INCLUDE payload were
// dropped here, which was invisible only because the PostgreSQL comparator
// ignored all three; once it started reading them, a `schema diff` whose
// --from side is a local file would have reported a rebuild for every index
// carrying any of them against the database it was inspected from.
func toDBIndexes(
	indexes []goschema.Index,
	tables map[string]goschema.Table,
	dialect string,
) []dbschematypes.DBIndex {
	out := make([]dbschematypes.DBIndex, 0, len(indexes))
	for _, index := range indexes {
		tableName, schema := indexTable(index.StructName, index.TableName, tables)
		out = append(out, dbschematypes.DBIndex{
			Name:           index.Name,
			TableName:      tableName,
			Schema:         schema,
			Columns:        append([]string(nil), index.Fields...),
			Parts:          toDBIndexParts(index.Parts, index.Operator),
			IsUnique:       index.Unique,
			Condition:      index.Condition,
			NullsDistinct:  index.NullsDistinct,
			Method:         indexAccessMethod(index.Type, dialect),
			IncludeColumns: append([]string(nil), index.IncludeColumns...),
			StorageParams:  maps.Clone(index.StorageParams),
			Type:           index.Type,
			Granularity:    index.Granularity,
		})
	}
	return out
}

// indexAccessMethod reports goschema.Index.Type as an access method only where
// that is what it means. On ClickHouse the same field carries the
// data-skipping-index type, which is a different concept the DB shape keeps in
// DBIndex.Type.
func indexAccessMethod(indexType, dialect string) string {
	if !platform.IsPostgresFamily(dialect) {
		return ""
	}
	return indexType
}

// toDBIndexParts converts structured key parts, resolving the index-level
// operator class the way the renderer applies it: a part without its own class
// inherits the index's, so the DB shape -- which has no index-level slot --
// records the resolved value per part.
func toDBIndexParts(parts []goschema.IndexPart, indexOperator string) []dbschematypes.DBIndexPart {
	if len(parts) == 0 {
		return nil
	}
	converted := make([]dbschematypes.DBIndexPart, len(parts))
	for position, part := range parts {
		operator := part.Operator
		if operator == "" {
			operator = indexOperator
		}
		converted[position] = dbschematypes.DBIndexPart{
			Name:       part.Name,
			Expr:       part.Expr,
			Operator:   operator,
			Desc:       part.Desc,
			NullsOrder: part.NullsOrder,
		}
	}
	return converted
}

func toDBConstraints(
	tablesList []goschema.Table,
	fields []goschema.Field,
	constraints []goschema.Constraint,
	tables map[string]goschema.Table,
) []dbschematypes.DBConstraint {
	fieldsByStruct := make(map[string][]goschema.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]dbschematypes.DBConstraint, 0, len(constraints)+len(tablesList)+len(fields))
	type constraintIdentity struct {
		schema string
		table  string
		name   string
	}
	seen := make(map[constraintIdentity]struct{})
	appendConstraint := func(constraint dbschematypes.DBConstraint) {
		key := constraintIdentity{
			schema: constraint.Schema,
			table:  constraint.TableName,
			name:   constraint.Name,
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, constraint)
	}

	for _, table := range tablesList {
		if len(table.PrimaryKey) == 0 {
			continue
		}
		appendConstraint(dbschematypes.DBConstraint{
			Name:        tablePrimaryKeyName(table),
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "PRIMARY KEY",
			ColumnNames: append([]string(nil), table.PrimaryKey...),
			ColumnName:  first(table.PrimaryKey),
		})
	}
	for _, constraint := range constraints {
		tableName, schema := indexTable(constraint.StructName, constraint.Table, tables)
		dbConstraint := dbschematypes.DBConstraint{
			Name:           constraint.Name,
			TableName:      tableName,
			Schema:         schema,
			Type:           constraint.Type,
			ColumnNames:    append([]string(nil), constraint.Columns...),
			ColumnName:     first(constraint.Columns),
			CheckClause:    optionalStringPtr(constraint.CheckExpression),
			NullsDistinct:  constraint.NullsDistinct,
			IncludeColumns: append([]string(nil), constraint.IncludeColumns...),
			UsingMethod:    optionalStringPtr(constraint.UsingMethod),
			ExcludeElements: optionalStringPtr(
				constraint.ExcludeElements,
			),
			WhereCondition: optionalStringPtr(constraint.WhereCondition),
		}
		if constraint.ForeignTable != "" {
			foreignTable, foreignSchema := splitTableIdentity(constraint.ForeignTable)
			dbConstraint.ForeignTable = new(foreignTable)
			dbConstraint.ForeignSchema = foreignSchema
			dbConstraint.ForeignColumn = optionalStringPtr(constraint.ForeignColumn)
			dbConstraint.ForeignColumns = append([]string(nil), constraint.ForeignColumnsOrDefault()...)
			dbConstraint.DeleteRule = optionalStringPtr(constraint.OnDelete)
			dbConstraint.UpdateRule = optionalStringPtr(constraint.OnUpdate)
		}
		appendConstraint(dbConstraint)
	}
	for _, table := range tablesList {
		for _, field := range fieldsByStruct[table.StructName] {
			for _, constraint := range toDBFieldConstraints(table, field) {
				appendConstraint(constraint)
			}
		}
	}
	return out
}

func toDBFieldConstraints(table goschema.Table, field goschema.Field) []dbschematypes.DBConstraint {
	var out []dbschematypes.DBConstraint
	if field.Check != "" {
		name := field.CheckName
		if name == "" {
			name = table.Name + "_" + field.Name + "_check"
		}
		out = append(out, dbschematypes.DBConstraint{
			Name:        name,
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "CHECK",
			ColumnName:  field.Name,
			ColumnNames: []string{field.Name},
			CheckClause: new(field.Check),
		})
	}
	if field.Foreign != "" {
		fkRef := fromschema.ParseForeignKeyReference(field.Foreign)
		if fkRef == nil {
			return out
		}
		name := field.ForeignKeyName
		if name == "" {
			name = fromschema.GenerateForeignKeyName(table.Name, field.Name)
		}
		foreignTable, foreignSchema := splitTableIdentity(fkRef.Table)
		out = append(out, dbschematypes.DBConstraint{
			Name:           name,
			TableName:      table.Name,
			Schema:         table.Schema,
			Type:           "FOREIGN KEY",
			ColumnName:     field.Name,
			ColumnNames:    []string{field.Name},
			ForeignTable:   new(foreignTable),
			ForeignSchema:  foreignSchema,
			ForeignColumn:  optionalStringPtr(fkRef.Column),
			ForeignColumns: append([]string(nil), fkRef.ReferencedColumns()...),
			DeleteRule:     optionalStringPtr(field.OnDelete),
			UpdateRule:     optionalStringPtr(field.OnUpdate),
		})
	}
	return out
}

func tablePrimaryKeyName(table goschema.Table) string {
	if table.Name == "" {
		return "primary"
	}
	return table.Name + "_pkey"
}

func indexTable(structName, explicitTable string, tables map[string]goschema.Table) (tableName string, schema string) {
	if explicitTable != "" {
		return splitTableIdentity(explicitTable)
	}
	table, ok := tables[structName]
	if !ok {
		return structName, ""
	}
	return table.Name, table.Schema
}

func splitTableIdentity(value string) (name, schema string) {
	ref, ok := tableref.Parse(value)
	if !ok {
		return value, ""
	}
	return ref.Name, ref.Schema
}

func toDBExtensions(extensions []goschema.Extension) []dbschematypes.DBExtension {
	out := make([]dbschematypes.DBExtension, 0, len(extensions))
	for _, extension := range extensions {
		out = append(out, dbschematypes.DBExtension{
			Name:    extension.Name,
			Version: extension.Version,
			Comment: optionalStringPtr(
				extension.Comment,
			),
		})
	}
	return out
}

func toDBSequences(sequences []goschema.Sequence) []dbschematypes.DBSequence {
	out := make([]dbschematypes.DBSequence, 0, len(sequences))
	for _, sequence := range sequences {
		out = append(out, dbschematypes.DBSequence{
			Name:      sequence.Name,
			Schema:    sequence.Schema,
			DataType:  sequence.AsType,
			Start:     clonePtr(sequence.Start),
			Increment: clonePtr(sequence.Increment),
			MinValue:  clonePtr(sequence.MinValue),
			MaxValue:  clonePtr(sequence.MaxValue),
			Cache:     clonePtr(sequence.Cache),
			Cycle:     sequence.Cycle,
			OwnedBy:   sequence.OwnedBy,
			Comment:   sequence.Comment,
		})
	}
	return out
}

func toDBDomains(domains []goschema.Domain) []dbschematypes.DBDomain {
	out := make([]dbschematypes.DBDomain, 0, len(domains))
	for _, domain := range domains {
		defaultValue := domain.Default
		if domain.DefaultExpr != "" {
			defaultValue = domain.DefaultExpr
		}
		out = append(out, dbschematypes.DBDomain{
			Name:     domain.Name,
			Schema:   domain.Schema,
			BaseType: domain.BaseType,
			NotNull:  domain.NotNull,
			Default:  defaultValue,
			Check:    domain.Check,
		})
	}
	return out
}

func toDBCompositeTypes(composites []goschema.CompositeType) []dbschematypes.DBComposite {
	out := make([]dbschematypes.DBComposite, 0, len(composites))
	for _, composite := range composites {
		fields := make([]dbschematypes.DBCompositeField, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			fields = append(fields, dbschematypes.DBCompositeField{
				Name: field.Name,
				Type: field.Type,
			})
		}
		out = append(out, dbschematypes.DBComposite{
			Name:   composite.Name,
			Schema: composite.Schema,
			Fields: fields,
		})
	}
	return out
}

func toDBRanges(ranges []goschema.Range) []dbschematypes.DBRange {
	out := make([]dbschematypes.DBRange, 0, len(ranges))
	for _, rangeType := range ranges {
		out = append(out, dbschematypes.DBRange{
			Name:    rangeType.Name,
			Schema:  rangeType.Schema,
			Subtype: rangeType.Subtype,
		})
	}
	return out
}

func toDBFunctions(functions []goschema.Function) []dbschematypes.DBFunction {
	out := make([]dbschematypes.DBFunction, 0, len(functions))
	for _, function := range functions {
		function.Canonicalize()
		name, schema := splitTableIdentity(function.Name)
		out = append(out, dbschematypes.DBFunction{
			Name:       name,
			Schema:     schema,
			Parameters: function.Parameters,
			Returns:    function.Returns,
			Language:   function.Language,
			Security:   function.Security,
			Volatility: function.Volatility,
			Body:       function.Body,
			Comment:    function.Comment,
		})
	}
	return out
}

func toDBViews(views []goschema.View) []dbschematypes.DBView {
	out := make([]dbschematypes.DBView, 0, len(views))
	for _, view := range views {
		name, schema := splitTableIdentity(view.Name)
		checkOption := "NONE"
		if view.WithCheck {
			checkOption = "LOCAL"
		}
		out = append(out, dbschematypes.DBView{
			Name:        name,
			Schema:      schema,
			Body:        view.Body,
			CheckOption: checkOption,
			Comment:     view.Comment,
		})
	}
	return out
}

func toDBMaterializedViews(views []goschema.MaterializedView) []dbschematypes.DBMatView {
	out := make([]dbschematypes.DBMatView, 0, len(views))
	for _, view := range views {
		view.Canonicalize()
		name, schema := splitTableIdentity(view.Name)
		out = append(out, dbschematypes.DBMatView{
			Name:            name,
			Schema:          schema,
			Body:            view.Body,
			RefreshStrategy: view.RefreshStrategy,
			Comment:         view.Comment,
		})
	}
	return out
}

func toDBTriggers(triggers []goschema.Trigger, tables map[string]goschema.Table) []dbschematypes.DBTrigger {
	out := make([]dbschematypes.DBTrigger, 0, len(triggers))
	for _, trigger := range triggers {
		trigger.Canonicalize()
		tableName, schema := indexTable(trigger.StructName, trigger.Table, tables)
		out = append(out, dbschematypes.DBTrigger{
			Name:    trigger.Name,
			Schema:  schema,
			Table:   tableName,
			Timing:  trigger.Timing,
			Event:   trigger.Event,
			ForEach: trigger.ForEach,
			Body:    trigger.Body,
			Comment: trigger.Comment,
		})
	}
	return out
}

func toDBRLSPolicies(policies []goschema.RLSPolicy) []dbschematypes.DBRLSPolicy {
	out := make([]dbschematypes.DBRLSPolicy, 0, len(policies))
	for _, policy := range policies {
		out = append(out, dbschematypes.DBRLSPolicy{
			Name:                policy.Name,
			Table:               policy.Table,
			PolicyFor:           policy.PolicyFor,
			ToRoles:             policy.ToRoles,
			UsingExpression:     policy.UsingExpression,
			WithCheckExpression: policy.WithCheckExpression,
			Comment:             policy.Comment,
		})
	}
	return out
}

func toDBRoles(roles []goschema.Role) []dbschematypes.DBRole {
	out := make([]dbschematypes.DBRole, 0, len(roles))
	for _, role := range roles {
		out = append(out, dbschematypes.DBRole{
			Name:        role.Name,
			Login:       role.Login,
			Superuser:   role.Superuser,
			CreateDB:    role.CreateDB,
			CreateRole:  role.CreateRole,
			Inherit:     role.Inherit,
			Replication: role.Replication,
			HasPassword: role.Password != "",
			Comment:     role.Comment,
		})
	}
	return out
}

func toDBGrants(grants []goschema.Grant) []dbschematypes.DBGrant {
	var out []dbschematypes.DBGrant
	for _, grant := range grants {
		grant.Canonicalize()
		for _, privilege := range grant.Privileges {
			objectType := "TABLE"
			objectName := grant.OnTable
			objectSchema := ""
			switch {
			case grant.OnSchema != "":
				objectType = "SCHEMA"
				objectName = grant.OnSchema
			case grant.OnSequence != "":
				objectType = "SEQUENCE"
				objectName, objectSchema = splitTableIdentity(grant.OnSequence)
			default:
				objectName, objectSchema = splitTableIdentity(objectName)
			}
			out = append(out, dbschematypes.DBGrant{
				Role:       grant.Role,
				Privilege:  privilege,
				ObjectType: objectType,
				Schema:     objectSchema,
				ObjectName: objectName,
				WithOption: grant.WithOption,
				GrantedBy:  grant.GrantedBy,
			})
		}
	}
	return out
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	return new(*value)
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func optionalStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return new(value)
}
