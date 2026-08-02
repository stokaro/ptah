package atlasreport

import (
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"

	"go.5x5.cz/ptah/core/sqlutil"
)

// Split modes mirror the documented Atlas schema inspect split strategies:
// one file per database object grouped by schema and object type (the
// default), one file per schema, or one file per object type.
const (
	splitModeObject = "object"
	splitModeSchema = "schema"
	splitModeType   = "type"
)

// splitFallbackSchema owns unqualified objects when the report cannot name a
// default schema.
const splitFallbackSchema = "main"

type schemaInspectArchive struct {
	Files []schemaInspectArchiveFile
}

func (a schemaInspectArchive) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

type schemaInspectArchiveFile struct {
	Path string
	Data string
}

func (a schemaInspectArchive) String() string {
	var out strings.Builder
	for _, file := range a.Files {
		fmt.Fprintf(&out, "-- %s --\n", file.Path)
		out.WriteString(file.Data)
		if !strings.HasSuffix(file.Data, "\n") {
			out.WriteString("\n")
		}
	}
	return out.String()
}

func atlasSchemaInspectSplit(defaultSchema string, args ...any) (schemaInspectArchive, error) {
	opts, input, err := atlasSchemaInspectSplitArgs(args...)
	if err != nil {
		return schemaInspectArchive{}, err
	}
	if defaultSchema == "" {
		defaultSchema = splitFallbackSchema
	}
	opts.DefaultSchema = defaultSchema

	hclArchive, hclErr := splitSchemaInspectHCL(input, opts.withDefaultExtension(".hcl"))
	if hclErr == nil && len(hclArchive.Files) > 0 {
		if err := validateUniqueSchemaInspectArchivePaths(hclArchive); err != nil {
			return schemaInspectArchive{}, err
		}
		return hclArchive, nil
	}

	sqlArchive, err := splitSchemaInspectSQL(input, opts.withDefaultExtension(".sql"))
	if err != nil {
		return schemaInspectArchive{}, err
	}
	if err := validateUniqueSchemaInspectArchivePaths(sqlArchive); err != nil {
		return schemaInspectArchive{}, err
	}
	return sqlArchive, nil
}

// atlasSchemaInspectWrite plans the archive's files under the write call's
// output directory. It only records the plan; the caller applies it after
// rendering, so template execution stays free of filesystem side effects.
func atlasSchemaInspectWrite(files *[]SchemaInspectFile, args ...any) (string, error) {
	root, archive, err := atlasSchemaInspectWriteArgs(args...)
	if err != nil {
		return "", err
	}
	if err := validateUniqueSchemaInspectArchivePaths(archive); err != nil {
		return "", err
	}
	for _, file := range archive.Files {
		*files = append(*files, SchemaInspectFile{Dir: root, Path: file.Path, Data: file.Data})
	}
	return "", nil
}

type schemaInspectSplitOptions struct {
	Mode          string
	Extension     string
	DefaultSchema string
}

func atlasSchemaInspectSplitArgs(args ...any) (schemaInspectSplitOptions, string, error) {
	if len(args) == 0 {
		return schemaInspectSplitOptions{}, "", fmt.Errorf("split requires hcl or sql schema output")
	}
	input := args[len(args)-1]
	rendered, ok := input.(string)
	if !ok {
		return schemaInspectSplitOptions{}, "", fmt.Errorf("split requires hcl or sql schema output")
	}
	opts := schemaInspectSplitOptions{Mode: splitModeObject}
	if len(args) > 3 {
		return schemaInspectSplitOptions{}, "", fmt.Errorf("split accepts at most mode and extension arguments")
	}
	if len(args) >= 2 {
		opts.Mode = atlasTemplateString(args[0])
	}
	if len(args) == 3 {
		opts.Extension = atlasTemplateString(args[1])
	}
	switch opts.Mode {
	case splitModeObject, splitModeSchema, splitModeType:
	default:
		return schemaInspectSplitOptions{}, "", fmt.Errorf(
			"unsupported split mode %q: supported modes are object, schema, and type", opts.Mode)
	}
	if err := validateSplitExtension(opts.Extension); err != nil {
		return schemaInspectSplitOptions{}, "", err
	}
	return opts, rendered, nil
}

// validateSplitExtension rejects extensions that could change the output
// layout: an extension is a file-name suffix, never a path.
func validateSplitExtension(extension string) error {
	if extension == "" {
		return nil
	}
	if !strings.HasPrefix(extension, ".") || len(extension) == 1 {
		return fmt.Errorf("split extension %q must start with a dot followed by a file suffix, for example \".pg.hcl\"", extension)
	}
	if strings.ContainsAny(extension, `/\`) || strings.Contains(extension, "..") {
		return fmt.Errorf("split extension %q must not contain path separators or traversal sequences", extension)
	}
	return nil
}

func (o schemaInspectSplitOptions) withDefaultExtension(extension string) schemaInspectSplitOptions {
	if o.Extension == "" {
		o.Extension = extension
	}
	return o
}

func atlasSchemaInspectWriteArgs(args ...any) (string, schemaInspectArchive, error) {
	if len(args) == 0 {
		return "", schemaInspectArchive{}, fmt.Errorf("write requires split schema output")
	}
	root := "."
	input := args[len(args)-1]
	if len(args) == 2 {
		root = atlasTemplateString(args[0])
	}
	if len(args) > 2 {
		return "", schemaInspectArchive{}, fmt.Errorf("write accepts at most an output path and split schema output")
	}
	archive, ok := input.(schemaInspectArchive)
	if !ok {
		return "", schemaInspectArchive{}, fmt.Errorf("write requires split schema output")
	}
	if strings.TrimSpace(root) == "" {
		return "", schemaInspectArchive{}, fmt.Errorf("write output path must not be empty")
	}
	return root, archive, nil
}

func splitSchemaInspectSQL(sqlText string, opts schemaInspectSplitOptions) (schemaInspectArchive, error) {
	statements := sqlutil.SplitSQLStatements(sqlText)
	// Piping non-schema output (for example json or mermaid text) into split
	// must fail explicitly instead of producing a nonsense object bucket: a
	// schema SQL dump always contains at least one recognizable CREATE
	// statement.
	if len(statements) == 0 || !anyRecognizedSQLStatement(statements) {
		return schemaInspectArchive{}, fmt.Errorf("split requires hcl or sql schema output")
	}
	// Schema and type modes produce flat single-level files; only the
	// per-object tree gets the main.sql atlas:import entry point, matching the
	// documented Atlas output layout (and keeping a "main" schema file from
	// colliding with it).
	if opts.Mode == splitModeSchema || opts.Mode == splitModeType {
		return schemaInspectArchive{Files: groupSQLStatements(statements, opts)}, nil
	}
	files := perObjectSQLFiles(statements, opts)
	imports := make([]string, 0, len(files))
	for _, file := range files {
		imports = append(imports, "./"+file.Path)
	}
	slices.Sort(imports)
	return schemaInspectArchive{Files: append([]schemaInspectArchiveFile{sqlMainFile(imports)}, files...)}, nil
}

func anyRecognizedSQLStatement(statements []string) bool {
	for _, statement := range statements {
		if kind, _ := sqlStatementKindAndName(statement); kind != "" {
			return true
		}
	}
	return false
}

func perObjectSQLFiles(statements []string, opts schemaInspectSplitOptions) []schemaInspectArchiveFile {
	files := make([]schemaInspectArchiveFile, 0, len(statements))
	for index, statement := range statements {
		files = append(files, schemaInspectArchiveFile{
			Path: sqlStatementPath(statement, index+1, opts.Extension),
			Data: ensureTrailingSemicolon(statement),
		})
	}
	return files
}

// groupSQLStatements buckets statements into one file per schema or per
// object type, preserving statement order inside every bucket and bucket
// order of first appearance.
func groupSQLStatements(statements []string, opts schemaInspectSplitOptions) []schemaInspectArchiveFile {
	grouped := newArchiveGrouper(opts.Extension)
	for _, statement := range statements {
		kind, name := sqlStatementKindAndName(statement)
		key := sqlGroupKey(opts.Mode, kind, name, opts.DefaultSchema)
		grouped.append(key, ensureTrailingSemicolon(statement))
	}
	return grouped.files()
}

func sqlGroupKey(mode, kind, name, defaultSchema string) string {
	if mode == splitModeType {
		if kind == "" {
			return "objects"
		}
		return kind
	}
	if schema, _, ok := strings.Cut(name, "."); ok {
		return sanitizeSchemaInspectFileName(trimSQLIdentifier(schema))
	}
	return sanitizeSchemaInspectFileName(defaultSchema)
}

func sqlMainFile(imports []string) schemaInspectArchiveFile {
	var data strings.Builder
	for _, importPath := range imports {
		fmt.Fprintf(&data, "-- atlas:import %s\n", importPath)
	}
	return schemaInspectArchiveFile{Path: "main.sql", Data: data.String()}
}

func sqlStatementPath(statement string, index int, extension string) string {
	kind, name := sqlStatementKindAndName(statement)
	if kind == "" {
		return fmt.Sprintf("objects/%04d%s", index, extension)
	}
	return kind + "/" + sanitizeSchemaInspectFileName(name) + extension
}

func sqlStatementKindAndName(statement string) (kind string, name string) {
	fields := strings.Fields(sqlutil.StripComments(statement))
	if len(fields) < 3 || !strings.EqualFold(fields[0], "CREATE") {
		return "", ""
	}
	offset := sqlCreateObjectOffset(fields)
	if offset >= len(fields) {
		return "", ""
	}
	kind = sqlCreateKind(fields[offset])
	if kind == "" {
		return "", ""
	}
	nameIndex := sqlCreateNameIndex(fields, offset)
	if nameIndex >= len(fields) {
		return "", ""
	}
	return kind, trimSQLIdentifier(fields[nameIndex])
}

func sqlCreateObjectOffset(fields []string) int {
	offset := 1
	for offset < len(fields) && isSQLCreateModifier(fields[offset]) {
		offset++
	}
	return offset
}

func isSQLCreateModifier(field string) bool {
	upper := strings.ToUpper(field)
	return upper == "UNIQUE" || upper == "TEMP" || upper == "TEMPORARY" || upper == "OR" || upper == "REPLACE"
}

func sqlCreateKind(field string) string {
	switch strings.ToUpper(field) {
	case "TABLE":
		return "tables"
	case "INDEX":
		return "indexes"
	case "VIEW":
		return "views"
	case "MATERIALIZED":
		return "materialized_views"
	case "FUNCTION", "PROCEDURE":
		return "functions"
	case "TYPE":
		return "types"
	case "EXTENSION":
		return "extensions"
	default:
		return ""
	}
}

func sqlCreateNameIndex(fields []string, offset int) int {
	nameIndex := offset + 1
	if strings.EqualFold(fields[offset], "MATERIALIZED") {
		nameIndex = offset + 2
	}
	for nameIndex < len(fields) && isSQLCreateNameModifier(fields[nameIndex]) {
		nameIndex++
	}
	if nameIndex+2 < len(fields) &&
		strings.EqualFold(fields[nameIndex], "IF") &&
		strings.EqualFold(fields[nameIndex+1], "NOT") &&
		strings.EqualFold(fields[nameIndex+2], "EXISTS") {
		return nameIndex + 3
	}
	return nameIndex
}

func isSQLCreateNameModifier(field string) bool {
	return strings.EqualFold(field, "CONCURRENTLY")
}

func trimSQLIdentifier(value string) string {
	trimmed := strings.Trim(value, "`\"[]")
	trimmed = strings.TrimSuffix(trimmed, "(")
	return strings.Trim(trimmed, "`\"[]")
}

func ensureTrailingSemicolon(statement string) string {
	trimmed := strings.TrimSpace(statement)
	if strings.HasSuffix(trimmed, ";") {
		return trimmed + "\n"
	}
	return trimmed + ";\n"
}

func splitSchemaInspectHCL(hclText string, opts schemaInspectSplitOptions) (schemaInspectArchive, error) {
	blocks, err := splitTopLevelHCLBlocks(hclText)
	if err != nil {
		return schemaInspectArchive{}, err
	}
	if opts.Mode == splitModeSchema || opts.Mode == splitModeType {
		return groupHCLBlocks(blocks, opts), nil
	}
	files := make([]schemaInspectArchiveFile, 0, len(blocks))
	for index, block := range blocks {
		kind, name := hclBlockKindAndName(block)
		filePath := fmt.Sprintf("objects/%04d%s", index+1, opts.Extension)
		if kind != "" {
			filePath = kind + "/" + sanitizeSchemaInspectFileName(name) + opts.Extension
		}
		files = append(files, schemaInspectArchiveFile{
			Path: filePath,
			Data: strings.TrimSpace(block.Text) + "\n",
		})
	}
	return schemaInspectArchive{Files: files}, nil
}

// groupHCLBlocks buckets top-level blocks into one file per schema or per
// object type, preserving block order inside every bucket and bucket order of
// first appearance.
func groupHCLBlocks(blocks []schemaInspectHCLBlock, opts schemaInspectSplitOptions) schemaInspectArchive {
	grouped := newArchiveGrouper(opts.Extension)
	for _, block := range blocks {
		key := hclGroupKey(opts.Mode, block, opts.DefaultSchema)
		grouped.append(key, strings.TrimSpace(block.Text)+"\n")
	}
	return schemaInspectArchive{Files: grouped.files()}
}

func hclGroupKey(mode string, block schemaInspectHCLBlock, defaultSchema string) string {
	if mode == splitModeType {
		if dir := hclBlockDir(block.Type); dir != "" {
			return dir
		}
		return "objects"
	}
	schema := block.Schema
	if block.Type == "schema" && len(block.Labels) > 0 {
		schema = block.Labels[0]
	}
	if schema == "" {
		schema = defaultSchema
	}
	return sanitizeSchemaInspectFileName(schema)
}

// archiveGrouper accumulates grouped file contents deterministically: buckets
// appear in first-appearance order and entries inside a bucket keep input
// order, separated by one blank line.
type archiveGrouper struct {
	extension string
	order     []string
	parts     map[string][]string
}

func newArchiveGrouper(extension string) *archiveGrouper {
	return &archiveGrouper{extension: extension, parts: map[string][]string{}}
}

func (g *archiveGrouper) append(key, data string) {
	if _, ok := g.parts[key]; !ok {
		g.order = append(g.order, key)
	}
	g.parts[key] = append(g.parts[key], data)
}

func (g *archiveGrouper) files() []schemaInspectArchiveFile {
	files := make([]schemaInspectArchiveFile, 0, len(g.order))
	for _, key := range g.order {
		files = append(files, schemaInspectArchiveFile{
			Path: key + g.extension,
			Data: strings.Join(g.parts[key], "\n"),
		})
	}
	return files
}

type schemaInspectHCLBlock struct {
	Type   string
	Labels []string
	Schema string
	Text   string
}

func splitTopLevelHCLBlocks(hclText string) ([]schemaInspectHCLBlock, error) {
	source := []byte(hclText)
	file, diags := hclsyntax.ParseConfig(source, "schema.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("split hcl schema: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("split hcl schema: unsupported body type %T", file.Body)
	}
	blocks := make([]schemaInspectHCLBlock, 0, len(body.Blocks))
	for _, block := range body.Blocks {
		blockRange := block.Range()
		blocks = append(blocks, schemaInspectHCLBlock{
			Type:   block.Type,
			Labels: slices.Clone(block.Labels),
			Schema: hclBlockSchemaName(block, source),
			Text:   string(blockRange.SliceBytes(source)),
		})
	}
	return blocks, nil
}

func hclBlockKindAndName(block schemaInspectHCLBlock) (kind string, name string) {
	if len(block.Labels) == 0 {
		return "", ""
	}
	name = block.Labels[0]
	if block.Schema != "" && hclBlockTypeUsesSchemaInPath(block.Type) {
		name = block.Schema + "_" + name
	}
	return hclBlockDir(block.Type), name
}

func hclBlockSchemaName(block *hclsyntax.Block, source []byte) string {
	attr := block.Body.Attributes["schema"]
	if attr == nil {
		return ""
	}
	raw := string(attr.Expr.Range().SliceBytes(source))
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "schema.")
	raw = strings.Trim(raw, `"`)
	return raw
}

func hclBlockTypeUsesSchemaInPath(kind string) bool {
	switch kind {
	case "table", "view", "materialized", "function", "trigger", "policy", "grant":
		return true
	default:
		return false
	}
}

func hclBlockDir(kind string) string {
	switch kind {
	case "schema":
		return "schemas"
	case "table":
		return "tables"
	case "enum":
		return "enums"
	case "extension":
		return "extensions"
	case "function":
		return "functions"
	case "view":
		return "views"
	case "materialized":
		return "materialized_views"
	case "trigger":
		return "triggers"
	case "policy":
		return "policies"
	case "role":
		return "roles"
	case "grant":
		return "grants"
	default:
		return ""
	}
}

func validateUniqueSchemaInspectArchivePaths(archive schemaInspectArchive) error {
	seen := make(map[string]struct{}, len(archive.Files))
	for _, file := range archive.Files {
		clean := path.Clean(file.Path)
		if _, ok := seen[clean]; ok {
			return fmt.Errorf("split generated duplicate output path %q", clean)
		}
		seen[clean] = struct{}{}
	}
	return nil
}

func sanitizeSchemaInspectFileName(name string) string {
	var out strings.Builder
	for _, r := range name {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '_', r == '-':
			out.WriteRune(r)
		case r == '.':
			out.WriteRune('_')
		default:
			out.WriteRune('_')
		}
	}
	sanitized := strings.Trim(out.String(), "_")
	if sanitized == "" {
		return "unnamed"
	}
	return sanitized
}

func atlasTemplateString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}
