package atlasreport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/internal/pathguard"
)

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

func atlasSchemaInspectSplit(args ...any) (schemaInspectArchive, error) {
	opts, input, err := atlasSchemaInspectSplitArgs(args...)
	if err != nil {
		return schemaInspectArchive{}, err
	}

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

func atlasSchemaInspectWrite(args ...any) (string, error) {
	root, archive, err := atlasSchemaInspectWriteArgs(args...)
	if err != nil {
		return "", err
	}
	resolvedRoot, err := pathguard.ResolveCLIPath(root)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	if err := os.MkdirAll(resolvedRoot, 0755); err != nil {
		return "", fmt.Errorf("create output directory %s: %w", root, err)
	}
	if err := validateUniqueSchemaInspectArchivePaths(archive); err != nil {
		return "", err
	}
	for _, file := range archive.Files {
		path, err := atlasSchemaInspectWritePath(resolvedRoot, file.Path)
		if err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return "", fmt.Errorf("create output directory for %s: %w", file.Path, err)
		}
		if err := os.WriteFile(path, []byte(file.Data), 0600); err != nil {
			return "", fmt.Errorf("write %s: %w", file.Path, err)
		}
	}
	return "", nil
}

type schemaInspectSplitOptions struct {
	Mode      string
	Extension string
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
	opts := schemaInspectSplitOptions{Mode: "type"}
	if len(args) > 3 {
		return schemaInspectSplitOptions{}, "", fmt.Errorf("split accepts at most mode and extension arguments")
	}
	if len(args) >= 2 {
		opts.Mode = atlasTemplateString(args[0])
	}
	if len(args) == 3 {
		opts.Extension = atlasTemplateString(args[1])
	}
	if opts.Mode != "type" {
		return schemaInspectSplitOptions{}, "", fmt.Errorf("unsupported split mode %q: only type is supported", opts.Mode)
	}
	if opts.Extension != "" && strings.TrimSpace(opts.Extension) == "" {
		return schemaInspectSplitOptions{}, "", fmt.Errorf("split extension must not be empty")
	}
	return opts, rendered, nil
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
	if len(statements) == 0 {
		return schemaInspectArchive{}, fmt.Errorf("split requires hcl or sql schema output")
	}
	files := make([]schemaInspectArchiveFile, 0, len(statements)+1)
	imports := make([]string, 0, len(statements))
	for index, statement := range statements {
		file := schemaInspectArchiveFile{
			Path: sqlStatementPath(statement, index+1, opts.Extension),
			Data: ensureTrailingSemicolon(statement),
		}
		files = append(files, file)
		imports = append(imports, "./"+file.Path)
	}
	slices.Sort(imports)
	return schemaInspectArchive{Files: append([]schemaInspectArchiveFile{sqlMainFile(imports)}, files...)}, nil
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
	files := make([]schemaInspectArchiveFile, 0, len(blocks))
	for index, block := range blocks {
		kind, name := hclBlockKindAndName(block)
		path := fmt.Sprintf("objects/%04d%s", index+1, opts.Extension)
		if kind != "" {
			path = kind + "/" + sanitizeSchemaInspectFileName(name) + opts.Extension
		}
		files = append(files, schemaInspectArchiveFile{
			Path: path,
			Data: strings.TrimSpace(block.Text) + "\n",
		})
	}
	return schemaInspectArchive{Files: files}, nil
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

func atlasSchemaInspectWritePath(root, relative string) (string, error) {
	clean := filepath.Clean(relative)
	if filepath.IsAbs(clean) || clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe split output path %q", relative)
	}
	resolved, err := pathguard.ResolveWithinRoot(filepath.Join(root, clean), root)
	if err != nil {
		return "", fmt.Errorf("unsafe split output path %q: %w", relative, err)
	}
	return resolved, nil
}

func validateUniqueSchemaInspectArchivePaths(archive schemaInspectArchive) error {
	seen := make(map[string]struct{}, len(archive.Files))
	for _, file := range archive.Files {
		clean := filepath.Clean(file.Path)
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
