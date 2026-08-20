package projectconfig

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing/fstest"
	"text/template"
	"time"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasregistry"
	"go.5x5.cz/ptah/internal/atlasruntimevar"
	"go.5x5.cz/ptah/internal/cloudtoken"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/processcapture"
	"go.5x5.cz/ptah/internal/secretdisplay"
)

const (
	atlasExternalDataSourceTimeout = 60 * time.Second
	atlasRuntimeVariableTimeout    = 10 * time.Second
)

func (p atlasParser) resolveAtlasDataSource(block *hclsyntax.Block) (cty.Value, error) {
	if err := p.runContext.Err(); err != nil {
		return cty.NilVal, err
	}
	switch block.Labels[0] {
	case "hcl_schema":
		return p.hclSchemaDataSource(block)
	case "external_schema":
		return p.resolveExternalSchemaDataSource(block)
	case "sql":
		return p.sqlDataSource(block)
	case "external":
		return p.externalDataSource(block)
	case "runtimevar":
		return p.runtimeVariableDataSource(block)
	case "template_dir":
		return p.templateDirectoryDataSource(block)
	case "aws_rds_token":
		return p.awsRDSTokenDataSource(block)
	case "gcp_cloudsql_token":
		return p.gcpCloudSQLTokenDataSource(block)
	case "remote_dir":
		return p.remoteDirectoryDataSource(block)
	case "remote_schema":
		return p.remoteSchemaDataSource(block)
	default:
		return cty.NilVal, unsupported("data."+block.Labels[0], block.TypeRange)
	}
}

func (p atlasParser) resolveExternalSchemaDataSource(block *hclsyntax.Block) (cty.Value, error) {
	source, err := p.externalSchemaDataSource(block)
	if err != nil {
		return cty.NilVal, err
	}
	name := block.Labels[1]
	p.externalSchemas[name] = source
	return cty.ObjectVal(map[string]cty.Value{
		"url": cty.StringVal(externalSchemaMarkerScheme + name),
	}), nil
}

func (p atlasParser) sqlDataSource(block *hclsyntax.Block) (_ cty.Value, resultErr error) {
	if err := validateAtlasDataSourceBody(block, "url", "query", "args"); err != nil {
		return cty.NilVal, err
	}
	rawURL, err := p.requiredDataSourceString(block, "url")
	if err != nil {
		return cty.NilVal, err
	}
	query, err := p.requiredDataSourceString(block, "query")
	if err != nil {
		return cty.NilVal, err
	}
	args, err := p.sqlDataSourceArgs(block)
	if err != nil {
		return cty.NilVal, err
	}

	connection, err := dbschema.ConnectToDatabase(p.runContext, rawURL)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "opening database", err, rawURL)
	}
	defer func() {
		resultErr = errors.Join(resultErr, connection.Close())
	}()
	rows, err := connection.QueryContext(p.runContext, query, args...)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "executing query", err, rawURL, query)
	}
	defer func() {
		resultErr = errors.Join(resultErr, rows.Close())
	}()

	values := make([]cty.Value, 0)
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return cty.NilVal, p.dataSourceError(block, "scanning row", err, rawURL, query)
		}
		value, err := atlasSQLValue(raw)
		if err != nil {
			return cty.NilVal, p.dataSourceError(block, "", err, rawURL, query)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return cty.NilVal, p.dataSourceError(block, "reading rows", err, rawURL, query)
	}
	result, err := atlasSQLResult(values)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "", err, rawURL, query)
	}
	return result, nil
}

func (p atlasParser) sqlDataSourceArgs(block *hclsyntax.Block) ([]any, error) {
	attr, ok := block.Body.Attributes["args"]
	if !ok {
		return nil, nil
	}
	value, err := p.decodedAttrValue("args", attr, "a tuple or list")
	if err != nil {
		return nil, err
	}
	if !value.CanIterateElements() || value.Type() == cty.String {
		return nil, p.invalidValue("args", attr, fmt.Errorf("expected a tuple or list"))
	}
	args := make([]any, 0, value.LengthInt())
	iterator := value.ElementIterator()
	for iterator.Next() {
		_, item := iterator.Element()
		arg, err := atlasSQLArgument(item)
		if err != nil {
			return nil, p.invalidValue("args", attr, err)
		}
		args = append(args, arg)
	}
	return args, nil
}

func atlasSQLArgument(value cty.Value) (any, error) {
	if value.IsNull() || !value.IsKnown() {
		return nil, nil
	}
	switch value.Type() {
	case cty.String:
		return value.AsString(), nil
	case cty.Bool:
		return value.True(), nil
	case cty.Number:
		return atlasNumberToGo(value)
	default:
		return nil, fmt.Errorf("unsupported SQL argument type %s", value.Type().FriendlyName())
	}
}

func atlasSQLValue(value any) (cty.Value, error) {
	switch value := value.(type) {
	case string:
		return cty.StringVal(value), nil
	case []byte:
		return cty.StringVal(string(value)), nil
	case bool:
		return cty.BoolVal(value), nil
	case int:
		return cty.NumberIntVal(int64(value)), nil
	case int8:
		return cty.NumberIntVal(int64(value)), nil
	case int16:
		return cty.NumberIntVal(int64(value)), nil
	case int32:
		return cty.NumberIntVal(int64(value)), nil
	case int64:
		return cty.NumberIntVal(value), nil
	case uint:
		return cty.NumberUIntVal(uint64(value)), nil
	case uint8:
		return cty.NumberUIntVal(uint64(value)), nil
	case uint16:
		return cty.NumberUIntVal(uint64(value)), nil
	case uint32:
		return cty.NumberUIntVal(uint64(value)), nil
	case uint64:
		return cty.NumberUIntVal(value), nil
	case float32:
		return cty.NumberFloatVal(float64(value)), nil
	case float64:
		return cty.NumberFloatVal(value), nil
	case time.Time:
		return cty.StringVal(value.Format(time.RFC3339Nano)), nil
	default:
		return cty.NilVal, fmt.Errorf("unsupported row type: %T", value)
	}
}

func atlasSQLResult(values []cty.Value) (cty.Value, error) {
	resultValues := cty.ListValEmpty(cty.NilType)
	first := cty.NullVal(cty.DynamicPseudoType)
	if len(values) > 0 {
		firstType := values[0].Type()
		for _, value := range values[1:] {
			if !value.Type().Equals(firstType) {
				return cty.NilVal, fmt.Errorf(
					"query rows have inconsistent types: %s then %s",
					firstType.FriendlyName(),
					value.Type().FriendlyName(),
				)
			}
		}
		resultValues = cty.ListVal(values)
		first = values[0]
	}
	return cty.ObjectVal(map[string]cty.Value{
		"count":  cty.NumberIntVal(int64(len(values))),
		"value":  first,
		"values": resultValues,
	}), nil
}

func (p atlasParser) externalDataSource(block *hclsyntax.Block) (cty.Value, error) {
	if err := validateAtlasDataSourceBody(block, "program", "working_dir"); err != nil {
		return cty.NilVal, err
	}
	attr, ok := block.Body.Attributes["program"]
	if !ok {
		return cty.NilVal, missingAtlasDataSourceAttr(block, "program")
	}
	program, err := p.stringListAttr("program", attr)
	if err != nil {
		return cty.NilVal, err
	}
	if len(program) == 0 || strings.TrimSpace(program[0]) == "" {
		return cty.NilVal, p.invalidValue("program", attr, fmt.Errorf("expected a non-empty program list"))
	}
	workingDir := ""
	if attr, ok := block.Body.Attributes["working_dir"]; ok {
		workingDir, err = p.stringAttr("working_dir", attr)
		if err != nil {
			return cty.NilVal, err
		}
		workingDir = p.resolveExternalSchemaWorkingDir(workingDir)
	}
	result, err := processcapture.Run(p.runContext, processcapture.Command{
		Args:    program,
		Dir:     workingDir,
		Timeout: atlasExternalDataSourceTimeout,
	})
	if err != nil {
		return cty.NilVal, p.externalDataSourceError(block, program, err)
	}
	return cty.StringVal(string(result.Stdout)), nil
}

func (p atlasParser) externalDataSourceError(
	block *hclsyntax.Block,
	program []string,
	err error,
) error {
	var failure *processcapture.Failure
	if !errors.As(err, &failure) {
		return p.dataSourceError(block, "running program "+program[0], err, program...)
	}
	cause := failure.Err
	stderr := strings.TrimSpace(secretdisplay.Sanitize(failure.Stderr, os.Environ(), program))
	switch failure.Kind {
	case processcapture.FailureStartOrExit:
		if stderr != "" {
			cause = errors.New(stderr)
		}
	case processcapture.FailureCanceled, processcapture.FailureTimedOut:
		if stderr != "" {
			cause = fmt.Errorf("%s: %w", stderr, failure.Err)
		}
	case processcapture.FailureOutputLimit:
		cause = fmt.Errorf("program produced more than %d bytes of output", processcapture.DefaultMaxStdout)
	}
	return p.dataSourceError(block, "running program "+program[0], cause, program...)
}

func (p atlasParser) runtimeVariableDataSource(block *hclsyntax.Block) (_ cty.Value, resultErr error) {
	if err := validateAtlasDataSourceBody(block, "url"); err != nil {
		return cty.NilVal, err
	}
	rawURL, err := p.requiredDataSourceString(block, "url")
	if err != nil {
		return cty.NilVal, err
	}
	openURL, timeout, err := atlasRuntimeVariableURL(rawURL)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "opening variable", err, rawURL)
	}
	runContext, cancel := context.WithTimeout(p.runContext, timeout)
	defer cancel()
	variable, err := atlasruntimevar.Open(runContext, openURL)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "opening variable", err, rawURL)
	}
	defer func() {
		resultErr = errors.Join(resultErr, variable.Close())
	}()
	snapshot, err := variable.Latest(runContext)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "getting latest snapshot", err, rawURL)
	}
	switch value := snapshot.Value.(type) {
	case []byte:
		return cty.StringVal(string(value)), nil
	case string:
		return cty.StringVal(value), nil
	default:
		return cty.NilVal, p.dataSourceError(
			block,
			"getting latest snapshot",
			fmt.Errorf("unsupported runtime variable type %T", value),
			rawURL,
		)
	}
}

func atlasRuntimeVariableURL(rawURL string) (string, time.Duration, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, err
	}
	query := parsed.Query()
	timeout := atlasRuntimeVariableTimeout
	if rawTimeout := query.Get("timeout"); rawTimeout != "" {
		parsedTimeout, err := time.ParseDuration(rawTimeout)
		if err != nil {
			return "", 0, fmt.Errorf("invalid timeout %q: %w", rawTimeout, err)
		}
		if parsedTimeout <= 0 {
			return "", 0, fmt.Errorf("timeout must be positive, got %q", rawTimeout)
		}
		timeout = parsedTimeout
		query.Del("timeout")
		parsed.RawQuery = query.Encode()
	}
	return parsed.String(), timeout, nil
}

func (p atlasParser) templateDirectoryDataSource(block *hclsyntax.Block) (cty.Value, error) {
	if err := validateAtlasDataSourceBody(block, "path", "vars"); err != nil {
		return cty.NilVal, err
	}
	rawPath, err := p.requiredDataSourceString(block, "path")
	if err != nil {
		return cty.NilVal, err
	}
	variables, err := p.templateDirectoryVariables(block)
	if err != nil {
		return cty.NilVal, err
	}
	fsPath, memPath, err := p.templateDirectoryPath(rawPath)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "reading template directory", err, rawPath)
	}
	directory, err := renderAtlasTemplateDirectory(p.fsys, fsPath, variables)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "rendering template directory", err, rawPath)
	}
	memURL := memDirectoryURL(filepath.ToSlash(memPath), block.Labels[1])
	p.migrationDirectories[memURL] = MigrationDirectorySource{
		FileSystem: directory,
		Path:       fsPath,
	}
	return cty.ObjectVal(map[string]cty.Value{
		"url": cty.StringVal(memURL),
	}), nil
}

func (p atlasParser) templateDirectoryVariables(block *hclsyntax.Block) (map[string]any, error) {
	attr, ok := block.Body.Attributes["vars"]
	if !ok {
		return make(map[string]any), nil
	}
	value, err := p.decodedAttrValue("vars", attr, "an object or map")
	if err != nil {
		return nil, err
	}
	if !value.Type().IsObjectType() && !value.Type().IsMapType() {
		return nil, p.invalidValue("vars", attr, fmt.Errorf("expected an object or map"))
	}
	variables := make(map[string]any, value.LengthInt())
	iterator := value.ElementIterator()
	for iterator.Next() {
		key, item := iterator.Element()
		converted, err := atlasTemplateVariableToGo(item)
		if err != nil {
			return nil, p.invalidValue("vars", attr, err)
		}
		variables[key.AsString()] = converted
	}
	return variables, nil
}

// memDirectoryURL builds the in-memory handle a rendered or pulled directory is
// registered under.
//
// The block label is escaped rather than joined as a path segment. Joining
// normalizes, and two distinct labels that normalize to the same segment --
// `a/../b` and `b` -- would produce one key: the second registration would
// replace the first filesystem while both HCL values still carry that key, so
// an env selecting the first block would execute the second block's migrations.
func memDirectoryURL(prefix, name string) string {
	// The prefix keeps the escaping and the slash count url.URL gave it before
	// this helper existed -- data.template_dir's handle is compared against the
	// pinned binary's output, and `mem://templates/x` and `mem:///templates/x`
	// are not the same string to that comparison.
	base := (&url.URL{Scheme: "mem", Path: prefix}).String()
	return strings.TrimSuffix(base, "/") + "/" + url.PathEscape(name)
}

func (p atlasParser) templateDirectoryPath(rawPath string) (fsPath, memPath string, err error) {
	if strings.Contains(rawPath, "://") && !strings.HasPrefix(rawPath, "file://") {
		return "", "", fmt.Errorf("unsupported URL scheme: %s", rawPath)
	}
	pathValue := strings.TrimPrefix(rawPath, "file://")
	absBase, err := filepath.Abs(p.baseDir)
	if err != nil {
		return "", "", err
	}
	fsPath = pathValue
	if filepath.IsAbs(pathValue) {
		fsPath, err = filepath.Rel(absBase, pathValue)
		if err != nil {
			return "", "", err
		}
	}
	fsPath = filepath.ToSlash(filepath.Clean(fsPath))
	if fsPath == ".." || strings.HasPrefix(fsPath, "../") {
		return "", "", fmt.Errorf("path escapes atlas.hcl directory: %s", rawPath)
	}
	if err := atlasCheckSandboxedFSPath(p.fsys, fsPath); err != nil {
		return "", "", err
	}
	return fsPath, filepath.Clean(pathValue), nil
}

func renderAtlasTemplateDirectory(
	fsys fs.FS,
	dir string,
	variables map[string]any,
) (fs.FS, error) {
	paths, names, err := atlasTemplateDirectoryFiles(fsys, dir)
	if err != nil {
		return nil, err
	}
	templates := template.New("template_dir").Option("missingkey=error")
	for _, filePath := range paths {
		relative, err := filepath.Rel(dir, filePath)
		if err != nil {
			return nil, err
		}
		relative = filepath.ToSlash(relative)
		if err := atlasCheckSandboxedFSPath(fsys, filePath); err != nil {
			return nil, fmt.Errorf("read %s: %w", relative, err)
		}
		raw, err := fs.ReadFile(fsys, filePath)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", relative, err)
		}
		_, err = templates.New(filepath.Base(relative)).Parse(string(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", relative, err)
		}
	}
	files := fstest.MapFS{}
	for _, name := range names {
		var rendered bytes.Buffer
		if err := templates.ExecuteTemplate(&rendered, name, variables); err != nil {
			return nil, fmt.Errorf("execute %s: %w", name, err)
		}
		files[name] = &fstest.MapFile{Data: bytes.Clone(rendered.Bytes()), Mode: 0o444}
	}
	sum, err := migratesum.ComputeAtlasFiles(files, names)
	if err != nil {
		return nil, err
	}
	files[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes(), Mode: 0o444}
	return migrationsnapshot.Capture(files)
}

func atlasTemplateDirectoryFiles(fsys fs.FS, dir string) (paths, names []string, err error) {
	err = fs.WalkDir(fsys, dir, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(dir, filePath)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		paths = append(paths, filePath)
		if !strings.Contains(relative, "/") && filepath.Ext(relative) == ".sql" {
			names = append(names, relative)
		}
		return nil
	})
	sort.Strings(paths)
	sort.Strings(names)
	return paths, names, err
}

func atlasTemplateVariableToGo(value cty.Value) (any, error) {
	typeName := value.Type().FriendlyName()
	if value.IsNull() || !value.IsKnown() {
		return nil, atlasTemplateVariableTypeError(typeName)
	}
	switch value.Type() {
	case cty.String:
		return value.AsString(), nil
	case cty.Bool:
		return value.True(), nil
	case cty.Number:
		return atlasNumberToGo(value)
	}
	if !value.Type().IsListType() {
		return nil, atlasTemplateVariableTypeError(typeName)
	}
	switch value.Type().ElementType() {
	case cty.String:
		return atlasTemplateStringList(value), nil
	case cty.Number:
		return atlasTemplateNumberList(value), nil
	case cty.Bool:
		return atlasTemplateBoolList(value), nil
	default:
		return nil, atlasTemplateVariableTypeError(typeName)
	}
}

func atlasNumberToGo(value cty.Value) (any, error) {
	number, _ := value.AsBigFloat().Float64()
	return number, nil
}

func atlasTemplateStringList(value cty.Value) []string {
	values := make([]string, 0, value.LengthInt())
	iterator := value.ElementIterator()
	for iterator.Next() {
		_, item := iterator.Element()
		values = append(values, item.AsString())
	}
	return values
}

func atlasTemplateNumberList(value cty.Value) []float64 {
	values := make([]float64, 0, value.LengthInt())
	iterator := value.ElementIterator()
	for iterator.Next() {
		_, item := iterator.Element()
		number, _ := item.AsBigFloat().Float64()
		values = append(values, number)
	}
	return values
}

func atlasTemplateBoolList(value cty.Value) []bool {
	values := make([]bool, 0, value.LengthInt())
	iterator := value.ElementIterator()
	for iterator.Next() {
		_, item := iterator.Element()
		values = append(values, item.True())
	}
	return values
}

func atlasTemplateVariableTypeError(typeName string) error {
	return fmt.Errorf(
		`attribute "vars" must be a map of strings, numbers or booleans, got: %s`,
		typeName,
	)
}

func validateAtlasDataSourceBody(block *hclsyntax.Block, allowed ...string) error {
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	for name, attr := range block.Body.Attributes {
		if !slices.Contains(allowed, name) {
			return unsupportedAttr(name, attr)
		}
	}
	return nil
}

func validateAtlasDataSourceShape(block *hclsyntax.Block) error {
	typ := block.Labels[0]
	switch typ {
	case "hcl_schema":
		if err := validateAtlasDataSourceBody(block, "path", "paths", "vars"); err != nil {
			return err
		}
		_, hasPath := block.Body.Attributes["path"]
		pathsAttr, hasPaths := block.Body.Attributes["paths"]
		switch {
		case hasPath && hasPaths:
			return unsupportedAttr("paths", pathsAttr)
		case !hasPath && !hasPaths:
			return fmt.Errorf(
				"atlas.hcl data.hcl_schema %q requires path or paths at %s:%d",
				block.Labels[1],
				block.TypeRange.Filename,
				block.TypeRange.Start.Line,
			)
		}
		return nil
	case "external_schema":
		if err := validateAtlasDataSourceBody(block, "program", "format", "working_dir", "env"); err != nil {
			return err
		}
		if _, ok := block.Body.Attributes["program"]; !ok {
			return fmt.Errorf(
				"atlas.hcl data.external_schema %q requires a non-empty program list at %s:%d",
				block.Labels[1],
				block.TypeRange.Filename,
				block.TypeRange.Start.Line,
			)
		}
		return nil
	case "sql":
		return validateRequiredAtlasDataSourceAttrs(block, []string{"url", "query"}, "url", "query", "args")
	case "external":
		return validateRequiredAtlasDataSourceAttrs(block, []string{"program"}, "program", "working_dir")
	case "runtimevar":
		return validateRequiredAtlasDataSourceAttrs(block, []string{"url"}, "url")
	case "template_dir":
		return validateRequiredAtlasDataSourceAttrs(block, []string{"path"}, "path", "vars")
	case "aws_rds_token":
		return validateAWSRDSTokenShape(block)
	case "gcp_cloudsql_token":
		// No body schema, and none required: the pinned community binary
		// v1.3.0 decodes a block carrying an unrecognized attribute and goes
		// straight to the token exchange, where `data "gcp_cloudsql_token"
		// "g" { bogus = "x" }` answers `getting token: oauth2: ...` rather
		// than "Unsupported argument". Refusing the attribute here would
		// refuse a project that binary accepts.
		return nil
	case "remote_dir":
		return validateRequiredAtlasDataSourceAttrs(block, []string{"name"}, "name", "tag", "version")
	case "remote_schema":
		return validateRequiredAtlasDataSourceAttrs(block, []string{"name"}, "name", "tag", "version")
	default:
		return unsupported("data."+typ, block.TypeRange)
	}
}

// validateAtlasDataSourceDeclarationShape validates the syntax Ptah knows
// without executing a data source. Recognized cloud sources stay lazy because
// this compatibility layer does not implement their runtime contracts yet.
func validateAtlasDataSourceDeclarationShape(block *hclsyntax.Block) error {
	// Every recognized source now has a runtime contract, so a declaration is
	// checked the same way whether or not anything references it. `remote_schema`
	// used to stay lazy here because resolving it was refused outright, which
	// meant a misspelled attribute was learned about on the day the block
	// started being used (stokaro/ptah#1210).
	return validateAtlasDataSourceShape(block)
}

func validateRequiredAtlasDataSourceAttrs(
	block *hclsyntax.Block,
	required []string,
	allowed ...string,
) error {
	if err := validateAtlasDataSourceBody(block, allowed...); err != nil {
		return err
	}
	for _, name := range required {
		if _, ok := block.Body.Attributes[name]; !ok {
			return missingAtlasDataSourceAttr(block, name)
		}
	}
	return nil
}

func (p atlasParser) requiredDataSourceString(block *hclsyntax.Block, name string) (string, error) {
	attr, ok := block.Body.Attributes[name]
	if !ok {
		return "", missingAtlasDataSourceAttr(block, name)
	}
	return p.stringAttr(name, attr)
}

func missingAtlasDataSourceAttr(block *hclsyntax.Block, name string) error {
	return fmt.Errorf(
		"atlas.hcl data.%s %q requires %s at %s:%d",
		block.Labels[0],
		block.Labels[1],
		name,
		block.TypeRange.Filename,
		block.TypeRange.Start.Line,
	)
}

func (p atlasParser) dataSourceError(
	block *hclsyntax.Block,
	action string,
	err error,
	secrets ...string,
) error {
	if err == nil {
		return nil
	}
	allSecrets := append(make([]string, 0), secrets...)
	if p.sensitiveValues != nil {
		allSecrets = append(allSecrets, (*p.sensitiveValues)...)
	}
	safe := secretdisplay.SanitizeError(err, os.Environ(), allSecrets)
	prefix := fmt.Sprintf("data.%s.%s", block.Labels[0], block.Labels[1])
	if action == "" {
		return fmt.Errorf("%s: %w", prefix, safe)
	}
	return fmt.Errorf("%s: %s: %w", prefix, action, safe)
}

// awsRDSTokenDataSource mints an RDS IAM authentication token.
//
// The value is a password, so it joins the sensitive set before it can reach a
// diagnostic: a signing or credential failure that echoed the token would put
// a live credential in a log line.
func (p atlasParser) awsRDSTokenDataSource(block *hclsyntax.Block) (cty.Value, error) {
	if err := validateAWSRDSTokenShape(block); err != nil {
		return cty.NilVal, err
	}
	endpoint, err := p.requiredDataSourceString(block, "endpoint")
	if err != nil {
		return cty.NilVal, err
	}
	username, err := p.requiredDataSourceString(block, "username")
	if err != nil {
		return cty.NilVal, err
	}
	region, err := p.optionalAtlasDataSourceString(block, "region")
	if err != nil {
		return cty.NilVal, err
	}
	profile, err := p.optionalAtlasDataSourceString(block, "profile")
	if err != nil {
		return cty.NilVal, err
	}
	token, err := cloudtoken.AWSRDSToken(p.runContext, cloudtoken.AWSRDSOptions{
		Endpoint: endpoint,
		Username: username,
		Region:   region,
		Profile:  profile,
	})
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "minting token", err)
	}
	p.recordSensitiveValue(token)
	return cty.StringVal(token), nil
}

// gcpCloudSQLTokenDataSource mints a Cloud SQL IAM access token.
func (p atlasParser) gcpCloudSQLTokenDataSource(block *hclsyntax.Block) (cty.Value, error) {
	token, err := cloudtoken.GCPCloudSQLToken(p.runContext)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "minting token", err)
	}
	p.recordSensitiveValue(token)
	return cty.StringVal(token), nil
}

// optionalAtlasDataSourceString reads an attribute that may be absent,
// answering the empty string when it is.
func (p atlasParser) optionalAtlasDataSourceString(
	block *hclsyntax.Block,
	name string,
) (string, error) {
	attr, ok := block.Body.Attributes[name]
	if !ok {
		return "", nil
	}
	return p.stringAttr(name, attr)
}

// recordSensitiveValue adds a minted credential to the set every diagnostic is
// sanitized against.
func (p atlasParser) recordSensitiveValue(value string) {
	if p.sensitiveValues == nil || value == "" {
		return
	}
	*p.sensitiveValues = append(*p.sensitiveValues, value)
}

// validateAWSRDSTokenShape is the single statement of the block's schema.
//
// It is one function because the shape is asked for twice -- once when a
// declaration is validated and once when it is resolved -- and two copies of
// an attribute list are two lists that drift.
func validateAWSRDSTokenShape(block *hclsyntax.Block) error {
	return validateRequiredAtlasDataSourceAttrs(
		block,
		[]string{"endpoint", "username"},
		"endpoint", "username", "region", "profile",
	)
}

// remoteDirectoryDataSource fetches a migration directory from the OCI
// namespace an `atlas://` reference resolves against.
//
// The vendor spelling names a repository and a pointer with no registry host in
// it, because it assumes one hosted account. Ptah has none: the reference is
// resolved through [atlasregistry] against a namespace the operator configures,
// and a run with none configured is refused rather than sent anywhere
// (stokaro/ptah#1210).
//
// The fetched directory is registered the way a rendered template directory is,
// so everything downstream reads it through the same in-memory route. That is
// what the consumer accepts: the Atlas-compatible --dir takes a local or an
// in-memory directory, not an oci:// reference, so handing the reference on
// would only move the refusal.
func (p atlasParser) remoteDirectoryDataSource(block *hclsyntax.Block) (cty.Value, error) {
	if err := validateRequiredAtlasDataSourceAttrs(
		block, []string{"name"}, "name", "tag", "version",
	); err != nil {
		return cty.NilVal, err
	}
	reference, err := p.remoteArtifactReference(block)
	if err != nil {
		return cty.NilVal, err
	}
	plainHTTP, err := atlasregistry.PlainHTTP.Resolve()
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "reading the registry transport setting", err)
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "opening the registry client", err, reference.OCI)
	}
	artifact, err := migrationartifact.Pull(p.runContext, client, reference.OCI)
	if err != nil {
		return cty.NilVal, p.dataSourceError(block, "fetching remote directory", err, reference.OCI)
	}
	memURL := memDirectoryURL("/remote_dir", block.Labels[1])
	p.migrationDirectories[memURL] = MigrationDirectorySource{
		FileSystem: artifact.FileSystem,
		// The reference is carried for display, and ReadOnly is what keeps it
		// out of a writer: it is not a local path, and joining it to the
		// project root would create a directory named after it.
		Path:     reference.OCI,
		ReadOnly: true,
	}
	return cty.ObjectVal(map[string]cty.Value{
		"url": cty.StringVal(memURL),
	}), nil
}

// RemoteSchemaMarkerScheme prefixes the value `data "remote_schema"` mints.
//
// It is a Ptah-internal marker rather than a runnable location, and this
// package is its only author. The scheme is reserved in the desired-state
// classifier, so a hand-written URL cannot impersonate a declared data source.
//
// It lives here rather than beside the classifier because the classifier
// already imports this package; the reverse would be an import cycle, and a
// second copy of the string is how two layers stop agreeing on what a marker
// looks like.
const RemoteSchemaMarkerScheme = "ptah-remote-schema"

// remoteSchemaDataSource resolves `data "remote_schema"` to the internal marker
// naming the artifact that holds the desired state.
//
// It returns a MARKER rather than a pulled schema, for two reasons. A project
// file is read by every verb, so fetching an artifact the run never uses would
// make an unrelated command fail whenever the registry is unreachable. And the
// marker keeps the capability off the flag surface: `--to oci://...` goes on
// being refused, because the pinned community binary answers that spelling with
// `unknown driver "oci"` at exit 1 (stokaro/ptah#1210).
//
// The mapping from name, tag and version is the one remote_dir uses, so a
// project addressing a migration directory and a schema in the same namespace
// cannot have the two disagree about what `version` means.
func (p atlasParser) remoteSchemaDataSource(block *hclsyntax.Block) (cty.Value, error) {
	// The block's shape is checked by the data-source shape validator, which
	// runs for a declaration whether or not anything references it.
	reference, err := p.remoteArtifactReference(block)
	if err != nil {
		return cty.NilVal, err
	}
	return cty.ObjectVal(map[string]cty.Value{
		// reference.OCI already carries the oci:// scheme, which is what every
		// Ptah artifact API expects.
		"url": cty.StringVal(RemoteSchemaMarkerScheme + "://" + reference.OCI),
	}), nil
}

// remoteArtifactReference resolves the block's name, tag and version into the
// OCI reference the artifact is read through.
//
// The attributes are spelled the way the block spells them rather than
// assembled into an atlas:// string first, because that string would only be
// taken apart again -- but the resolver still owns the mapping, so the two
// routes cannot disagree about what `version` means.
func (p atlasParser) remoteArtifactReference(block *hclsyntax.Block) (atlasregistry.Reference, error) {
	name, err := p.requiredDataSourceString(block, "name")
	if err != nil {
		return atlasregistry.Reference{}, err
	}
	query := url.Values{}
	for _, attr := range []string{"tag", "version"} {
		value, attrErr := p.optionalAtlasDataSourceString(block, attr)
		if attrErr != nil {
			return atlasregistry.Reference{}, attrErr
		}
		if strings.TrimSpace(value) != "" {
			query.Set(attr, value)
		}
	}
	raw := atlasregistry.Scheme + name
	if encoded := query.Encode(); encoded != "" {
		raw += "?" + encoded
	}
	reference, err := atlasregistry.Resolve(raw)
	if err != nil {
		return atlasregistry.Reference{}, p.dataSourceError(block, "resolving the reference", err)
	}
	return reference, nil
}
