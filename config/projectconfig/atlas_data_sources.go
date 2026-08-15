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
	"go.5x5.cz/ptah/internal/atlasruntimevar"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
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
	case "remote_dir", "remote_schema", "aws_rds_token", "gcp_cloudsql_token":
		return cty.NilVal, unsupported("data."+block.Labels[0], block.TypeRange)
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
	name := block.Labels[1]
	memURL := (&url.URL{
		Scheme: "mem",
		Path:   filepath.ToSlash(filepath.Join(memPath, name)),
	}).String()
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
		return map[string]any{}, nil
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
	case "remote_dir", "remote_schema", "aws_rds_token", "gcp_cloudsql_token":
		return unsupported("data."+typ, block.TypeRange)
	default:
		return unsupported("data."+typ, block.TypeRange)
	}
}

// validateAtlasDataSourceDeclarationShape validates the syntax Ptah knows
// without executing a data source. Recognized cloud sources stay lazy because
// this compatibility layer does not implement their runtime contracts yet.
func validateAtlasDataSourceDeclarationShape(block *hclsyntax.Block) error {
	switch block.Labels[0] {
	case "remote_dir", "remote_schema", "aws_rds_token", "gcp_cloudsql_token":
		return nil
	default:
		return validateAtlasDataSourceShape(block)
	}
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
	allSecrets := append([]string{}, secrets...)
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
