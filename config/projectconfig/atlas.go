package projectconfig

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

// AtlasLoadOptions selects Atlas project config evaluation settings.
type AtlasLoadOptions struct {
	EnvName string
	Vars    []string
}

// LoadAtlasFile loads the supported subset of an Atlas project config file. A
// missing file returns an empty config.
func LoadAtlasFile(path, envName string) (Config, error) {
	return LoadAtlasFileWithOptions(path, AtlasLoadOptions{EnvName: envName})
}

// LoadAtlasFileWithOptions loads the supported subset of an Atlas project
// config file with Atlas-compatible evaluation options. A missing file returns
// an empty config.
func LoadAtlasFileWithOptions(path string, opts AtlasLoadOptions) (Config, error) {
	return loadAtlasFileWithOptions(path, opts)
}

func loadAtlasFileWithOptions(
	path string,
	opts AtlasLoadOptions,
) (
	cfg Config,
	returnErr error,
) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return Config{}, fmt.Errorf("resolve atlas config path %s: %w", path, err)
	}
	root, err := os.OpenRoot(filepath.Dir(absolute))
	if errors.Is(err, fs.ErrNotExist) {
		return Config{}, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("open atlas config directory %s: %w", filepath.Dir(path), err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, root.Close())
	}()
	raw, err := fs.ReadFile(root.FS(), filepath.Base(absolute))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Config{}, nil
	case err != nil:
		return Config{}, fmt.Errorf("failed to read atlas config %s: %w", path, err)
	}
	return ParseAtlasFSWithOptions(raw, path, root.FS(), opts)
}

// ParseAtlas parses the supported subset of an Atlas project config file.
func ParseAtlas(data []byte, filename, envName string) (Config, error) {
	return ParseAtlasWithOptions(data, filename, AtlasLoadOptions{EnvName: envName})
}

// ParseAtlasWithOptions parses the supported subset of an Atlas project config
// file with Atlas-compatible evaluation options.
func ParseAtlasWithOptions(data []byte, filename string, opts AtlasLoadOptions) (Config, error) {
	if filename == "" {
		filename = AtlasFileName
	}
	return ParseAtlasFSWithOptions(data, filename, os.DirFS(filepath.Dir(filename)), opts)
}

// ParseAtlasFSWithOptions parses an Atlas project config while resolving
// file() and fileset() through fsys. fsys must be rooted at the directory that
// contains filename.
func ParseAtlasFSWithOptions(
	data []byte,
	filename string,
	fsys fs.FS,
	opts AtlasLoadOptions,
) (Config, error) {
	if filename == "" {
		filename = AtlasFileName
	}
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return Config{}, fmt.Errorf("parse atlas project config: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return Config{}, fmt.Errorf("parse atlas project config: unsupported body type %T", file.Body)
	}

	p, err := newAtlasParser(fsys, opts.Vars, filename)
	if err != nil {
		return Config{}, err
	}
	return p.parse(body, opts.EnvName)
}

type atlasParser struct {
	ctx         *hcl.EvalContext
	varOverride map[string]cty.Value
	// baseDir is the directory that contains the parsed atlas.hcl file, as
	// spelled by the caller. Relative data.external_schema working_dir values
	// resolve against it so the configured program runs where the config file
	// lives, matching how other atlas.hcl relative paths behave.
	baseDir string
	// externalSchemas holds the declared data.external_schema sources by name.
	externalSchemas map[string]externalSchemaDataSource
}

func newAtlasParser(fsys fs.FS, rawVars []string, filename string) (atlasParser, error) {
	overrides, err := parseAtlasVarOverrides(rawVars)
	if err != nil {
		return atlasParser{}, err
	}
	return atlasParser{
		ctx: &hcl.EvalContext{
			Variables: map[string]cty.Value{},
			Functions: map[string]function.Function{
				"file":       atlasFileFunc(fsys),
				"fileset":    atlasFilesetFunc(fsys),
				"format":     stdlib.FormatFunc,
				"getenv":     atlasGetenvFunc(),
				"jsonencode": stdlib.JSONEncodeFunc,
			},
		},
		varOverride:     overrides,
		baseDir:         filepath.Dir(filename),
		externalSchemas: map[string]externalSchemaDataSource{},
	}, nil
}

func (p atlasParser) parse(body *hclsyntax.Body, envName string) (Config, error) {
	if len(body.Attributes) > 0 {
		for name, attr := range body.Attributes {
			return Config{}, unsupportedAttr(name, attr)
		}
	}

	base := Config{}
	blocks, err := collectAtlasTopBlocks(body.Blocks)
	if err != nil {
		return Config{}, err
	}
	if err := validateAtlasEnvStructures(blocks.envs); err != nil {
		return Config{}, err
	}

	if err := p.configureEvalContext(blocks.variables, blocks.locals, blocks.data); err != nil {
		return Config{}, err
	}
	if err := p.parseSingleAtlasBlock(blocks.globalDiff, &base, p.parseDiff); err != nil {
		return Config{}, err
	}
	if err := p.parseSingleAtlasBlock(blocks.globalLint, &base, p.parseLint); err != nil {
		return Config{}, err
	}
	if len(blocks.envs) == 0 {
		return base, nil
	}

	selected, err := selectAtlasEnvBlock(blocks.envs, envName)
	if err != nil {
		return Config{}, err
	}
	cfg, err := p.parseEnv(selected)
	if err != nil {
		return Config{}, err
	}
	merged := Merge(base, cfg)
	if err := p.resolveExternalSchemaMarkers(&merged); err != nil {
		return Config{}, err
	}
	return merged, nil
}

type atlasTopBlocks struct {
	data       []*hclsyntax.Block
	globalDiff []*hclsyntax.Block
	globalLint []*hclsyntax.Block
	envs       []atlasEnvBlock
	locals     []*hclsyntax.Block
	variables  []*hclsyntax.Block
}

func collectAtlasTopBlocks(blocks []*hclsyntax.Block) (atlasTopBlocks, error) {
	collected := atlasTopBlocks{}
	for _, block := range blocks {
		if err := collectAtlasTopBlock(block, &collected); err != nil {
			return atlasTopBlocks{}, err
		}
	}
	return collected, nil
}

func collectAtlasTopBlock(block *hclsyntax.Block, collected *atlasTopBlocks) error {
	switch block.Type {
	case "data":
		collected.data = append(collected.data, block)
	case "diff":
		collected.globalDiff = append(collected.globalDiff, block)
	case "env":
		env, err := atlasEnvBlockFromHCL(block)
		if err != nil {
			return err
		}
		collected.envs = append(collected.envs, env)
	case "lint":
		collected.globalLint = append(collected.globalLint, block)
	case "locals":
		collected.locals = append(collected.locals, block)
	case "variable":
		collected.variables = append(collected.variables, block)
	default:
		return unsupportedBlock(block)
	}
	return nil
}

func (p atlasParser) parseSingleAtlasBlock(
	blocks []*hclsyntax.Block,
	cfg *Config,
	parse func(*hclsyntax.Block, *Config) error,
) error {
	if len(blocks) > 1 {
		return unsupportedBlock(blocks[1])
	}
	if len(blocks) == 0 {
		return nil
	}
	return parse(blocks[0], cfg)
}

type atlasEnvBlock struct {
	name  string
	block *hclsyntax.Block
}

func atlasEnvBlockFromHCL(block *hclsyntax.Block) (atlasEnvBlock, error) {
	if len(block.Labels) > 1 {
		return atlasEnvBlock{}, unsupportedBlock(block)
	}
	name := ""
	if len(block.Labels) == 1 {
		name = block.Labels[0]
	}
	return atlasEnvBlock{
		name:  name,
		block: block,
	}, nil
}

func (p atlasParser) parseEnv(env atlasEnvBlock) (Config, error) {
	cfg := Config{
		EnvName: env.name,
	}
	cfg.presence.mark(fieldEnvName)

	for attrName, attr := range env.block.Body.Attributes {
		if err := p.parseEnvAttr(attrName, attr, &cfg); err != nil {
			return Config{}, err
		}
	}

	seen := map[string]struct{}{}
	for _, nested := range env.block.Body.Blocks {
		if err := p.parseEnvBlock(nested, seen, &cfg); err != nil {
			return Config{}, err
		}
	}

	return cfg, nil
}

func (p atlasParser) parseEnvBlock(block *hclsyntax.Block, seen map[string]struct{}, cfg *Config) error {
	if _, ok := seen[block.Type]; ok {
		return unsupportedBlock(block)
	}
	seen[block.Type] = struct{}{}

	switch block.Type {
	case "diff":
		return p.parseDiff(block, cfg)
	case "format":
		return p.parseFormat(block, cfg)
	case "lint":
		return p.parseLint(block, cfg)
	case "migration":
		return p.parseMigration(block, cfg)
	case "schema":
		return p.parseSchema(block, cfg)
	default:
		return unsupportedBlock(block)
	}
}

func (p atlasParser) parseSchema(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "src":
			values, err := p.stringOrStringListAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.SchemaSources = values
			cfg.presence.mark(fieldSchemaSources)
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	seenMode := false
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "mode":
			if seenMode {
				return unsupportedBlock(nested)
			}
			seenMode = true
			if err := p.parseSchemaMode(nested, cfg); err != nil {
				return err
			}
		default:
			return unsupportedBlock(nested)
		}
	}
	return nil
}

func (p atlasParser) parseSchemaMode(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		value, err := p.schemaModeAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "funcs":
			cfg.Schema.Mode.Funcs = value
		case "objects":
			cfg.Schema.Mode.Objects = value
		case "permissions":
			cfg.Schema.Mode.Permissions = value
		case "roles":
			cfg.Schema.Mode.Roles = value
		case "tables":
			cfg.Schema.Mode.Tables = value
		case "triggers":
			cfg.Schema.Mode.Triggers = value
		case "types":
			cfg.Schema.Mode.Types = value
		case "views":
			cfg.Schema.Mode.Views = value
		case "sensitive":
			if value.Value {
				return unsupportedAttr(attrName, attr)
			}
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseEnvAttr(attrName string, attr *hclsyntax.Attribute, cfg *Config) error {
	switch attrName {
	case "url":
		value, err := p.stringAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.DatabaseURL = value
		cfg.presence.mark(fieldDatabaseURL)
	case "dev":
		value, err := p.stringAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.DevURL = value
		cfg.presence.mark(fieldDevURL)
	case "src":
		values, err := p.stringOrStringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.SchemaSources = values
		cfg.presence.mark(fieldSchemaSources)
	case "exclude":
		values, err := p.stringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.Exclude = values
		cfg.presence.mark(fieldExclude)
	default:
		return unsupportedAttr(attrName, attr)
	}
	return nil
}

func (p atlasParser) parseMigration(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	migration := cfg.Migration
	if migration.Format == "" {
		migration.Format = "atlas"
	}
	cfg.presence.mark(fieldMigrationFormat)
	if migration.RevisionFormat == "" {
		migration.RevisionFormat = "atlas"
	}
	cfg.presence.mark(fieldMigrationRevisionFormat)

	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "dir":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.Dir = normalizeAtlasMigrationDir(value)
			cfg.presence.mark(fieldMigrationDir)
		case "format":
			value, err := p.scopedEnumOrStringAttr(
				attrName,
				attr,
				"atlas",
				"golang-migrate",
				"goose",
				"flyway",
				"liquibase",
				"dbmate",
			)
			if err != nil {
				return err
			}
			migration.Format = value
			cfg.presence.mark(fieldMigrationFormat)
		case "revisions_schema":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.RevisionsSchema = value
			cfg.presence.mark(fieldMigrationRevisionsSchema)
		case "lock_timeout":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.LockTimeout = value
			cfg.presence.mark(fieldMigrationLockTimeout)
		case "exec_order":
			value, err := p.scopedEnumOrStringAttr(attrName, attr, "LINEAR", "LINEAR_SKIP", "NON_LINEAR")
			if err != nil {
				return err
			}
			migration.ExecOrder = strings.ReplaceAll(strings.ToLower(value), "_", "-")
			cfg.presence.mark(fieldMigrationExecOrder)
		case "tx_mode":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.TxMode = value
			cfg.presence.mark(fieldMigrationTxMode)
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	cfg.Migration = migration
	return nil
}

func (p atlasParser) parseLint(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		if err := p.parseLintAttr(attrName, attr, cfg); err != nil {
			return err
		}
	}
	if err := p.parseLintPolicyBlocks(block, cfg); err != nil {
		return err
	}
	if cfg.presence.has(fieldLintLatest) &&
		cfg.presence.has(fieldLintGitBase) &&
		cfg.Lint.GitBase != "" {
		return fmt.Errorf(
			"atlas.hcl lint.latest and lint.git.base are mutually exclusive at %s:%d",
			block.TypeRange.Filename,
			block.TypeRange.Start.Line,
		)
	}
	return nil
}

func (p atlasParser) parseLintAttr(attrName string, attr *hclsyntax.Attribute, cfg *Config) error {
	switch attrName {
	case "latest":
		value, err := p.intAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.Lint.Latest = &value
		cfg.presence.mark(fieldLintLatest)
	case "log":
		// Atlas's lint.log is a Go text/template that renders the migrate lint
		// output. It shares the format IR with format.migrate.lint, so the CLI
		// --format flag and env/global merge precedence apply uniformly (env
		// lint.log overrides the global one; an explicit --format overrides both).
		value, err := p.nonEmptyStringAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.Format.Migrate.Lint = value
		cfg.presence.mark(fieldFormatMigrateLint)
	default:
		return unsupportedAttr(attrName, attr)
	}
	return nil
}

func (p atlasParser) parseLintPolicyBlocks(block *hclsyntax.Block, cfg *Config) error {
	seen := map[string]struct{}{}
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "concurrent_index":
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintAnalyzer(nested, cfg, "PG101", "PG103")
			}); err != nil {
				return err
			}
		case "data_depend":
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintAnalyzer(nested, cfg, "DD")
			}); err != nil {
				return err
			}
		case "destructive":
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintAnalyzer(nested, cfg, "DS")
			}); err != nil {
				return err
			}
		case "git":
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintGit(nested, cfg)
			}); err != nil {
				return err
			}
		case "nestedtx":
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintAnalyzer(nested, cfg, "TX201")
			}); err != nil {
				return err
			}
		case "incompatible":
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintAnalyzer(nested, cfg, "BC")
			}); err != nil {
				return err
			}
		default:
			return unsupportedBlock(nested)
		}
	}
	return nil
}

func (p atlasParser) parseSingleLintBlock(
	block *hclsyntax.Block,
	seen map[string]struct{},
	parse func() error,
) error {
	if _, ok := seen[block.Type]; ok {
		return unsupportedBlock(block)
	}
	seen[block.Type] = struct{}{}
	return parse()
}

func (p atlasParser) parseLintAnalyzer(block *hclsyntax.Block, cfg *Config, codes ...string) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "error":
			value, err := p.boolAttr(attrName, attr)
			if err != nil {
				return err
			}
			severity := "warning"
			if value {
				severity = "error"
			}
			for _, code := range codes {
				setLintRuleSeverity(cfg, code, severity)
			}
		case "force":
			return unsupportedAttr(attrName, attr)
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func setLintRuleSeverity(cfg *Config, code, severity string) {
	config := lintRuleConfig(cfg, code)
	config.Severity = severity
	setLintRuleConfig(cfg, code, config)
}

func lintRuleConfig(cfg *Config, code string) LintRuleConfig {
	if cfg.Lint.RuleConfigs == nil {
		return LintRuleConfig{}
	}
	return cfg.Lint.RuleConfigs[code]
}

func setLintRuleConfig(cfg *Config, code string, config LintRuleConfig) {
	if cfg.Lint.RuleConfigs == nil {
		cfg.Lint.RuleConfigs = map[string]LintRuleConfig{}
	}
	cfg.Lint.RuleConfigs[code] = config
	cfg.presence.mark(fieldLintRuleConfigs)
	cfg.presence.mark(lintRuleSeverityField(code))
}

func (p atlasParser) parseLintGit(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "base":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.Lint.GitBase = value
			cfg.presence.mark(fieldLintGitBase)
		case "dir":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.Lint.GitDir = value
			cfg.presence.mark(fieldLintGitDir)
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseFormat(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	if len(block.Body.Attributes) > 0 {
		for name, attr := range block.Body.Attributes {
			return unsupportedAttr(name, attr)
		}
	}
	seenMigrate := false
	seenSchema := false
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "migrate":
			if seenMigrate {
				return unsupportedBlock(nested)
			}
			seenMigrate = true
			if err := p.parseMigrateFormat(nested, cfg); err != nil {
				return err
			}
		case "schema":
			if seenSchema {
				return unsupportedBlock(nested)
			}
			seenSchema = true
			if err := p.parseSchemaFormat(nested, cfg); err != nil {
				return err
			}
		default:
			return unsupportedBlock(nested)
		}
	}
	return nil
}

func (p atlasParser) parseMigrateFormat(block *hclsyntax.Block, cfg *Config) error {
	return p.parseFormatAttributes(block, &cfg.presence, map[string]atlasFormatField{
		"apply":  {destination: &cfg.Format.Migrate.Apply, presence: fieldFormatMigrateApply},
		"diff":   {destination: &cfg.Format.Migrate.Diff, presence: fieldFormatMigrateDiff},
		"lint":   {destination: &cfg.Format.Migrate.Lint, presence: fieldFormatMigrateLint},
		"status": {destination: &cfg.Format.Migrate.Status, presence: fieldFormatMigrateStatus},
	})
}

func (p atlasParser) parseSchemaFormat(block *hclsyntax.Block, cfg *Config) error {
	return p.parseFormatAttributes(block, &cfg.presence, map[string]atlasFormatField{
		"apply":   {destination: &cfg.Format.Schema.Apply, presence: fieldFormatSchemaApply},
		"clean":   {destination: &cfg.Format.Schema.Clean, presence: fieldFormatSchemaClean},
		"diff":    {destination: &cfg.Format.Schema.Diff, presence: fieldFormatSchemaDiff},
		"inspect": {destination: &cfg.Format.Schema.Inspect, presence: fieldFormatSchemaInspect},
	})
}

type atlasFormatField struct {
	destination *string
	presence    configField
}

func (p atlasParser) parseFormatAttributes(
	block *hclsyntax.Block,
	presence *configPresence,
	fields map[string]atlasFormatField,
) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		field, ok := fields[attrName]
		if !ok {
			return unsupportedAttr(attrName, attr)
		}
		value, err := p.nonEmptyStringAttr(attrName, attr)
		if err != nil {
			return err
		}
		*field.destination = value
		presence.mark(field.presence)
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseDiff(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	if len(block.Body.Attributes) > 0 {
		for name, attr := range block.Body.Attributes {
			return unsupportedAttr(name, attr)
		}
	}
	seenSkip := false
	seenConcurrentIndex := false
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "skip":
			if seenSkip {
				return unsupportedBlock(nested)
			}
			seenSkip = true
			if err := p.parseDiffSkip(nested, cfg); err != nil {
				return err
			}
		case "concurrent_index":
			if seenConcurrentIndex {
				return unsupportedBlock(nested)
			}
			seenConcurrentIndex = true
			if err := p.parseDiffConcurrentIndex(nested, cfg); err != nil {
				return err
			}
		default:
			return unsupportedBlock(nested)
		}
	}
	return nil
}

func (p atlasParser) parseDiffSkip(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		value, err := p.configBoolAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "drop_table":
			cfg.Diff.Skip.DropTable = value
		case "drop_schema":
			return unsupportedAttr(attrName, attr)
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseDiffConcurrentIndex(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		value, err := p.configBoolAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "create":
			cfg.Diff.ConcurrentIndex.Create = value
		case "drop":
			cfg.Diff.ConcurrentIndex.Drop = value
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) configureEvalContext(
	variableBlocks []*hclsyntax.Block,
	localsBlocks []*hclsyntax.Block,
	dataBlocks []*hclsyntax.Block,
) error {
	if err := p.configureVariables(variableBlocks); err != nil {
		return err
	}
	if err := p.configureLocals(localsBlocks); err != nil {
		return err
	}
	return p.configureDataSources(dataBlocks)
}

func (p atlasParser) configureVariables(blocks []*hclsyntax.Block) error {
	vars := map[string]cty.Value{}
	for _, block := range blocks {
		if len(block.Labels) != 1 {
			return unsupportedBlock(block)
		}
		name := block.Labels[0]
		if _, ok := vars[name]; ok {
			return fmt.Errorf("duplicate atlas.hcl variable %q at %s:%d", name, block.TypeRange.Filename, block.TypeRange.Start.Line)
		}
		variable, err := p.parseVariableBlock(block)
		if err != nil {
			return err
		}
		value, err := p.variableValue(variable)
		if err != nil {
			return err
		}
		vars[name] = value
	}
	for name, value := range p.varOverride {
		if _, ok := vars[name]; !ok {
			vars[name] = value
		}
	}
	if len(vars) > 0 {
		p.ctx.Variables["var"] = cty.ObjectVal(vars)
	}
	return nil
}

func parseAtlasVarOverrides(rawVars []string) (map[string]cty.Value, error) {
	vars := map[string]cty.Value{}
	for _, raw := range rawVars {
		values, err := csv.NewReader(strings.NewReader(raw)).Read()
		if err != nil {
			return nil, fmt.Errorf("parse atlas variable override %q: %w", raw, err)
		}
		for _, value := range values {
			name, text, ok := strings.Cut(value, "=")
			if !ok {
				return nil, fmt.Errorf("atlas variable overrides must use name=value, got %q", value)
			}
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("atlas variable override %q has an empty name", value)
			}
			value := cty.StringVal(text)
			if existing, ok := vars[name]; ok {
				value = appendAtlasVarValue(existing, value)
			}
			vars[name] = value
		}
	}
	return vars, nil
}

func appendAtlasVarValue(existing cty.Value, value cty.Value) cty.Value {
	if existing.Type().IsListType() {
		return cty.ListVal(append(existing.AsValueSlice(), value))
	}
	return cty.ListVal([]cty.Value{existing, value})
}

// atlasVariable is one parsed atlas.hcl variable block.
type atlasVariable struct {
	name       string
	typ        cty.Type // cty.NilType when the block declares no type
	sensitive  bool
	defValue   cty.Value
	hasDefault bool
}

// typed reports whether the variable block declared a type constraint.
func (v atlasVariable) typed() bool {
	return v.typ != cty.NilType
}

// atlasSupportedVariableTypes names the variable type constraints Ptah
// implements, for error messages. Anything else fails loudly so a config never
// silently drops a constraint Atlas enforces.
const atlasSupportedVariableTypes = "string, number, bool, and list(string)"

func (p atlasParser) parseVariableBlock(block *hclsyntax.Block) (atlasVariable, error) {
	variable := atlasVariable{name: block.Labels[0]}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "default", "description", "sensitive", "type":
		default:
			return atlasVariable{}, unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		// validation blocks land here: their semantics are not implemented, so
		// they fail loudly instead of being silently dropped.
		return atlasVariable{}, unsupportedBlock(block.Body.Blocks[0])
	}
	if attr, ok := block.Body.Attributes["type"]; ok {
		typ, err := atlasVariableTypeAttr(variable.name, attr)
		if err != nil {
			return atlasVariable{}, err
		}
		variable.typ = typ
	}
	if attr, ok := block.Body.Attributes["sensitive"]; ok {
		sensitive, err := p.boolAttr("sensitive", attr)
		if err != nil {
			return atlasVariable{}, err
		}
		variable.sensitive = sensitive
	}
	attr, ok := block.Body.Attributes["default"]
	if !ok {
		return variable, nil
	}
	if _, overridden := p.varOverride[variable.name]; overridden {
		// An override replaces the default, so the default expression stays
		// unevaluated: a default that only resolves in another environment
		// (for example file() on a machine-specific path) must not fail an
		// invocation that supplies the value. variableValue checks the
		// override before hasDefault, so the unset default is never read.
		return variable, nil
	}
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return atlasVariable{}, unsupportedAttr("default", attr)
	}
	if variable.typed() {
		converted, err := convert.Convert(value, variable.typ)
		if err != nil {
			// The default value stays out of the message: for sensitive
			// variables it must not leak, and the file location already
			// pinpoints it for everyone else.
			return atlasVariable{}, fmt.Errorf(
				"atlas.hcl variable %q default does not match type %s at %s:%d",
				variable.name,
				atlasVariableTypeName(variable.typ),
				attr.NameRange.Filename,
				attr.NameRange.Start.Line,
			)
		}
		value = converted
	}
	variable.defValue = value
	variable.hasDefault = true
	return variable, nil
}

func (p atlasParser) variableValue(variable atlasVariable) (cty.Value, error) {
	if value, ok := p.varOverride[variable.name]; ok {
		return convertAtlasVariableOverride(variable, value)
	}
	if !variable.hasDefault {
		return cty.NilVal, fmt.Errorf("atlas.hcl variable %q requires a default or --var %s=value", variable.name, variable.name)
	}
	return variable.defValue, nil
}

// convertAtlasVariableOverride converts a --var override (a string, or a list
// of strings when the flag was repeated) to the variable's declared type.
// Overrides for untyped variables keep their raw shape.
func convertAtlasVariableOverride(variable atlasVariable, value cty.Value) (cty.Value, error) {
	if !variable.typed() {
		return value, nil
	}
	if variable.typ.IsListType() {
		if value.Type().IsListType() {
			return value, nil
		}
		// One --var occurrence for a list(string) variable is a one-element
		// list, consistent with N occurrences producing an N-element list.
		return cty.ListVal([]cty.Value{value}), nil
	}
	if value.Type().IsListType() {
		return cty.NilVal, fmt.Errorf(
			"atlas.hcl variable %q expects %s, got %d --var values",
			variable.name,
			atlasVariableTypeName(variable.typ),
			value.LengthInt(),
		)
	}
	converted, err := convert.Convert(value, variable.typ)
	if err != nil {
		return cty.NilVal, fmt.Errorf(
			"atlas.hcl variable %q expects %s, got --var value %s",
			variable.name,
			atlasVariableTypeName(variable.typ),
			redactedAtlasVariableValue(variable, value),
		)
	}
	return converted, nil
}

// redactedAtlasVariableValue renders an override value for an error message.
// Sensitive variables never leak their raw value into error text.
func redactedAtlasVariableValue(variable atlasVariable, value cty.Value) string {
	if variable.sensitive {
		return "(sensitive value)"
	}
	return fmt.Sprintf("%q", value.AsString())
}

func atlasVariableTypeAttr(name string, attr *hclsyntax.Attribute) (cty.Type, error) {
	if typ, ok := atlasVariableType(attr.Expr); ok {
		return typ, nil
	}
	return cty.NilType, fmt.Errorf(
		"atlas.hcl variable %q type at %s:%d is not supported: supported types are %s",
		name,
		attr.NameRange.Filename,
		attr.NameRange.Start.Line,
		atlasSupportedVariableTypes,
	)
}

// atlasVariableType maps the accepted atlas.hcl type expressions to cty types.
// Exotic constraints (object, tuple, map, set, ...) report not-ok so the
// caller rejects them with an error naming the supported set.
func atlasVariableType(expr hclsyntax.Expression) (cty.Type, bool) {
	switch expr := expr.(type) {
	case *hclsyntax.ScopeTraversalExpr:
		switch atlasTypeKeyword(expr) {
		case "string":
			return cty.String, true
		case "number":
			return cty.Number, true
		case "bool":
			return cty.Bool, true
		}
	case *hclsyntax.FunctionCallExpr:
		if expr.Name == "list" && len(expr.Args) == 1 {
			if arg, ok := expr.Args[0].(*hclsyntax.ScopeTraversalExpr); ok && atlasTypeKeyword(arg) == "string" {
				return cty.List(cty.String), true
			}
		}
	}
	return cty.NilType, false
}

func atlasTypeKeyword(expr *hclsyntax.ScopeTraversalExpr) string {
	if len(expr.Traversal) != 1 {
		return ""
	}
	root, ok := expr.Traversal[0].(hcl.TraverseRoot)
	if !ok {
		return ""
	}
	return root.Name
}

func atlasVariableTypeName(typ cty.Type) string {
	switch {
	case typ == cty.String:
		return "string"
	case typ == cty.Number:
		return "number"
	case typ == cty.Bool:
		return "bool"
	case typ.IsListType():
		return "list(string)"
	default:
		return typ.FriendlyName()
	}
}

func (p atlasParser) configureLocals(blocks []*hclsyntax.Block) error {
	locals := map[string]cty.Value{}
	pending := hclsyntax.Attributes{}
	for _, block := range blocks {
		if len(block.Labels) > 0 {
			return unsupportedBlock(block)
		}
		if len(block.Body.Blocks) > 0 {
			return unsupportedBlock(block.Body.Blocks[0])
		}
		for name, attr := range block.Body.Attributes {
			if _, ok := pending[name]; ok {
				return fmt.Errorf("duplicate atlas.hcl local %q at %s:%d", name, attr.NameRange.Filename, attr.NameRange.Start.Line)
			}
			pending[name] = attr
		}
	}
	return p.evaluateLocals(locals, pending)
}

func (p atlasParser) evaluateLocals(locals map[string]cty.Value, pending hclsyntax.Attributes) error {
	for len(pending) > 0 {
		firstName := sortedAttributeNames(pending)[0]
		progress := false
		for _, name := range sortedAttributeNames(pending) {
			attr := pending[name]
			value, diags := attr.Expr.Value(p.ctx)
			if diags.HasErrors() {
				continue
			}
			locals[name] = value
			p.ctx.Variables["local"] = cty.ObjectVal(locals)
			delete(pending, name)
			progress = true
		}
		if !progress {
			return unsupportedAttr(firstName, pending[firstName])
		}
	}
	return nil
}

func (p atlasParser) configureDataSources(blocks []*hclsyntax.Block) error {
	hclSchemas := map[string]cty.Value{}
	externalSchemas := map[string]cty.Value{}
	for _, block := range blocks {
		if len(block.Labels) != 2 {
			return unsupportedBlock(block)
		}
		switch block.Labels[0] {
		case "hcl_schema":
			if err := p.configureHCLSchemaDataSource(block, hclSchemas); err != nil {
				return err
			}
		case "external_schema":
			if err := p.configureExternalSchemaDataSource(block, externalSchemas); err != nil {
				return err
			}
		default:
			return unsupported(block.Type+"."+block.Labels[0], block.TypeRange)
		}
	}
	data := map[string]cty.Value{}
	if len(hclSchemas) > 0 {
		data["hcl_schema"] = cty.ObjectVal(hclSchemas)
	}
	if len(externalSchemas) > 0 {
		data["external_schema"] = cty.ObjectVal(externalSchemas)
	}
	if len(data) > 0 {
		p.ctx.Variables["data"] = cty.ObjectVal(data)
	}
	return nil
}

func (p atlasParser) configureHCLSchemaDataSource(
	block *hclsyntax.Block,
	values map[string]cty.Value,
) error {
	name := block.Labels[1]
	if _, ok := values[name]; ok {
		return fmt.Errorf("duplicate atlas.hcl data.hcl_schema %q at %s:%d", name, block.TypeRange.Filename, block.TypeRange.Start.Line)
	}
	value, err := p.hclSchemaDataSource(block)
	if err != nil {
		return err
	}
	values[name] = value
	return nil
}

// externalSchemaMarkerScheme prefixes the opaque data.external_schema.<name>.url
// value. It is a Ptah-internal marker, never a runnable location: the scheme is
// reserved (Classify and the schema-file loaders reject it), so a
// user-provided URL cannot collide with a declared data source.
const externalSchemaMarkerScheme = "ptah-external-schema://"

// externalSchemaMarkerName reports whether value is a data.external_schema
// marker URL and returns the declared data source name it references.
func externalSchemaMarkerName(value string) (string, bool) {
	return strings.CutPrefix(value, externalSchemaMarkerScheme)
}

// externalSchemaDataSource is one declared data.external_schema source. It
// mirrors ExternalSchemaConfig: program is an explicit argv list run without a
// shell, format is the stdout format, working_dir is the program's working
// directory, and env holds extra KEY=VALUE entries.
type externalSchemaDataSource struct {
	program    []string
	format     string
	workingDir string
	env        []string
}

func (p atlasParser) configureExternalSchemaDataSource(
	block *hclsyntax.Block,
	values map[string]cty.Value,
) error {
	name := block.Labels[1]
	if _, ok := p.externalSchemas[name]; ok {
		return fmt.Errorf("duplicate atlas.hcl data.external_schema %q at %s:%d", name, block.TypeRange.Filename, block.TypeRange.Start.Line)
	}
	source, err := p.externalSchemaDataSource(block)
	if err != nil {
		return err
	}
	p.externalSchemas[name] = source
	values[name] = cty.ObjectVal(map[string]cty.Value{
		"url": cty.StringVal(externalSchemaMarkerScheme + name),
	})
	return nil
}

func (p atlasParser) externalSchemaDataSource(block *hclsyntax.Block) (externalSchemaDataSource, error) {
	if len(block.Body.Blocks) > 0 {
		return externalSchemaDataSource{}, unsupportedBlock(block.Body.Blocks[0])
	}
	name := block.Labels[1]
	source := externalSchemaDataSource{format: "sql"}
	for attrName, attr := range block.Body.Attributes {
		if err := p.parseExternalSchemaAttr(name, attrName, attr, &source); err != nil {
			return externalSchemaDataSource{}, err
		}
	}
	if len(source.program) == 0 {
		return externalSchemaDataSource{}, fmt.Errorf(
			"atlas.hcl data.external_schema %q requires a non-empty program list at %s:%d",
			name, block.TypeRange.Filename, block.TypeRange.Start.Line,
		)
	}
	return source, nil
}

func (p atlasParser) parseExternalSchemaAttr(
	name string,
	attrName string,
	attr *hclsyntax.Attribute,
	source *externalSchemaDataSource,
) error {
	switch attrName {
	case "program":
		values, err := p.stringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
			return fmt.Errorf(
				"atlas.hcl data.external_schema %q requires a non-empty program list at %s:%d",
				name, attr.NameRange.Filename, attr.NameRange.Start.Line,
			)
		}
		source.program = values
		return nil
	case "format":
		value, err := p.stringAttr(attrName, attr)
		if err != nil {
			return err
		}
		return applyExternalSchemaFormat(name, value, attr, source)
	case "working_dir":
		value, err := p.stringAttr(attrName, attr)
		if err != nil {
			return err
		}
		source.workingDir = value
		return nil
	case "env":
		values, err := p.stringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		return applyExternalSchemaEnv(name, values, attr, source)
	default:
		return unsupportedAttr(attrName, attr)
	}
}

func applyExternalSchemaFormat(
	name string,
	value string,
	attr *hclsyntax.Attribute,
	source *externalSchemaDataSource,
) error {
	switch normalized := strings.ToLower(strings.TrimSpace(value)); normalized {
	case "sql", "hcl", "yaml":
		source.format = normalized
		return nil
	case "yml":
		source.format = "yaml"
		return nil
	default:
		return fmt.Errorf(
			"atlas.hcl data.external_schema %q format must be sql, hcl, or yaml, got %q at %s:%d",
			name, value, attr.NameRange.Filename, attr.NameRange.Start.Line,
		)
	}
}

func applyExternalSchemaEnv(
	name string,
	values []string,
	attr *hclsyntax.Attribute,
	source *externalSchemaDataSource,
) error {
	for _, entry := range values {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(key) == "" {
			return fmt.Errorf(
				"atlas.hcl data.external_schema %q env entries must be KEY=VALUE, got %q at %s:%d",
				name, entry, attr.NameRange.Filename, attr.NameRange.Start.Line,
			)
		}
	}
	source.env = values
	return nil
}

// resolveExternalSchemaMarkers translates a data.external_schema marker
// referenced by the selected env into the merged config's ExternalSchema
// block. The marker is only valid as the env desired-state source (env src or
// schema.src); any other reference is rejected. Declared-but-unreferenced data
// sources are ignored and never executed.
func (p atlasParser) resolveExternalSchemaMarkers(cfg *Config) error {
	if err := rejectExternalSchemaMarker(cfg.DatabaseURL, "env url"); err != nil {
		return err
	}
	if err := rejectExternalSchemaMarker(cfg.DevURL, "env dev"); err != nil {
		return err
	}
	if err := rejectExternalSchemaMarker(cfg.Migration.Dir, "env migration.dir"); err != nil {
		return err
	}
	for _, value := range cfg.Exclude {
		if err := rejectExternalSchemaMarker(value, "env exclude"); err != nil {
			return err
		}
	}
	return p.consumeExternalSchemaMarker(cfg)
}

func rejectExternalSchemaMarker(value, location string) error {
	name, ok := externalSchemaMarkerName(value)
	if !ok {
		return nil
	}
	return fmt.Errorf(
		"atlas.hcl data.external_schema.%s.url can only be the env desired-state source (env src or schema.src), not %s",
		name, location,
	)
}

func (p atlasParser) consumeExternalSchemaMarker(cfg *Config) error {
	for _, value := range cfg.SchemaSources {
		name, ok := externalSchemaMarkerName(value)
		if !ok {
			continue
		}
		if len(cfg.SchemaSources) > 1 {
			return fmt.Errorf("atlas.hcl data.external_schema.%s.url must be the only env src value", name)
		}
		source, declared := p.externalSchemas[name]
		if !declared {
			return fmt.Errorf("atlas.hcl env src references undeclared data.external_schema %q", name)
		}
		p.applyExternalSchemaSource(cfg, source)
		return nil
	}
	return nil
}

// applyExternalSchemaSource replaces the config's external schema wholesale
// with the referenced data source and drops the marker from the schema
// sources, so downstream consumers see the ordinary "external schema
// configured" state. Every field is marked present, defaults included, so an
// atlas.hcl data source never mixes with a ptah.yaml external_schema block.
func (p atlasParser) applyExternalSchemaSource(cfg *Config, source externalSchemaDataSource) {
	cfg.ExternalSchema = ExternalSchemaConfig{
		Program:    slices.Clone(source.program),
		Format:     source.format,
		WorkingDir: p.resolveExternalSchemaWorkingDir(source.workingDir),
		Env:        slices.Clone(source.env),
		Origin:     AtlasFileName,
	}
	cfg.presence.mark(fieldExternalSchemaProgram)
	cfg.presence.mark(fieldExternalSchemaFormat)
	cfg.presence.mark(fieldExternalSchemaWorkingDir)
	cfg.presence.mark(fieldExternalSchemaEnv)
	cfg.SchemaSources = nil
	cfg.presence.unmark(fieldSchemaSources)
}

// resolveExternalSchemaWorkingDir resolves a relative working_dir against the
// atlas.hcl directory, so the program runs where the config file lives no
// matter which directory the CLI was invoked from.
func (p atlasParser) resolveExternalSchemaWorkingDir(dir string) string {
	if dir == "" || filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(p.baseDir, dir)
}

func (p atlasParser) hclSchemaDataSource(block *hclsyntax.Block) (cty.Value, error) {
	if len(block.Body.Blocks) > 0 {
		return cty.NilVal, unsupportedBlock(block.Body.Blocks[0])
	}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "path", "paths":
		default:
			return cty.NilVal, unsupportedAttr(attrName, attr)
		}
	}
	pathAttr, hasPath := block.Body.Attributes["path"]
	pathsAttr, hasPaths := block.Body.Attributes["paths"]
	switch {
	case hasPath && hasPaths:
		return cty.NilVal, unsupportedAttr("paths", pathsAttr)
	case hasPath:
		value, err := p.stringAttr("path", pathAttr)
		if err != nil {
			return cty.NilVal, err
		}
		url, err := atlasLocalFileURL(value, pathAttr)
		if err != nil {
			return cty.NilVal, err
		}
		return cty.ObjectVal(map[string]cty.Value{
			"url": cty.StringVal(url),
		}), nil
	case hasPaths:
		values, err := p.stringListAttr("paths", pathsAttr)
		if err != nil {
			return cty.NilVal, err
		}
		urls := make([]string, 0, len(values))
		for _, value := range values {
			url, err := atlasLocalFileURL(value, pathsAttr)
			if err != nil {
				return cty.NilVal, err
			}
			urls = append(urls, url)
		}
		return cty.ObjectVal(map[string]cty.Value{
			"url": ctyStringList(urls),
		}), nil
	default:
		return cty.NilVal, fmt.Errorf("atlas.hcl data.hcl_schema %q requires path or paths at %s:%d",
			block.Labels[1], block.TypeRange.Filename, block.TypeRange.Start.Line)
	}
}

func selectAtlasEnvBlock(envs []atlasEnvBlock, envName string) (atlasEnvBlock, error) {
	if envName != "" {
		for _, env := range envs {
			if env.name == envName {
				return env, nil
			}
		}
		return atlasEnvBlock{}, fmt.Errorf("atlas env %q not found", envName)
	}
	if len(envs) == 1 {
		return envs[0], nil
	}
	return atlasEnvBlock{}, fmt.Errorf("atlas.hcl contains multiple env blocks; pass --env")
}

func (p atlasParser) stringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() || value.Type() != cty.String {
		return "", unsupportedAttr(name, attr)
	}
	return value.AsString(), nil
}

func (p atlasParser) nonEmptyStringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, err := p.stringAttr(name, attr)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(value) == "" {
		return "", unsupportedAttr(name, attr)
	}
	return value, nil
}

func (p atlasParser) schemaModeAttr(name string, attr *hclsyntax.Attribute) (ConfigBool, error) {
	if name == "sensitive" {
		value, err := p.sensitiveModeAttr(name, attr)
		if err != nil {
			return ConfigBool{}, err
		}
		return ConfigBool{Value: value, Set: true}, nil
	}
	return p.configBoolAttr(name, attr)
}

func (p atlasParser) sensitiveModeAttr(name string, attr *hclsyntax.Attribute) (bool, error) {
	value, err := p.identifierOrStringAttr(name, attr)
	if err != nil {
		return false, err
	}
	switch strings.ToUpper(value) {
	case "DENY":
		return false, nil
	case "ALLOW":
		return true, nil
	default:
		return false, unsupportedAttr(name, attr)
	}
}

func (p atlasParser) identifierOrStringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if !diags.HasErrors() && value.Type() == cty.String {
		return value.AsString(), nil
	}
	traversal, ok := attr.Expr.(*hclsyntax.ScopeTraversalExpr)
	if !ok || len(traversal.Traversal) != 1 {
		return "", unsupportedAttr(name, attr)
	}
	root, ok := traversal.Traversal[0].(hcl.TraverseRoot)
	if !ok {
		return "", unsupportedAttr(name, attr)
	}
	return root.Name, nil
}

func (p atlasParser) scopedEnumOrStringAttr(
	name string,
	attr *hclsyntax.Attribute,
	allowed ...string,
) (string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if !diags.HasErrors() && value.Type() == cty.String {
		return value.AsString(), nil
	}
	traversal, ok := attr.Expr.(*hclsyntax.ScopeTraversalExpr)
	if !ok || len(traversal.Traversal) != 1 {
		return "", unsupportedAttr(name, attr)
	}
	root, ok := traversal.Traversal[0].(hcl.TraverseRoot)
	if !ok || !slices.Contains(allowed, root.Name) {
		return "", unsupportedAttr(name, attr)
	}
	return root.Name, nil
}

func (p atlasParser) configBoolAttr(name string, attr *hclsyntax.Attribute) (ConfigBool, error) {
	value, err := p.boolAttr(name, attr)
	if err != nil {
		return ConfigBool{}, err
	}
	return ConfigBool{Value: value, Set: true}, nil
}

func (p atlasParser) boolAttr(name string, attr *hclsyntax.Attribute) (bool, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return false, unsupportedAttr(name, attr)
	}
	return value.True(), nil
}

func (p atlasParser) stringOrStringListAttr(name string, attr *hclsyntax.Attribute) ([]string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, unsupportedAttr(name, attr)
	}
	if value.Type() == cty.String {
		return []string{value.AsString()}, nil
	}
	return stringListValue(name, attr, value)
}

func (p atlasParser) intAttr(name string, attr *hclsyntax.Attribute) (int, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() || value.Type() != cty.Number {
		return 0, unsupportedAttr(name, attr)
	}
	raw, accuracy := value.AsBigFloat().Int64()
	if accuracy != big.Exact {
		return 0, unsupportedAttr(name, attr)
	}
	return int(raw), nil
}

func (p atlasParser) stringListAttr(name string, attr *hclsyntax.Attribute) ([]string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, unsupportedAttr(name, attr)
	}
	return stringListValue(name, attr, value)
}

func stringListValue(name string, attr *hclsyntax.Attribute, value cty.Value) ([]string, error) {
	valueType := value.Type()
	if !value.CanIterateElements() || (!valueType.IsTupleType() && !valueType.IsListType()) {
		return nil, unsupportedAttr(name, attr)
	}
	values := make([]string, 0, value.LengthInt())
	it := value.ElementIterator()
	for it.Next() {
		_, item := it.Element()
		if item.Type() != cty.String {
			return nil, unsupportedAttr(name, attr)
		}
		values = append(values, item.AsString())
	}
	return values, nil
}

func normalizeAtlasMigrationDir(value string) string {
	if path, found := strings.CutPrefix(value, "file://"); found && path != "" {
		// Preserve URL spelling when later resolution needs query or escape semantics.
		if strings.ContainsAny(path, "?%") {
			return value
		}
		return path
	}
	return value
}

func atlasGetenvFunc() function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{
				Name: "name",
				Type: cty.String,
			},
		},
		Type: function.StaticReturnType(cty.String),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			return cty.StringVal(os.Getenv(args[0].AsString())), nil
		},
	})
}

func atlasFileFunc(fsys fs.FS) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{
				Name: "path",
				Type: cty.String,
			},
		},
		Type: function.StaticReturnType(cty.String),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			path, err := atlasLocalFSPath(args[0].AsString())
			if err != nil {
				return cty.NilVal, err
			}
			raw, err := fs.ReadFile(fsys, path)
			if err != nil {
				return cty.NilVal, err
			}
			return cty.StringVal(string(raw)), nil
		},
	})
}

func atlasFilesetFunc(fsys fs.FS) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{
				Name: "pattern",
				Type: cty.String,
			},
		},
		Type: function.StaticReturnType(cty.List(cty.String)),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			values, err := atlasFileset(fsys, args[0].AsString())
			if err != nil {
				return cty.NilVal, err
			}
			return ctyStringList(values), nil
		},
	})
}

func atlasFileset(fsys fs.FS, pattern string) ([]string, error) {
	if err := validateAtlasLocalPathValue(pattern); err != nil {
		return nil, err
	}
	if strings.Contains(pattern, "**") {
		return atlasRecursiveFileset(fsys, pattern)
	}
	localPattern, err := atlasLocalFSPath(pattern)
	if err != nil {
		return nil, err
	}
	matches, err := fs.Glob(fsys, localPattern)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		info, err := fs.Stat(fsys, match)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			continue
		}
		values = append(values, filepath.ToSlash(match))
	}
	sort.Strings(values)
	return values, nil
}

func atlasRecursiveFileset(fsys fs.FS, pattern string) ([]string, error) {
	localPattern, err := atlasLocalFSPath(pattern)
	if err != nil {
		return nil, err
	}
	values := []string{}
	err = fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		slashRel := filepath.ToSlash(name)
		matched, err := atlasMatchDoubleStar(localPattern, slashRel)
		if err != nil {
			return err
		}
		if matched {
			if _, err := fs.Stat(fsys, name); err != nil {
				return err
			}
			values = append(values, slashRel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(values)
	return values, nil
}

func atlasMatchDoubleStar(pattern, name string) (bool, error) {
	return atlasMatchSegments(strings.Split(pattern, "/"), strings.Split(name, "/"))
}

func atlasMatchSegments(patternParts, nameParts []string) (bool, error) {
	if len(patternParts) == 0 {
		return len(nameParts) == 0, nil
	}
	if patternParts[0] == "**" {
		matched, err := atlasMatchSegments(patternParts[1:], nameParts)
		if matched || err != nil {
			return matched, err
		}
		if len(nameParts) == 0 {
			return false, nil
		}
		return atlasMatchSegments(patternParts, nameParts[1:])
	}
	if len(nameParts) == 0 {
		return false, nil
	}
	matched, err := pathpkg.Match(patternParts[0], nameParts[0])
	if !matched || err != nil {
		return matched, err
	}
	return atlasMatchSegments(patternParts[1:], nameParts[1:])
}

func atlasLocalFSPath(value string) (string, error) {
	if err := validateAtlasLocalPathValue(value); err != nil {
		return "", err
	}
	rawPath := strings.TrimPrefix(value, "file://")
	localPath := filepath.Clean(filepath.FromSlash(rawPath))
	if localPath == ".." || strings.HasPrefix(localPath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes atlas.hcl directory: %s", value)
	}
	return filepath.ToSlash(localPath), nil
}

func validateAtlasLocalPathValue(value string) error {
	switch {
	case filepath.IsAbs(strings.TrimPrefix(value, "file://")):
		return fmt.Errorf("absolute paths are not supported: %s", value)
	case strings.Contains(value, "://") && !strings.HasPrefix(value, "file://"):
		return fmt.Errorf("unsupported URL scheme: %s", value)
	default:
		return nil
	}
}

func atlasLocalFileURL(value string, attr *hclsyntax.Attribute) (string, error) {
	if err := validateAtlasLocalPathValue(value); err != nil {
		return "", unsupportedAttr(attr.Name, attr)
	}
	return "file://" + filepath.ToSlash(strings.TrimPrefix(value, "file://")), nil
}

func ctyStringList(values []string) cty.Value {
	if len(values) == 0 {
		return cty.ListValEmpty(cty.String)
	}
	items := make([]cty.Value, 0, len(values))
	for _, value := range values {
		items = append(items, cty.StringVal(value))
	}
	return cty.ListVal(items)
}

func sortedAttributeNames(attrs hclsyntax.Attributes) []string {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func unsupportedBlock(block *hclsyntax.Block) error {
	return unsupported(block.Type, block.TypeRange)
}

func unsupportedAttr(name string, attr *hclsyntax.Attribute) error {
	return unsupported(name, attr.NameRange)
}

func unsupported(name string, rng hcl.Range) error {
	return fmt.Errorf("unsupported atlas.hcl construct %q at %s:%d", name, rng.Filename, rng.Start.Line)
}
