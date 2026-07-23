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
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
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
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Config{}, nil
	case err != nil:
		return Config{}, fmt.Errorf("failed to read atlas config %s: %w", path, err)
	}
	return ParseAtlasWithOptions(raw, path, opts)
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
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return Config{}, fmt.Errorf("parse atlas project config: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return Config{}, fmt.Errorf("parse atlas project config: unsupported body type %T", file.Body)
	}

	baseDir := filepath.Dir(filename)
	p, err := newAtlasParser(baseDir, opts.Vars)
	if err != nil {
		return Config{}, err
	}
	return p.parse(body, opts.EnvName)
}

type atlasParser struct {
	ctx         *hcl.EvalContext
	varOverride map[string]cty.Value
}

func newAtlasParser(baseDir string, rawVars []string) (atlasParser, error) {
	overrides, err := parseAtlasVarOverrides(rawVars)
	if err != nil {
		return atlasParser{}, err
	}
	return atlasParser{
		ctx: &hcl.EvalContext{
			Variables: map[string]cty.Value{},
			Functions: map[string]function.Function{
				"file":       atlasFileFunc(baseDir),
				"fileset":    atlasFilesetFunc(baseDir),
				"format":     stdlib.FormatFunc,
				"getenv":     atlasGetenvFunc(),
				"jsonencode": stdlib.JSONEncodeFunc,
			},
		},
		varOverride: overrides,
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
	return Merge(base, cfg), nil
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
			cfg.presence.schemaSources = true
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
		cfg.presence.databaseURL = true
	case "dev":
		value, err := p.stringAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.DevURL = value
		cfg.presence.devURL = true
	case "src":
		values, err := p.stringOrStringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.SchemaSources = values
		cfg.presence.schemaSources = true
	case "exclude":
		values, err := p.stringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		cfg.Exclude = values
		cfg.presence.exclude = true
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
	if migration.RevisionFormat == "" {
		migration.RevisionFormat = "atlas"
	}

	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "dir":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			dir, err := normalizeAtlasMigrationDir(value, attr)
			if err != nil {
				return err
			}
			migration.Dir = dir
		case "format":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.Format = value
		case "revisions_schema":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.RevisionsSchema = value
		case "lock_timeout":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.LockTimeout = value
		case "exec_order":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.ExecOrder = value
		case "tx_mode":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.TxMode = value
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
		switch attrName {
		case "latest":
			value, err := p.intAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.Lint.Latest = &value
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
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
		case "dir":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.Lint.GitDir = value
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
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		value, err := p.nonEmptyStringAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "apply":
			cfg.Format.Migrate.Apply = value
		case "diff":
			cfg.Format.Migrate.Diff = value
		case "lint":
			cfg.Format.Migrate.Lint = value
		case "status":
			cfg.Format.Migrate.Status = value
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseSchemaFormat(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		value, err := p.nonEmptyStringAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "apply":
			cfg.Format.Schema.Apply = value
		case "clean":
			cfg.Format.Schema.Clean = value
		case "diff":
			cfg.Format.Schema.Diff = value
		case "inspect":
			cfg.Format.Schema.Inspect = value
		default:
			return unsupportedAttr(attrName, attr)
		}
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
		if value, ok := p.varOverride[name]; ok {
			if err := p.validateVariableBlock(block); err != nil {
				return err
			}
			vars[name] = value
			continue
		}
		value, err := p.variableDefault(block)
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

func (p atlasParser) validateVariableBlock(block *hclsyntax.Block) error {
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "default", "description":
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) variableDefault(block *hclsyntax.Block) (cty.Value, error) {
	var value cty.Value
	hasDefault := false
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "default":
			var diags hcl.Diagnostics
			value, diags = attr.Expr.Value(p.ctx)
			if diags.HasErrors() {
				return cty.NilVal, unsupportedAttr(attrName, attr)
			}
			hasDefault = true
		case "description":
		default:
			return cty.NilVal, unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return cty.NilVal, unsupportedBlock(block.Body.Blocks[0])
	}
	if !hasDefault {
		return cty.NilVal, fmt.Errorf("atlas.hcl variable %q requires a default or --var %s=value", block.Labels[0], block.Labels[0])
	}
	return value, nil
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
	for _, block := range blocks {
		if len(block.Labels) != 2 {
			return unsupportedBlock(block)
		}
		if block.Labels[0] != "hcl_schema" {
			return unsupported(block.Type+"."+block.Labels[0], block.TypeRange)
		}
		name := block.Labels[1]
		if _, ok := hclSchemas[name]; ok {
			return fmt.Errorf("duplicate atlas.hcl data.hcl_schema %q at %s:%d", name, block.TypeRange.Filename, block.TypeRange.Start.Line)
		}
		value, err := p.hclSchemaDataSource(block)
		if err != nil {
			return err
		}
		hclSchemas[name] = value
	}
	if len(hclSchemas) > 0 {
		p.ctx.Variables["data"] = cty.ObjectVal(map[string]cty.Value{
			"hcl_schema": cty.ObjectVal(hclSchemas),
		})
	}
	return nil
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

func normalizeAtlasMigrationDir(value string, attr *hclsyntax.Attribute) (string, error) {
	switch {
	case strings.HasPrefix(value, "file://"):
		dir := strings.TrimPrefix(value, "file://")
		if dir == "" {
			return "", unsupportedAttr("dir", attr)
		}
		return dir, nil
	case strings.Contains(value, "://"):
		return "", unsupportedAttr("dir", attr)
	default:
		return value, nil
	}
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

func atlasFileFunc(baseDir string) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{
				Name: "path",
				Type: cty.String,
			},
		},
		Type: function.StaticReturnType(cty.String),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			path, err := atlasLocalPath(baseDir, args[0].AsString())
			if err != nil {
				return cty.NilVal, err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return cty.NilVal, err
			}
			return cty.StringVal(string(raw)), nil
		},
	})
}

func atlasFilesetFunc(baseDir string) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{
				Name: "pattern",
				Type: cty.String,
			},
		},
		Type: function.StaticReturnType(cty.List(cty.String)),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			values, err := atlasFileset(baseDir, args[0].AsString())
			if err != nil {
				return cty.NilVal, err
			}
			return ctyStringList(values), nil
		},
	})
}

func atlasFileset(baseDir, pattern string) ([]string, error) {
	if err := validateAtlasLocalPathValue(pattern); err != nil {
		return nil, err
	}
	if strings.Contains(pattern, "**") {
		return atlasRecursiveFileset(baseDir, pattern)
	}
	fullPattern, err := atlasLocalPath(baseDir, pattern)
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(fullPattern)
	if err != nil {
		return nil, err
	}
	baseAbs, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			continue
		}
		rel, err := filepath.Rel(baseAbs, match)
		if err != nil {
			return nil, err
		}
		values = append(values, filepath.ToSlash(rel))
	}
	sort.Strings(values)
	return values, nil
}

func atlasRecursiveFileset(baseDir, pattern string) ([]string, error) {
	localPattern := strings.TrimPrefix(filepath.ToSlash(pattern), "file://")
	values := []string{}
	err := filepath.WalkDir(baseDir, func(rawPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(baseDir, rawPath)
		if err != nil {
			return err
		}
		slashRel := filepath.ToSlash(rel)
		matched, err := atlasMatchDoubleStar(localPattern, slashRel)
		if err != nil {
			return err
		}
		if matched {
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

func atlasLocalPath(baseDir, value string) (string, error) {
	if err := validateAtlasLocalPathValue(value); err != nil {
		return "", err
	}
	rawPath := strings.TrimPrefix(value, "file://")
	fullPath, err := filepath.Abs(filepath.Join(baseDir, filepath.FromSlash(rawPath)))
	if err != nil {
		return "", err
	}
	baseAbs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(baseAbs, fullPath)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes atlas.hcl directory: %s", value)
	}
	return fullPath, nil
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
