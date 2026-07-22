package projectconfig

import (
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// LoadAtlasFile loads the supported subset of an Atlas project config file. A
// missing file returns an empty config.
func LoadAtlasFile(path, envName string) (Config, error) {
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Config{}, nil
	case err != nil:
		return Config{}, fmt.Errorf("failed to read atlas config %s: %w", path, err)
	}
	return ParseAtlas(raw, path, envName)
}

// ParseAtlas parses the supported subset of an Atlas project config file.
func ParseAtlas(data []byte, filename, envName string) (Config, error) {
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

	p := atlasParser{}
	return p.parse(body, envName)
}

type atlasParser struct{}

func (p atlasParser) parse(body *hclsyntax.Body, envName string) (Config, error) {
	if len(body.Attributes) > 0 {
		for name, attr := range body.Attributes {
			return Config{}, unsupportedAttr(name, attr)
		}
	}

	envs := make([]atlasEnv, 0)
	for _, block := range body.Blocks {
		if block.Type != "env" {
			return Config{}, unsupportedBlock(block)
		}
		env, err := p.parseEnv(block)
		if err != nil {
			return Config{}, err
		}
		envs = append(envs, env)
	}
	if len(envs) == 0 {
		return Config{}, nil
	}

	selected, err := selectAtlasEnv(envs, envName)
	if err != nil {
		return Config{}, err
	}
	return selected.config, nil
}

type atlasEnv struct {
	name   string
	config Config
	rng    hcl.Range
}

func (p atlasParser) parseEnv(block *hclsyntax.Block) (atlasEnv, error) {
	if len(block.Labels) > 1 {
		return atlasEnv{}, unsupportedBlock(block)
	}
	name := ""
	if len(block.Labels) == 1 {
		name = block.Labels[0]
	}

	env := atlasEnv{
		name: name,
		config: Config{
			EnvName: name,
		},
		rng: block.TypeRange,
	}

	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "url":
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return atlasEnv{}, err
			}
			env.config.DatabaseURL = value
		case "dev":
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return atlasEnv{}, err
			}
			env.config.DevURL = value
		case "exclude":
			values, err := stringListAttr(attrName, attr)
			if err != nil {
				return atlasEnv{}, err
			}
			env.config.Exclude = values
			env.config.presence.exclude = true
		default:
			return atlasEnv{}, unsupportedAttr(attrName, attr)
		}
	}

	seenMigration := false
	seenLint := false
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "migration":
			if seenMigration {
				return atlasEnv{}, unsupportedBlock(nested)
			}
			seenMigration = true
			if err := p.parseMigration(nested, &env.config); err != nil {
				return atlasEnv{}, err
			}
		case "lint":
			if seenLint {
				return atlasEnv{}, unsupportedBlock(nested)
			}
			seenLint = true
			if err := p.parseLint(nested, &env.config); err != nil {
				return atlasEnv{}, err
			}
		default:
			return atlasEnv{}, unsupportedBlock(nested)
		}
	}

	return env, nil
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
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			dir, err := normalizeAtlasMigrationDir(value, attr)
			if err != nil {
				return err
			}
			migration.Dir = dir
		case "format":
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.Format = value
		case "revisions_schema":
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.RevisionsSchema = value
		case "lock_timeout":
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.LockTimeout = value
		case "exec_order":
			value, err := stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			migration.ExecOrder = value
		case "tx_mode":
			value, err := stringAttr(attrName, attr)
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
			value, err := intAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.Lint.Latest = &value
		default:
			return unsupportedAttr(attrName, attr)
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func selectAtlasEnv(envs []atlasEnv, envName string) (atlasEnv, error) {
	if envName != "" {
		for _, env := range envs {
			if env.name == envName {
				return env, nil
			}
		}
		return atlasEnv{}, fmt.Errorf("atlas env %q not found", envName)
	}
	if len(envs) == 1 {
		return envs[0], nil
	}
	return atlasEnv{}, fmt.Errorf("atlas.hcl contains multiple env blocks; pass --env")
}

func stringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.String {
		return "", unsupportedAttr(name, attr)
	}
	return value.AsString(), nil
}

func intAttr(name string, attr *hclsyntax.Attribute) (int, error) {
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Number {
		return 0, unsupportedAttr(name, attr)
	}
	raw, accuracy := value.AsBigFloat().Int64()
	if accuracy != big.Exact {
		return 0, unsupportedAttr(name, attr)
	}
	return int(raw), nil
}

func stringListAttr(name string, attr *hclsyntax.Attribute) ([]string, error) {
	value, diags := attr.Expr.Value(nil)
	valueType := value.Type()
	if diags.HasErrors() || !value.CanIterateElements() || (!valueType.IsTupleType() && !valueType.IsListType()) {
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

func unsupportedBlock(block *hclsyntax.Block) error {
	return unsupported(block.Type, block.TypeRange)
}

func unsupportedAttr(name string, attr *hclsyntax.Attribute) error {
	return unsupported(name, attr.NameRange)
}

func unsupported(name string, rng hcl.Range) error {
	return fmt.Errorf("unsupported atlas.hcl construct %q at %s:%d", name, rng.Filename, rng.Start.Line)
}
