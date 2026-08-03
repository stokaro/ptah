package projectconfig

import (
	"slices"

	"github.com/hashicorp/hcl/v2/hclsyntax"
)

type atlasBodyStructure struct {
	attributes []string
	blocks     map[string]atlasBlockStructure
}

type atlasBlockStructure struct {
	body   atlasBodyStructure
	labels int
}

func atlasEnvBodyStructure() atlasBodyStructure {
	return atlasBodyStructure{
		attributes: []string{"dev", "exclude", "src", "url"},
		blocks: map[string]atlasBlockStructure{
			"diff": {
				body: atlasBodyStructure{blocks: map[string]atlasBlockStructure{
					"concurrent_index": {
						body: atlasBodyStructure{attributes: []string{"create", "drop"}},
					},
					"skip": {
						body: atlasBodyStructure{attributes: []string{"drop_schema", "drop_table"}},
					},
				}},
			},
			"format": {
				body: atlasBodyStructure{blocks: map[string]atlasBlockStructure{
					"migrate": {
						body: atlasBodyStructure{attributes: []string{"apply", "diff", "lint", "status"}},
					},
					"schema": {
						body: atlasBodyStructure{attributes: []string{"apply", "clean", "diff", "inspect"}},
					},
				}},
			},
			"lint": {
				body: atlasBodyStructure{
					attributes: []string{"latest", "log"},
					blocks: map[string]atlasBlockStructure{
						"concurrent_index": {body: atlasBodyStructure{attributes: []string{"error"}}},
						"condrop":          {body: atlasBodyStructure{attributes: []string{"error"}}},
						"data_depend":      {body: atlasBodyStructure{attributes: []string{"error"}}},
						"destructive":      {body: atlasBodyStructure{attributes: []string{"error"}}},
						"git":              {body: atlasBodyStructure{attributes: []string{"base", "dir"}}},
						"incompatible":     {body: atlasBodyStructure{attributes: []string{"error"}}},
						"nestedtx":         {body: atlasBodyStructure{attributes: []string{"error"}}},
					},
				},
			},
			"migration": {
				body: atlasBodyStructure{attributes: []string{
					"dir",
					"exec_order",
					"format",
					"lock_timeout",
					"revisions_schema",
					"tx_mode",
				}},
			},
			"schema": {
				body: atlasBodyStructure{
					attributes: []string{"src"},
					blocks: map[string]atlasBlockStructure{
						"mode": {
							body: atlasBodyStructure{attributes: []string{
								"funcs",
								"objects",
								"permissions",
								"roles",
								"sensitive",
								"tables",
								"triggers",
								"types",
								"views",
							}},
						},
						"repo": {
							body: atlasBodyStructure{attributes: []string{"name"}},
						},
					},
				},
			},
		},
	}
}

func (p atlasParser) validateAtlasEnvStructures(envs []atlasEnvBlock) error {
	structure := atlasEnvBodyStructure()
	for _, env := range envs {
		if err := p.validateAtlasBodyStructure("env", env.block.Body, structure); err != nil {
			return err
		}
	}
	return nil
}

// scope is the dotted path of body within atlas.hcl ("env", "env.schema", ...).
// It is what lets the unknown-name tolerance stay clear of the handful of names
// Atlas CE really does decode -- see ceEnforcedConstructs.
func (p atlasParser) validateAtlasBodyStructure(scope string, body *hclsyntax.Body, structure atlasBodyStructure) error {
	// This validator, not the per-block parsers, is what refuses an unknown
	// name anywhere under `env` -- it runs first and recurses, so relaxing the
	// parsers' own switch defaults would have changed nothing here.
	for _, name := range sortedAttributeNames(body.Attributes) {
		if !slices.Contains(structure.attributes, name) {
			if err := p.tolerateUnknownAttr(scope, name, body.Attributes[name]); err != nil {
				return err
			}
			continue
		}
	}

	seen := map[string]struct{}{}
	for _, block := range body.Blocks {
		blockStructure, ok := structure.blocks[block.Type]
		if !ok {
			if err := p.tolerateUnknownBlock(scope, block); err != nil {
				return err
			}
			continue
		}
		if len(block.Labels) != blockStructure.labels {
			// Label arity is left refusing for now. CE applies the block and
			// ignores the extra labels -- measured -- so this is a known
			// remaining divergence, not the rule above.
			return unsupportedBlock(block)
		}
		if _, duplicate := seen[block.Type]; duplicate {
			return unsupportedBlock(block)
		}
		seen[block.Type] = struct{}{}
		if err := p.validateAtlasBodyStructure(scope+"."+block.Type, block.Body, blockStructure.body); err != nil {
			return err
		}
	}
	return nil
}
