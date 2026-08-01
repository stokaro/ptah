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
						body: atlasBodyStructure{attributes: []string{"drop_table"}},
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
					},
				},
			},
		},
	}
}

func validateAtlasEnvStructures(envs []atlasEnvBlock) error {
	structure := atlasEnvBodyStructure()
	for _, env := range envs {
		if err := validateAtlasBodyStructure(env.block.Body, structure); err != nil {
			return err
		}
	}
	return nil
}

func validateAtlasBodyStructure(body *hclsyntax.Body, structure atlasBodyStructure) error {
	for _, name := range sortedAttributeNames(body.Attributes) {
		if !slices.Contains(structure.attributes, name) {
			return unsupportedAttr(name, body.Attributes[name])
		}
	}

	seen := map[string]struct{}{}
	for _, block := range body.Blocks {
		blockStructure, ok := structure.blocks[block.Type]
		if !ok || len(block.Labels) != blockStructure.labels {
			return unsupportedBlock(block)
		}
		if _, duplicate := seen[block.Type]; duplicate {
			return unsupportedBlock(block)
		}
		seen[block.Type] = struct{}{}
		if err := validateAtlasBodyStructure(block.Body, blockStructure.body); err != nil {
			return err
		}
	}
	return nil
}
