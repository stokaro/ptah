package projectconfig

import (
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"

	"ptah.run/internal/hcldiag"
)

// atlasEnvBlockType is the type of an atlas.hcl env block. A parse and
// [DeclaredAtlasEnvs] both recognize the block by it, so the list a caller
// reads without evaluating a file cannot miss an env the parse would select.
const atlasEnvBlockType = "env"

// DeclaredAtlasEnvs lists the env blocks an atlas.hcl source declares, in the
// order the source declares them, without evaluating it: no variable, data
// source, docker block or function is read, so listing a file runs nothing it
// names. The list is the one [Config.DeclaredEnvs] holds after a parse of the
// same source, and an env block with no label is listed with an empty Name.
//
// filename is used in [DeclaredEnv.File] and in diagnostics only; an empty
// filename stands for [AtlasFileName]. A source that is not valid HCL returns
// the parse error, and an env block with more than one label is refused as a
// parse refuses it. A source that declares no env returns nil.
func DeclaredAtlasEnvs(data []byte, filename string) ([]DeclaredEnv, error) {
	if filename == "" {
		filename = AtlasFileName
	}
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse atlas project config: %s", hcldiag.Stable(diags).Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse atlas project config: unsupported body type %T", file.Body)
	}
	var envs []atlasEnvBlock
	for _, block := range body.Blocks {
		if block.Type != atlasEnvBlockType {
			continue
		}
		env, err := atlasEnvBlockFromHCL(block)
		if err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	return declaredAtlasEnvs(envs), nil
}
