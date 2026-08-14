package projectconfig

import (
	"slices"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

type atlasBodyStructure struct {
	attributes             []string
	blocks                 map[string]atlasBlockStructure
	allowUnknownAttributes bool
	allowUnknownBlocks     bool
}

type atlasBlockStructure struct {
	body   atlasBodyStructure
	labels int
}

func atlasEnvBodyStructure() atlasBodyStructure {
	return atlasBodyStructure{
		attributes:             []string{"dev", "exclude", "for_each", "name", "schemas", "src", "url"},
		allowUnknownAttributes: true,
		allowUnknownBlocks:     true,
		blocks: map[string]atlasBlockStructure{
			"diff": {
				body: atlasBodyStructure{
					allowUnknownAttributes: true,
					allowUnknownBlocks:     true,
					blocks: map[string]atlasBlockStructure{
						"concurrent_index": {
							body: atlasBodyStructure{
								attributes:             []string{"create", "drop"},
								allowUnknownAttributes: true,
							},
						},
						"skip": {
							body: atlasBodyStructure{
								attributes:             []string{"drop_schema", "drop_table"},
								allowUnknownAttributes: true,
							},
						},
					},
				},
			},
			"format": {
				body: atlasBodyStructure{
					allowUnknownAttributes: true,
					allowUnknownBlocks:     true,
					blocks: map[string]atlasBlockStructure{
						"migrate": {
							body: atlasBodyStructure{attributes: []string{"apply", "diff", "lint", "status"}},
						},
						"schema": {
							body: atlasBodyStructure{attributes: []string{"apply", "clean", "diff", "inspect"}},
						},
					},
				},
			},
			"lint": {
				body: atlasBodyStructure{
					attributes:             []string{"latest", "log"},
					allowUnknownAttributes: true,
					allowUnknownBlocks:     true,
					blocks: map[string]atlasBlockStructure{
						"concurrent_index": {body: atlasTolerantLeafStructure("error")},
						"condrop":          {body: atlasTolerantLeafStructure("error")},
						"data_depend":      {body: atlasTolerantLeafStructure("error")},
						"destructive":      {body: atlasTolerantLeafStructure("error")},
						"git":              {body: atlasTolerantLeafStructure("base", "dir")},
						"incompatible":     {body: atlasTolerantLeafStructure("error")},
						"nestedtx":         {body: atlasTolerantLeafStructure("error")},
					},
				},
			},
			"migration": {
				body: atlasTolerantLeafStructure(
					"dir",
					"exec_order",
					"format",
					"lock_timeout",
					"revisions_schema",
					"tx_mode",
				),
			},
			"schema": {
				body: atlasBodyStructure{
					attributes:             []string{"src"},
					allowUnknownAttributes: true,
					allowUnknownBlocks:     true,
					blocks: map[string]atlasBlockStructure{
						"mode": {
							body: atlasTolerantLeafStructure(
								"funcs",
								"objects",
								"permissions",
								"roles",
								"tables",
								"triggers",
								"types",
								"views",
							),
						},
						"repo": {
							body: atlasTolerantLeafStructure("name"),
						},
					},
				},
			},
		},
	}
}

func atlasTolerantLeafStructure(attributes ...string) atlasBodyStructure {
	return atlasBodyStructure{
		attributes:             attributes,
		allowUnknownAttributes: true,
	}
}

// atlasStructAttributes maps a tolerance-path scope to the names Atlas CE
// decodes into a STRUCT field of its project type at that scope, when the name
// is written as an ATTRIBUTE (`lint = { ... }`) rather than as a block
// (`lint { ... }`).
//
// CE routes both spellings to the same field. The block spelling configures it;
// the attribute spelling reaches CE's object decoder, which refuses EVERY member
// name -- including the ones the block spelling accepts -- so the only values
// the pinned community binary v1.3.0 takes are an empty object and null. The
// attribute spelling therefore carries no configuration at all: there is nothing
// here for Ptah to implement, only a refusal to reproduce.
//
// Measured on the pinned binary with `schema inspect --env local`, exit codes
// read directly from unpiped invocations:
//
//	lint = {}                   -> 0
//	lint = null                 -> 0
//	lint = { k = "v" }          -> 1  converting cty.Value to *cmdapi.Lint:
//	                                  unsupported attribute "k"
//	lint = { latest = 1 }       -> 1  same message, naming "latest" -- a member
//	                                  the BLOCK spelling accepts at exit 0
//	lint = "x" / 1 / true       -> 1  object or tuple value is required
//	lint = []                   -> 1  a tuple of 5 elements is required
//	lint = [1,2,3,4,5]          -> 1  object or tuple value is required
//	lint = [1,"a",true,2,3]     -> 1  mixed list types used in "lint" attribute
//	lint { latest = 1 }         -> 0  the block spelling, unaffected
//
// No tuple value was found that the binary accepts: a homogeneous one converts
// to a list and is refused, and a heterogeneous one is refused by HCL before the
// decoder sees it. Ptah refuses every tuple, which is the safe direction if some
// tuple the three probes above did not reach were to decode.
//
// The membership is measured name by name and is NOT "every known block type" --
// that reading is wrong in nine places. Within one scope the two sets interleave:
// `lint.git` is decoded but `lint.condrop` is not, `diff.skip` is but
// `diff.concurrent_index` is not, `schema.repo` is but `schema.mode` is not.
// Scope matters too: `format`, `migration` and `schema` are decoded under `env`
// and tolerated at the top level. Each name that is NOT listed was probed with
// the same object value and answered exit 0, alongside a `frobnicate9` nonsense
// control that also answered 0 -- which is what keeps those silences meaningful.
//
// The keys are the scope strings the tolerance-path callers already pass:
// atlasTopLevelScope for the project body, "env" for an env body, and the bare
// block names for the container parsers.
//
// The bare keys are deliberate. `diff` and `lint` are the two blocks that may
// sit at the top level as well as inside `env`, and parseDiff/parseLint serve
// both, so "diff" and "lint" cover both spellings -- measured, both refuse:
//
//	diff { skip = { k = "v" } }  (top level)  -> 1  *cmdapi.SkipChanges
//	lint { git  = { k = "v" } }  (top level)  -> 1  set field "git"
//	diff { concurrent_index = { … } }         -> 0  (control, same scope)
//	lint { condrop = { … } }                  -> 0  (control, same scope)
//
// "format" and "env.schema" are NOT the same case, which is why only one of
// them carries the env prefix. A top-level `format` or `schema` block is not
// decoded into those structures by the pinned binary and is not collected by
// collectAtlasTopBlock either, so it never reaches these parsers:
//
//	format { schema = { k = "v" } }  (top level)  -> 0 on both
//	schema { repo   = { k = "v" } }  (top level)  -> 0 on both
//
// Labelling every nested row "top level and env alike" would therefore
// over-claim by two scopes.
var atlasStructAttributes = map[string][]string{
	atlasTopLevelScope: {"diff", "lint", "test"},
	"env":              {"diff", "format", "lint", "migration", "schema", "test"},
	"diff":             {"skip"},
	"format":           {"migrate", "schema"},
	"lint":             {"git"},
	"env.schema":       {"repo"},
}

// atlasTopLevelScope is the scope of the project file's own body. It is not ""
// by accident: the tolerance path never passes an empty scope, so the top level
// cannot collide with a block scope.
const atlasTopLevelScope = "<project>"

// atlasStructAttribute reports whether a name is one CE decodes into a struct at
// the given scope.
func atlasStructAttribute(scope, name string) bool {
	return slices.Contains(atlasStructAttributes[scope], name)
}

// checkAtlasStructAttribute applies the rule on [atlasStructAttributes] to an
// already-evaluated value.
//
// It takes the value rather than the attribute because every caller has already
// evaluated the expression: the evaluation failure has to be reported as such,
// and it has to happen on the tolerance path, which runs per SELECTED env after
// `var`, `local` and `data` are in the context. Checking earlier -- in the
// structure validator -- refused `lint = local.x` in the selected env and
// `lint = missing.value` in an env the command never selects, both of which the
// pinned binary accepts at exit 0.
func checkAtlasStructAttribute(name string, attr *hclsyntax.Attribute, value cty.Value) error {
	if emptyAtlasStructValue(value) {
		return nil
	}
	return wrongValueType(name, attr, "a block, or an empty object")
}

// emptyAtlasStructValue reports whether value is one of the two shapes the
// pinned community binary accepts for a struct-valued name written as an
// attribute: null, or an object with no members.
func emptyAtlasStructValue(value cty.Value) bool {
	if value.IsNull() {
		return true
	}
	typ := value.Type()
	return typ.IsObjectType() && len(typ.AttributeTypes()) == 0
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
	// Nothing here evaluates an expression. This pass runs before the evaluation
	// context carries `var` and `local`, and it runs for EVERY env including the
	// unselected ones, so a value check here would refuse `lint = local.x` in the
	// selected env and `lint = missing.value` in an env the command never uses --
	// both exit 0 on the pinned binary. Value rules belong on the tolerance path,
	// which runs per selected env after the context is built. See
	// [atlasStructAttributeRule].
	for _, name := range sortedAttributeNames(body.Attributes) {
		if !slices.Contains(structure.attributes, name) {
			if !structure.allowUnknownAttributes {
				return unsupportedAttr(name, body.Attributes[name])
			}
			if err := p.recordIgnoredAttr(scope, name, body.Attributes[name]); err != nil {
				return err
			}
			continue
		}
	}

	seen := map[string]struct{}{}
	for _, block := range body.Blocks {
		blockStructure, ok := structure.blocks[block.Type]
		if !ok {
			if !structure.allowUnknownBlocks {
				return unsupportedBlock(block)
			}
			if err := p.recordIgnoredBlock(scope, block); err != nil {
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
