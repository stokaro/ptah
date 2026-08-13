package projectconfig

import (
	"slices"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

type atlasBodyStructure struct {
	attributes []string
	// structAttributes are names Atlas CE decodes into a STRUCT field of its
	// project type at this scope. See [atlasStructAttributeRule].
	structAttributes       []string
	blocks                 map[string]atlasBlockStructure
	allowUnknownAttributes bool
	allowUnknownBlocks     bool
}

type atlasBlockStructure struct {
	body   atlasBodyStructure
	labels int
}

// atlasStructAttributeRule is the measured rule for a name Atlas CE decodes
// into a struct field of its project type, when that name is written as an
// ATTRIBUTE (`lint = { ... }`) rather than as a block (`lint { ... }`).
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
// The membership below is measured name by name and is NOT "every known block
// type" -- that reading is wrong in nine places. Within one scope the two sets
// interleave: `lint.git` is decoded but `lint.condrop` is not, `diff.skip` is
// but `diff.concurrent_index` is not, `schema.repo` is but `schema.mode` is not.
// Scope matters too: `format`, `migration` and `schema` are decoded under `env`
// and tolerated at the top level. Each name that is NOT listed was probed with
// the same object value and answered exit 0, alongside a `frobnicate9` nonsense
// control that also answered 0 -- which is what keeps those silences meaningful.
//
// See [atlasTopLevelStructAttributes] for the top-level set.
func atlasEnvBodyStructure() atlasBodyStructure {
	return atlasBodyStructure{
		attributes: []string{"dev", "exclude", "for_each", "name", "schemas", "src", "url"},
		// Tolerated at env scope with the same probe: frobnicate9 (control).
		structAttributes:       []string{"diff", "format", "lint", "migration", "schema", "test"},
		allowUnknownAttributes: true,
		allowUnknownBlocks:     true,
		blocks: map[string]atlasBlockStructure{
			"diff": {
				body: atlasBodyStructure{
					// Tolerated at env.diff scope: concurrent_index.
					structAttributes:       []string{"skip"},
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
					structAttributes:       []string{"migrate", "schema"},
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
					attributes: []string{"latest", "log"},
					// Tolerated at env.lint scope with the same probe:
					// concurrent_index, condrop, data_depend, destructive,
					// incompatible, nestedtx.
					structAttributes:       []string{"git"},
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
					attributes: []string{"src"},
					// Tolerated at env.schema scope with the same probe: mode.
					structAttributes:       []string{"repo"},
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

// atlasTopLevelStructAttributes are the top-level names Atlas CE decodes into a
// struct field, under the rule documented on [atlasStructAttributeRule].
//
// The set is smaller than the `env` one and was measured the same way, with
// `<name> = { k = "v" }` at the top level of the project file:
//
//	diff, lint, test                    -> 1
//	atlas, data, format, locals,        -> 0
//	migration, schema, variable
//	frobnicate9 (control)               -> 0
//
// `format`, `migration` and `schema` are the three names that separate this set
// from the `env` one: decoded there, tolerated here. A scope-blind
// implementation would refuse all three and exit 1 where the binary exits 0.
func atlasTopLevelStructAttributes() []string {
	return []string{"diff", "lint", "test"}
}

// checkAtlasStructAttribute applies the rule on [atlasStructAttributeRule] to
// one attribute: the value must be an empty object or null.
//
// The expression is evaluated first, so an unresolvable reference inside the
// value is reported as an evaluation failure rather than as a shape refusal --
// the same order the tolerance path uses, and the same order CE uses.
func (p atlasParser) checkAtlasStructAttribute(name string, attr *hclsyntax.Attribute) error {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return p.evaluationFailed(name, attr, diags)
	}
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
	for _, name := range sortedAttributeNames(body.Attributes) {
		attr := body.Attributes[name]
		if slices.Contains(structure.attributes, name) {
			continue
		}
		// A name CE decodes into a struct is checked BEFORE the tolerance, not
		// after: the tolerance is what was letting `lint = { k = "v" }` through
		// at exit 0 where the pinned binary exits 1.
		if slices.Contains(structure.structAttributes, name) {
			if err := p.checkAtlasStructAttribute(name, attr); err != nil {
				return err
			}
			if err := p.recordIgnoredAttr(scope, name, attr); err != nil {
				return err
			}
			continue
		}
		if !structure.allowUnknownAttributes {
			return unsupportedAttr(name, attr)
		}
		if err := p.recordIgnoredAttr(scope, name, attr); err != nil {
			return err
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
