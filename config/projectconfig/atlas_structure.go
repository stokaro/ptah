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
						// The two nested bodies tolerate unknown names for the same
						// reason every other body here does: the pinned community
						// binary v1.3.0 decodes only the four template names each
						// one lists and ignores the rest. Measured with
						// `schema inspect --env local`, exit codes read directly
						// from unpiped invocations:
						//
						//	format { migrate { new = "{{ .Name }}" } }  -> 0
						//	format { migrate { hash = { k = "v" } } }   -> 0
						//	format { migrate { frobnicate9 { … } } }    -> 0
						//	format { schema  { fmt = "{{ .Name }}" } }  -> 0
						//	format { migrate { apply = { k = "v" } } }  -> 1  (the control:
						//	                                                 a listed name
						//	                                                 IS decoded)
						//
						// Ptah answered 1 on every tolerated row above, refusing a
						// project file the community binary reads.
						"migrate": {
							body: atlasBodyStructure{
								attributes:             []string{"apply", "diff", "lint", "status"},
								allowUnknownAttributes: true,
								allowUnknownBlocks:     true,
							},
						},
						"schema": {
							body: atlasBodyStructure{
								attributes:             []string{"apply", "clean", "diff", "inspect"},
								allowUnknownAttributes: true,
								allowUnknownBlocks:     true,
							},
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
			// `migration` mirrors `schema` below rather than being a tolerant
			// leaf. It was the leaf spelling that made every nested block under
			// it a refusal, because atlasTolerantLeafStructure leaves
			// allowUnknownBlocks false and carries no blocks map at all.
			// Measured with `schema inspect --env local`:
			//
			//	migration { repo { name = "myrepo" } }   binary 0, Ptah 1
			//	migration { repo { frobnicate9 = "x" } } binary 0, Ptah 1
			//	migration { frobnicate9 { k = "v" } }    binary 0, Ptah 1
			//	migration { repo { name = 1 } }          binary 1  -- `repo` IS
			//	                                         decoded, so the block
			//	                                         needs a parser arm and
			//	                                         not just tolerance
			"migration": {
				body: atlasBodyStructure{
					attributes: []string{
						"dir",
						"exec_order",
						"format",
						"lock_timeout",
						"revisions_schema",
						"tx_mode",
					},
					allowUnknownAttributes: true,
					allowUnknownBlocks:     true,
					blocks: map[string]atlasBlockStructure{
						"repo": {
							body: atlasTolerantLeafStructure("name"),
						},
					},
				},
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
//
// `env.migration` carries `repo` for the same reason `env.schema` does, and it
// was measured the same way. `env.migration` is the scope, not `migration`: a
// TOP-LEVEL `migration` block is not decoded into that structure by the pinned
// binary and is not collected by collectAtlasTopBlock either, so the bare key
// would over-claim exactly as a bare `format` or `schema` key would:
//
//	env { migration { repo = { k = "v" } } }  -> 1  set field "repo": converting
//	                                                cty.Value to *cmdapi.Repo
//	env { migration { repo = {} } }           -> 0
//	env { migration { repo = null } }         -> 0
//	env { migration { frobnicate9 = {…} } }   -> 0  (control, same block)
//	migration { repo = { k = "v" } }  (top level)  -> 0
var atlasStructAttributes = map[string][]string{
	atlasTopLevelScope: {"diff", "lint", "test"},
	"env":              {"diff", "format", "lint", "migration", "schema", "test"},
	"diff":             {"skip"},
	"format":           {"migrate", "schema"},
	"lint":             {"git"},
	"env.migration":    {"repo"},
	"env.schema":       {"repo"},
}

// atlasLeafValueKind is the cty type a name in [atlasDecodedLeafAttributes]
// requires.
type atlasLeafValueKind uint8

const (
	atlasLeafBool atlasLeafValueKind = iota
	atlasLeafString
)

func (k atlasLeafValueKind) ctyType() cty.Type {
	if k == atlasLeafString {
		return cty.String
	}
	return cty.Bool
}

// want is the phrasing [wrongValueType] appends, kept identical to the wording
// the acted-on siblings already produce -- `drop_schema` answers "must be a
// bool" and `log` answers "must be a string", so a tolerated name in the same
// block reads the same way.
func (k atlasLeafValueKind) want() string {
	if k == atlasLeafString {
		return "a string"
	}
	return "a bool"
}

// atlasDecodedLeafAttributes maps a tolerance-path scope to the names Atlas CE
// decodes into a SCALAR field of its project type -- names Ptah does not act
// on, so they reach the tolerance path, but whose value the pinned community
// binary v1.3.0 still type-checks before any command runs. Tolerating the name
// is right; tolerating any value for it is a rule (a) violation, and that is
// what this table closes.
//
// It is the scalar counterpart of [atlasStructAttributes] and is consulted from
// the same place, so a name is answered once for every scope that reaches it
// rather than once per parser arm.
//
// Measured on the pinned binary with `schema inspect --env local`, exit codes
// read directly from unpiped invocations:
//
//	diff { skip { drop_column = true } }        -> 0
//	diff { skip { drop_column = null } }        -> 0
//	diff { skip { drop_column = "true" } }      -> 1  value of attr "drop_column"
//	                                                  cannot be read as bool
//	diff { skip { drop_column = 1 } }           -> 1  same message
//	diff { skip { drop_column = [true] } }      -> 1  same message
//	diff { skip { drop_column = { k = "v" } } } -> 1  same message
//	diff { skip { frobnicate9 = { k = "v" } } } -> 0  (control, same block)
//	lint { review = "ALWAYS" }                  -> 0
//	lint { review = null }                      -> 0
//	lint { review = 1 }                         -> 1  value of attr "review"
//	                                                  cannot be read as string
//	lint { frobnicate9 = { k = "v" } }          -> 0  (control, same block)
//
// null is accepted for both kinds, which is why the check below returns before
// comparing types on a null value.
//
// The `diff.skip` membership is exactly {add,modify,drop} x {schema, table,
// column, index, foreign_key}. That is measured name by name and is NOT "every
// object kind": add/modify/drop probed against view, func, trigger, proc, type,
// sequence, check, comment, role, policy, extension and domain all answer 0 on
// the pinned binary, alongside the frobnicate9 control that also answers 0 --
// which is what keeps those silences meaningful. `drop_schema` and `drop_table`
// are absent here because Ptah acts on them, so they never reach the tolerance
// path; they are the in-block positive control that the probe fires at all.
//
// The keys are bare rather than env-prefixed because parseDiffSkip and
// parseLintAttr each serve a top-level block and an env block, and both
// spellings were measured to refuse:
//
//	diff { skip { drop_column = { k = "v" } } }  (top level)  -> 1
//	lint { review = 1 }                          (top level)  -> 1
var atlasDecodedLeafAttributes = map[string]map[string]atlasLeafValueKind{
	"diff.skip": {
		"add_column":         atlasLeafBool,
		"add_foreign_key":    atlasLeafBool,
		"add_index":          atlasLeafBool,
		"add_schema":         atlasLeafBool,
		"add_table":          atlasLeafBool,
		"drop_column":        atlasLeafBool,
		"drop_foreign_key":   atlasLeafBool,
		"drop_index":         atlasLeafBool,
		"modify_column":      atlasLeafBool,
		"modify_foreign_key": atlasLeafBool,
		"modify_index":       atlasLeafBool,
		"modify_schema":      atlasLeafBool,
		"modify_table":       atlasLeafBool,
	},
	"lint": {
		"review": atlasLeafString,
	},
	// `baseline` is the type half of stokaro/ptah#934 item 5a and nothing more.
	// The pinned binary decodes it as a string, so a malformed value has to be
	// refused here or Ptah exits 0 where that binary exits 1; a well-formed one
	// is still only tolerated, because ACTING on it needs `migrate apply`, which
	// reads --baseline into its run options before project config is merged.
	// Type-checking it now does not stand in the way of that wiring.
	//
	//	migration { baseline = [1,2] }              (env)        -> 1  value of attr
	//	                                                              "baseline" cannot
	//	                                                              be read as string
	//	migration { baseline = "20240101000000" }   (env)        -> 0
	//	migration { baseline = null }               (env)        -> 0
	//	migration { skip_report = [1,2] }           (env)        -> 0  (in-block control)
	//	migration { baseline = [1,2] }              (top level)  -> 0  }
	//	lint       { baseline = [1,2] }                          -> 0  } scope controls
	//	env        { baseline = [1,2] }                          -> 0  }
	//
	// The three scope controls are why the key is `env.migration` and not
	// `migration` or a bare name: `baseline` is a real name in other scopes of
	// this file and the binary decodes none of them.
	"env.migration": {
		"baseline": atlasLeafString,
	},
}

// checkAtlasDecodedLeafAttribute applies the rule on
// [atlasDecodedLeafAttributes] to an already-evaluated value, for the same
// reason [checkAtlasStructAttribute] takes one: the check has to run on the
// tolerance path, per selected env, after `var`, `local` and `data` are in the
// evaluation context.
func checkAtlasDecodedLeafAttribute(
	scope, name string,
	attr *hclsyntax.Attribute,
	value cty.Value,
) error {
	kind, decoded := atlasDecodedLeafAttributes[scope][name]
	if !decoded || value.IsNull() {
		return nil
	}
	if value.Type() == kind.ctyType() {
		return nil
	}
	return wrongValueType(name, attr, kind.want())
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
