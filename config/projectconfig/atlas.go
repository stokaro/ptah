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
	// RejectListMapForEach refuses dynamic env expansion over list and map
	// values. Atlas Community Edition accepts tuple, object and set values but
	// refuses list and map values; compatibility adapters select this option
	// when they need that exact boundary. The default retains Ptah's complete
	// dynamic-env capability.
	RejectListMapForEach bool
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
	configs, err := LoadAtlasFileCollectionWithOptions(path, opts)
	if err != nil {
		return Config{}, err
	}
	return singularAtlasConfig(configs, opts.EnvName)
}

// LoadAtlasFileCollectionWithOptions loads every selected instance from an
// Atlas project config file with Atlas-compatible evaluation options. A
// missing file returns a collection containing one empty config.
func LoadAtlasFileCollectionWithOptions(
	path string,
	opts AtlasLoadOptions,
) ([]Config, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve atlas config path %s: %w", path, err)
	}
	root, err := os.OpenRoot(filepath.Dir(absolute))
	if errors.Is(err, fs.ErrNotExist) {
		return []Config{{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open atlas config directory %s: %w", filepath.Dir(path), err)
	}
	raw, err := fs.ReadFile(root.FS(), filepath.Base(absolute))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return []Config{{}}, root.Close()
	case err != nil:
		return nil, errors.Join(
			fmt.Errorf("failed to read atlas config %s: %w", path, err),
			root.Close(),
		)
	}
	configs, parseErr := ParseAtlasFSCollectionWithOptions(raw, path, root.FS(), opts)
	return configs, errors.Join(parseErr, root.Close())
}

// ParseAtlas parses the supported subset of an Atlas project config file.
func ParseAtlas(data []byte, filename, envName string) (Config, error) {
	return ParseAtlasWithOptions(data, filename, AtlasLoadOptions{EnvName: envName})
}

// ParseAtlasWithOptions parses the supported subset of an Atlas project config
// file with Atlas-compatible evaluation options.
//
// file() and fileset() resolve through a rooted handle on the directory that
// holds filename, so a symbolic link inside that directory cannot be used to
// read a file outside it. os.DirFS is deliberately not used here: it follows
// such a link out of the directory and hands back the target's contents.
func ParseAtlasWithOptions(data []byte, filename string, opts AtlasLoadOptions) (Config, error) {
	configs, err := ParseAtlasCollectionWithOptions(data, filename, opts)
	if err != nil {
		return Config{}, err
	}
	return singularAtlasConfig(configs, opts.EnvName)
}

// ParseAtlasCollectionWithOptions parses every selected instance from an
// Atlas project config file with Atlas-compatible evaluation options.
//
// file() and fileset() use the same rooted filesystem for every instance, so
// expansion does not weaken the project directory confinement.
func ParseAtlasCollectionWithOptions(data []byte, filename string, opts AtlasLoadOptions) ([]Config, error) {
	if filename == "" {
		filename = AtlasFileName
	}
	root, err := os.OpenRoot(filepath.Dir(filename))
	if err != nil {
		// The directory is only reached by file() and fileset(). A config that
		// calls neither parses from an unreadable directory exactly as it did
		// before the sandbox was rooted, so the failure is reported by the
		// first read instead of by the parse.
		return ParseAtlasFSCollectionWithOptions(data, filename, unreadableFS{err: err}, opts)
	}
	configs, parseErr := ParseAtlasFSCollectionWithOptions(data, filename, root.FS(), opts)
	return configs, errors.Join(parseErr, root.Close())
}

// unreadableFS reports the same error from every open. It stands in for a
// directory that could not be opened, so the error surfaces at the read that
// needs it rather than at the parse that may never read anything.
type unreadableFS struct {
	err error
}

func (f unreadableFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: f.err}
}

// ParseAtlasFSWithOptions parses an Atlas project config while resolving
// file() and fileset() through fsys. fsys must be rooted at the directory that
// contains filename.
//
// fsys is the outer boundary of the file() sandbox: pass an escape-resistant
// filesystem such as os.Root.FS(). The sandbox refuses absolute paths, parent
// traversal, and the symbolic-link escapes it can resolve itself, but only the
// filesystem can refuse the rest -- a link chain it cannot follow, or a path
// swapped for a link between the check and the read.
func ParseAtlasFSWithOptions(
	data []byte,
	filename string,
	fsys fs.FS,
	opts AtlasLoadOptions,
) (Config, error) {
	configs, err := ParseAtlasFSCollectionWithOptions(data, filename, fsys, opts)
	if err != nil {
		return Config{}, err
	}
	return singularAtlasConfig(configs, opts.EnvName)
}

// ParseAtlasFSCollectionWithOptions parses every selected Atlas project config
// instance while resolving file() and fileset() through fsys. fsys must be
// rooted at the directory that contains filename.
func ParseAtlasFSCollectionWithOptions(
	data []byte,
	filename string,
	fsys fs.FS,
	opts AtlasLoadOptions,
) ([]Config, error) {
	if filename == "" {
		filename = AtlasFileName
	}
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse atlas project config: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse atlas project config: unsupported body type %T", file.Body)
	}

	p, err := newAtlasParser(fsys, opts.Vars, filename, opts.RejectListMapForEach)
	if err != nil {
		return nil, err
	}
	return p.parseCollection(body, opts.EnvName)
}

func singularAtlasConfig(configs []Config, envName string) (Config, error) {
	switch len(configs) {
	case 1:
		return configs[0], nil
	case 0:
		return Config{}, fmt.Errorf("atlas env %q selected no project config instances", envName)
	default:
		return Config{}, fmt.Errorf(
			"atlas env %q selected %d project config instances; use the corresponding collection-valued API",
			envName,
			len(configs),
		)
	}
}

type atlasParser struct {
	ctx         *hcl.EvalContext
	varOverride map[string]cty.Value
	// baseDir is the directory that contains the parsed atlas.hcl file, as
	// spelled by the caller. Relative data.external_schema working_dir values
	// resolve against it so the configured program runs where the config file
	// lives, matching how other atlas.hcl relative paths behave.
	baseDir string
	// rejectListMapForEach selects the narrower dynamic-env type boundary for
	// consumers that need it without narrowing the parser's default behavior.
	rejectListMapForEach bool
	// externalSchemas holds the declared data.external_schema sources by name.
	externalSchemas map[string]externalSchemaDataSource
	// sensitiveValues holds the resolved values of variables declared
	// `sensitive = true`, so they can be scrubbed from any diagnostic before it
	// reaches stderr. HCL renders function arguments into its own error text --
	// `file(var.secret)` produces `openat <the secret>: no such file` -- so a
	// diagnostic that is merely passed through leaks exactly what `sensitive`
	// exists to protect.
	//
	// It is a pointer for the same reason ctx is: the parser is passed by
	// value, so a plain slice field would be appended to on a copy and the
	// values would never reach the scrubber.
	sensitiveValues *[]string
	// ignored records the constructs tolerated under Atlas CE's
	// unknown-name policy, so the caller can report them. Pointer for the same
	// reason as sensitiveValues: the parser is passed by value.
	ignored     *[]IgnoredAtlasConstruct
	ignoredSeen map[ignoredAtlasConstructKey]struct{}
}

type ignoredAtlasConstructKey struct {
	kind     string
	name     string
	filename string
	line     int
	column   int
}

// IgnoredAtlasConstruct is an atlas.hcl name that was accepted and not acted
// on, matching what Atlas CE does with a name it does not recognize.
//
// Atlas CE reports nothing for these names. Ptah records them so callers can
// make the no-op visible; the Ptah CLIs warn on stderr while preserving the
// command's stdout and exit code.
type IgnoredAtlasConstruct struct {
	// Name is the construct's name as written.
	Name string
	// Kind is "block" or "attribute".
	Kind string
	// Filename and Line locate it. The position matters because the same name
	// is tolerated in one place and fatal in another.
	Filename string
	Line     int
}

func newAtlasParser(
	fsys fs.FS,
	rawVars []string,
	filename string,
	rejectListMapForEach bool,
) (atlasParser, error) {
	overrides, err := parseAtlasVarOverrides(rawVars)
	if err != nil {
		return atlasParser{}, err
	}
	return atlasParser{
		sensitiveValues: &[]string{},
		ignored:         &[]IgnoredAtlasConstruct{},
		ignoredSeen:     map[ignoredAtlasConstructKey]struct{}{},
		ctx: &hcl.EvalContext{
			Variables: map[string]cty.Value{},
			Functions: map[string]function.Function{
				"file":       atlasFileFunc(fsys),
				"fileset":    atlasFilesetFunc(fsys),
				"format":     stdlib.FormatFunc,
				"getenv":     atlasGetenvFunc(),
				"jsonencode": stdlib.JSONEncodeFunc,
				"toset":      stdlib.MakeToFunc(cty.Set(cty.DynamicPseudoType)),
			},
		},
		varOverride:          overrides,
		baseDir:              filepath.Dir(filename),
		rejectListMapForEach: rejectListMapForEach,
		externalSchemas:      map[string]externalSchemaDataSource{},
	}, nil
}

func (p atlasParser) parseCollection(body *hclsyntax.Body, envName string) ([]Config, error) {
	p.ctx.Variables["atlas"] = cty.ObjectVal(map[string]cty.Value{
		"env": cty.StringVal(envName),
	})

	// CE's tolerance covers unknown ATTRIBUTES as well as blocks -- measured,
	// and the point stokaro/ptah#1014 left open. The expression is still
	// evaluated, so a bad reference in one is still fatal.
	//
	// The tolerance is not universal at this scope either: three top-level names
	// are decoded into a struct and refuse an object body. See
	// [atlasStructAttributes].
	for _, name := range sortedAttributeNames(body.Attributes) {
		attr := body.Attributes[name]
		value, diags := attr.Expr.Value(p.ctx)
		if diags.HasErrors() {
			return nil, p.evaluationFailed(name, attr, diags)
		}
		if atlasStructAttribute(atlasTopLevelScope, name) {
			if err := checkAtlasStructAttribute(name, attr, value); err != nil {
				return nil, err
			}
		}
		p.noteIgnored("attribute", name, attr.NameRange)
	}

	base := Config{}
	blocks, err := p.collectAtlasTopBlocks(body.Blocks)
	if err != nil {
		return nil, err
	}
	if err := p.validateAtlasEnvStructures(blocks.envs); err != nil {
		return nil, err
	}

	if err := p.configureEvalContext(blocks.variables, blocks.locals, blocks.data); err != nil {
		return nil, err
	}
	if err := p.parseSingleAtlasBlock(blocks.globalDiff, &base, p.parseDiff); err != nil {
		return nil, err
	}
	if err := p.parseSingleAtlasBlock(blocks.globalLint, &base, p.parseLint); err != nil {
		return nil, err
	}
	if len(blocks.envs) == 0 {
		base.IgnoredConstructs = p.ignoredConstructs()
		return []Config{base}, nil
	}

	selected, err := selectAtlasEnvBlocks(blocks.envs, envName)
	if err != nil {
		return nil, err
	}
	configs := make([]Config, 0, len(selected))
	for _, env := range selected {
		instances, err := p.parseAtlasEnvInstances(env, envName)
		if err != nil {
			return nil, err
		}
		for _, instance := range instances {
			merged := Merge(base, instance)
			if err := p.resolveExternalSchemaMarkers(&merged); err != nil {
				return nil, err
			}
			configs = append(configs, merged)
		}
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("atlas env %q not found", envName)
	}
	ignored := p.ignoredConstructs()
	for i := range configs {
		configs[i].IgnoredConstructs = slices.Clone(ignored)
	}
	return configs, nil
}

type atlasTopBlocks struct {
	data       []*hclsyntax.Block
	globalDiff []*hclsyntax.Block
	globalLint []*hclsyntax.Block
	envs       []atlasEnvBlock
	locals     []*hclsyntax.Block
	variables  []*hclsyntax.Block
}

func (p atlasParser) collectAtlasTopBlocks(blocks []*hclsyntax.Block) (atlasTopBlocks, error) {
	collected := atlasTopBlocks{}
	for _, block := range blocks {
		if err := p.collectAtlasTopBlock(block, &collected); err != nil {
			return atlasTopBlocks{}, err
		}
	}
	return collected, nil
}

// atlasBlockNotSupported reproduces CE's refusal of the `atlas` init block. Its
// two spellings fail differently, and both are measured.
func atlasBlockNotSupported(block *hclsyntax.Block) error {
	if len(block.Labels) > 0 {
		return fmt.Errorf("init block %q cannot have labels", block.Type)
	}
	return fmt.Errorf("atlas block is not supported by the community version of Atlas")
}

// ignoredConstructs returns the tolerated constructs recorded so far.
func (p atlasParser) ignoredConstructs() []IgnoredAtlasConstruct {
	if p.ignored == nil || len(*p.ignored) == 0 {
		return nil
	}
	out := make([]IgnoredAtlasConstruct, len(*p.ignored))
	copy(out, *p.ignored)
	return out
}

// ceEnforcedConstructs lists scope-qualified names that Atlas CE genuinely
// decodes and acts on, but Ptah does not implement.
//
// These must keep being refused rather than swept into the unknown-name
// tolerance. Accepting them would produce the worst outcome available: the user
// writes a policy, nothing enforces it, and nothing says so. A loud refusal is
// a divergence from CE, but it is the safe direction; silent non-enforcement is
// not. Each entry was measured on the pinned CE binary by planting a value the
// target field cannot hold and checking whether CE reports a decode failure --
// a name CE ignores cannot produce one:
//
//	lint { destructive { error = "x" } } -> "parsing destructive check options"
//	lint { condrop { error = "x" } }     -> "parsing datadepend check options"
//	diff { skip { drop_schema = "x" } }  -> attr "drop_schema" cannot be read as bool
//	env { schema { repo { name = 1 } } } -> attr "name" cannot be read as string
//	env { schemas = "one" }              -> field is of type slice but attr
//	                                        "schemas" is type: string
//
// The same probe run against lint.naming, lint.statement, lint.non_linear,
// lint.ownership, lint.check, lint.rule, destructive.allow_table and
// schema.mode.sensitive stays silent, so those are tolerated. At env scope it
// also stays silent for baseline, dir, driver, log, plan, project, registry, to
// and vars, measured with an object value against a nonsense sibling as the
// control -- a name CE decodes at all refuses an object, so silence on the
// control is what keeps silence on the others meaningful.
//
// Note the probe only proves the positive: a decode failure means CE reads the
// field. Silence alone means nothing unless a known-decoded name in the SAME
// command produces a failure -- `migration { baseline = [1,2] }` is silent under
// `migrate diff` purely because that command never reads baseline.
// lint.destructive is decoded by CE too, but Ptah already implements it, so it
// never reaches the tolerance path and is not listed here.
//
// The map is empty. lint.condrop, diff.skip.drop_schema and schema.repo were
// its three entries until stokaro/ptah#1048 gave each a parser arm of its own,
// which puts them in the same position as lint.destructive: decoded, so never
// reaching the tolerance path, so nothing to hold back here. env.schemas was
// the fourth candidate and took the same resolution in stokaro/ptah#934: a
// parser arm rather than a refusal, because the value has a meaning Ptah can
// carry out -- see [parseEnvAttr] and [IgnoreEnvSchemasEnvVar]. The map and
// enforcedByCE stay because the criterion above is the standing rule for the
// next name a probe catches -- an entry here is the holding pen for a construct
// CE acts on that Ptah has not implemented yet, and refusing is where such a
// construct waits.
//
// The class this map used to record and not answer -- a name CE decodes into a
// struct written as an OBJECT-VALUED ATTRIBUTE rather than as a block -- is
// answered in the structure validator's attribute/block split instead, which is
// where it belonged. It is not a holding-pen case: the attribute spelling
// carries no configuration on CE either, so matching it is a refusal and not an
// unimplemented setting. See [atlasStructAttributeRule].
//
// env.migration.baseline was the one name the probe had caught that read as a
// holding-pen case, and it turned out to be neither a holding-pen case nor a
// refusal. `migration { baseline = [1,2] }` under `schema inspect --env local`
// answers `value of attr "baseline" cannot be read as string`, exit 1, on the
// pinned binary -- while skip_report and a frobnicate9 control in the SAME
// block under the SAME command both answer 0, which is what makes baseline's
// refusal meaningful. A map entry here would have been wrong: it refuses the
// name outright, and a well-formed `baseline = "20240101000000"` exits 0 on
// that binary. The answer is [atlasDecodedLeafAttributes], which refuses the
// value the binary refuses and tolerates the value it takes.
//
// That closes the rule (a) exposure and not stokaro/ptah#934 item 5a. Acting on
// a well-formed baseline still needs `migrate apply`, which reads --baseline
// into its run options before project config is merged, so the value is carried
// out nowhere yet and the ignored-name warning says so.
var ceEnforcedConstructs = map[string]struct{}{}

// enforcedByCE reports whether a scope-qualified name is one CE acts on.
//
// The same block may sit at the top level or inside env -- `lint` and `diff`
// are both -- so the env prefix is stripped before the lookup rather than every
// name being registered twice.
func enforcedByCE(scope, name string) bool {
	if _, ok := ceEnforcedConstructs[scope+"."+name]; ok {
		return true
	}
	_, ok := ceEnforcedConstructs[strings.TrimPrefix(scope, "env.")+"."+name]
	return ok
}

// tolerateUnknownAttr accepts an attribute name Atlas CE does not recognize.
//
// The expression is still evaluated and its failure still returned: CE's
// tolerance is name-level, not subtree-level, so `env { frobnicate = var.nope }`
// is fatal on CE even though `env { frobnicate = "x" }` is not.
//
// scope is the dotted path of the enclosing body, used to keep names CE really
// enforces out of the tolerance -- see ceEnforcedConstructs.
func (p atlasParser) tolerateUnknownAttr(scope, name string, attr *hclsyntax.Attribute) error {
	if err := rejectEnforcedConstruct(scope, name, attr.NameRange); err != nil {
		return err
	}
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return p.evaluationFailed(name, attr, diags)
	}
	// A name CE decodes into a struct is not tolerable in every shape. This is
	// the right place for that check rather than the structure validator: it
	// runs once per SELECTED env with `var`, `local` and `data` already in the
	// context. See [atlasStructAttributes].
	if atlasStructAttribute(scope, name) {
		if err := checkAtlasStructAttribute(name, attr, value); err != nil {
			return err
		}
	}
	// The scalar counterpart of the rule above, and the same reasoning about
	// where it runs. See [atlasDecodedLeafAttributes].
	if err := checkAtlasDecodedLeafAttribute(scope, name, attr, value); err != nil {
		return err
	}
	p.noteIgnored("attribute", name, attr.NameRange)
	return nil
}

// tolerateUnknownBlock accepts a block name Atlas CE does not recognize, with
// the same name-level and enforced-name rules as tolerateUnknownAttr.
func (p atlasParser) tolerateUnknownBlock(scope string, block *hclsyntax.Block) error {
	if err := rejectEnforcedConstruct(scope, block.Type, block.TypeRange); err != nil {
		return err
	}
	if err := p.evaluateIgnoredBody(block.Body); err != nil {
		return err
	}
	p.noteIgnored("block", block.Type, block.TypeRange)
	return nil
}

func (p atlasParser) recordIgnoredAttr(scope, name string, attr *hclsyntax.Attribute) error {
	if err := rejectEnforcedConstruct(scope, name, attr.NameRange); err != nil {
		return err
	}
	p.noteIgnored("attribute", name, attr.NameRange)
	return nil
}

func (p atlasParser) recordIgnoredBlock(scope string, block *hclsyntax.Block) error {
	if err := rejectEnforcedConstruct(scope, block.Type, block.TypeRange); err != nil {
		return err
	}
	p.noteIgnored("block", block.Type, block.TypeRange)
	return nil
}

func rejectEnforcedConstruct(scope, name string, rng hcl.Range) error {
	if enforcedByCE(scope, name) {
		return unsupported(name, rng)
	}
	return nil
}

// noteIgnored records a construct tolerated under the unknown-name policy.
func (p atlasParser) noteIgnored(kind, name string, rng hcl.Range) {
	if p.ignored == nil {
		return
	}
	key := ignoredAtlasConstructKey{
		kind: kind, name: name, filename: rng.Filename,
		line: rng.Start.Line, column: rng.Start.Column,
	}
	if p.ignoredSeen != nil {
		if _, ok := p.ignoredSeen[key]; ok {
			return
		}
		p.ignoredSeen[key] = struct{}{}
	}
	*p.ignored = append(*p.ignored, IgnoredAtlasConstruct{
		Name: name, Kind: kind, Filename: rng.Filename, Line: rng.Start.Line,
	})
}

// evaluateIgnoredBody evaluates every expression inside a construct whose NAME
// is being ignored, discarding the values and returning the first failure.
//
// This is what makes the tolerance name-level rather than subtree-level, which
// is how Atlas CE behaves: it drops the decoded result of an unrecognized name
// but still evaluates the body, so an unresolvable reference inside an ignored
// block is fatal. Measured on the pinned CE binary:
//
//	frobnicate { v = "literal" }         -> exit 0, 0 bytes on stderr
//	frobnicate { v = var.undefined_ref } -> exit 1, "Unsupported attribute"
//
// Skipping the subtree instead would accept the second file, which CE rejects,
// making this implementation LOOSER than CE -- the more dangerous direction.
func (p atlasParser) evaluateIgnoredBody(body *hclsyntax.Body) error {
	if body == nil {
		return nil
	}
	for _, name := range sortedAttributeNames(body.Attributes) {
		attr := body.Attributes[name]
		if _, diags := attr.Expr.Value(p.ctx); diags.HasErrors() {
			return p.evaluationFailed(name, attr, diags)
		}
	}
	for _, block := range body.Blocks {
		if err := p.evaluateIgnoredBody(block.Body); err != nil {
			return err
		}
	}
	return nil
}

func (p atlasParser) collectAtlasTopBlock(block *hclsyntax.Block, collected *atlasTopBlocks) error {
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
	case "atlas":
		// The one top-level name CE KNOWS and refuses, rather than not
		// recognizing. Measured across nine candidate names on the pinned CE
		// binary -- `cloud`, `docker`, `remote_dir`, `project`, `check`,
		// `exporter`, `script` and `test` are all tolerated; only `atlas` is
		// gated. Folding it into the unknown-name path would accept a config
		// CE rejects, making this surface LOOSER than CE.
		return atlasBlockNotSupported(block)
	default:
		// Atlas CE accepts an unrecognized top-level name and drops it. The
		// body is still evaluated -- see evaluateIgnoredBody.
		if err := p.evaluateIgnoredBody(block.Body); err != nil {
			return err
		}
		p.noteIgnored("block", block.Type, block.TypeRange)
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

func (e atlasEnvBlock) labeled() bool {
	return len(e.block.Labels) == 1
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

func (p atlasParser) parseAtlasEnvInstances(env atlasEnvBlock, selectedName string) ([]Config, error) {
	nameAttr := env.block.Body.Attributes["name"]
	forEachAttr, ok := env.block.Body.Attributes["for_each"]
	if !ok {
		instance, selected, err := p.parseAtlasEnvInstance(env, nameAttr, selectedName)
		if err != nil {
			return nil, err
		}
		if !selected {
			return nil, nil
		}
		return []Config{instance}, nil
	}
	forEach, diags := forEachAttr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, p.evaluationFailed("for_each", forEachAttr, diags)
	}
	if forEach.IsNull() {
		return nil, fmt.Errorf("schemahcl: for_each cannot be null")
	}
	if !forEach.IsWhollyKnown() {
		return nil, fmt.Errorf("schemahcl: for_each must be wholly known")
	}
	forEachType := forEach.Type()
	if p.rejectListMapForEach && (forEachType.IsListType() || forEachType.IsMapType()) {
		return nil, fmt.Errorf("schemahcl: for_each does not support %s type", forEachType.FriendlyName())
	}
	if !forEachType.IsListType() &&
		!forEachType.IsTupleType() &&
		!forEachType.IsMapType() &&
		!forEachType.IsObjectType() &&
		!forEachType.IsSetType() {
		return nil, fmt.Errorf("schemahcl: for_each does not support %s type", forEachType.FriendlyName())
	}

	configs := make([]Config, 0, forEach.LengthInt())
	// cty preserves list/tuple order and gives map/object/set iterators a stable
	// key order, so one iterator defines the public expansion order.
	iterator := forEach.ElementIterator()
	instanceNumber := 0
	for iterator.Next() {
		instanceNumber++
		key, value := iterator.Element()
		child := p.ctx.NewChild()
		child.Variables = map[string]cty.Value{
			"each": cty.ObjectVal(map[string]cty.Value{
				"key":   key,
				"value": value,
			}),
		}
		instanceParser := p
		instanceParser.ctx = child
		instance, selected, err := instanceParser.parseAtlasEnvInstance(env, nameAttr, selectedName)
		if err != nil {
			// Values commonly contain database URLs and may originate in a
			// sensitive variable. The ordinal identifies the failing expansion
			// without publishing credentials through the error path.
			return nil, fmt.Errorf("schemahcl: evaluate env block instance %d: %w", instanceNumber, err)
		}
		if selected {
			configs = append(configs, instance)
		}
	}
	return configs, nil
}

func (p atlasParser) parseAtlasEnvInstance(
	env atlasEnvBlock,
	nameAttr *hclsyntax.Attribute,
	selectedName string,
) (Config, bool, error) {
	name := env.name
	if nameAttr != nil {
		var err error
		name, err = p.nonEmptyStringAttr("name", nameAttr)
		if err != nil {
			return Config{}, false, err
		}
	}
	cfg, err := p.parseEnv(env, name)
	if err != nil {
		return Config{}, false, err
	}
	if selectedName != "" && name != selectedName {
		return Config{}, false, nil
	}
	return cfg, true, nil
}

func (p atlasParser) parseEnv(env atlasEnvBlock, name string) (Config, error) {
	cfg := Config{
		EnvName: name,
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
		return p.tolerateUnknownBlock("env", block)
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
			if err := p.tolerateUnknownAttr("env.schema", attrName, attr); err != nil {
				return err
			}
		}
	}
	seen := map[string]struct{}{}
	for _, nested := range block.Body.Blocks {
		switch nested.Type {
		case "mode":
			if _, duplicate := seen[nested.Type]; duplicate {
				return unsupportedBlock(nested)
			}
			seen[nested.Type] = struct{}{}
			if err := p.parseSchemaMode(nested, cfg); err != nil {
				return err
			}
		case "repo":
			if _, duplicate := seen[nested.Type]; duplicate {
				return unsupportedBlock(nested)
			}
			seen[nested.Type] = struct{}{}
			if err := p.parseSchemaRepo(nested, cfg); err != nil {
				return err
			}
		default:
			// Same in-loop rule as the attribute switches: returning here
			// would end the loop and skip every later block.
			if err := p.tolerateUnknownBlock("env.schema", nested); err != nil {
				return err
			}
		}
	}
	return nil
}

// parseSchemaRepo decodes env.schema.repo, whose only decoded attribute is a
// string `name`. See SchemaRepoConfig for what the pinned community binary was
// measured to do with the value: type-check it, and nothing else.
func (p atlasParser) parseSchemaRepo(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
		switch attrName {
		case "name":
			value, err := p.stringAttr(attrName, attr)
			if err != nil {
				return err
			}
			cfg.Schema.Repo.Name = value
			cfg.presence.mark(fieldSchemaRepoName)
		default:
			if err := p.tolerateUnknownAttr("env.schema.repo", attrName, attr); err != nil {
				return err
			}
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseSchemaMode(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "funcs", "objects", "permissions", "roles", "sensitive", "tables", "triggers", "types", "views":
		default:
			if err := p.tolerateUnknownAttr("env.schema.mode", attrName, attr); err != nil {
				return err
			}
			continue
		}
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
				if err := p.tolerateUnknownAttr("env.schema.mode", attrName, attr); err != nil {
					return err
				}
			}
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseEnvAttr(attrName string, attr *hclsyntax.Attribute, cfg *Config) error {
	switch attrName {
	case "for_each", "name":
		// Meta attributes are evaluated before the instance body.
		return nil
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
	case "schemas":
		// The type check runs before the opt-out is consulted, and returns
		// before it: the pinned binary refuses `schemas = "one"` with
		// `field is of type slice but attr "schemas" is type: string`, and a
		// Ptah environment variable may not reopen an exit-0 where the binary
		// exits 1. See [IgnoreEnvSchemasEnvVar].
		values, err := p.stringListAttr(attrName, attr)
		if err != nil {
			return err
		}
		ignore, err := ignoreEnvSchemas.Resolve()
		if err != nil {
			return err
		}
		if ignore {
			p.noteIgnored("attribute", attrName, attr.NameRange)
			return nil
		}
		// An empty list is not a selection. Measured on the pinned binary:
		// `schemas = []` describes the same three schemas an absent attribute
		// describes, so the presence mark must not turn it into a restriction
		// naming nothing.
		cfg.Schemas = values
		cfg.presence.mark(fieldSchemas)
	default:
		if err := p.tolerateUnknownAttr("env", attrName, attr); err != nil {
			return err
		}
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
			if err := p.tolerateUnknownAttr("env.migration", attrName, attr); err != nil {
				return err
			}
		}
	}
	if err := p.parseMigrationBlocks(block); err != nil {
		return err
	}
	cfg.Migration = migration
	return nil
}

// parseMigrationBlocks handles the nested blocks of env.migration, which used
// to be refused wholesale. The pinned community binary v1.3.0 accepts every one
// of them -- see the `migration` entry in [atlasEnvBodyStructure] for the
// measurement -- so the whole set is tolerated and only `repo` is decoded.
func (p atlasParser) parseMigrationBlocks(block *hclsyntax.Block) error {
	seen := map[string]struct{}{}
	for _, nested := range block.Body.Blocks {
		if nested.Type != "repo" {
			// Not a `return` on the tolerated path: that would end the loop and
			// silently skip every later block.
			if err := p.tolerateUnknownBlock("env.migration", nested); err != nil {
				return err
			}
			continue
		}
		if _, duplicate := seen[nested.Type]; duplicate {
			return unsupportedBlock(nested)
		}
		seen[nested.Type] = struct{}{}
		if err := p.parseMigrationRepo(nested); err != nil {
			return err
		}
	}
	return nil
}

// parseMigrationRepo type-checks env.migration.repo, whose only decoded
// attribute is a string `name`, and records the block as carrying no effect.
//
// It mirrors [atlasParser.parseSchemaRepo] deliberately, down to refusing a
// nested block and a null `name`: the two blocks are the same construct in two
// scopes, and the pinned community binary treats them the same way. Nothing is
// stored, because unlike env.schema.repo there is no field for it and the
// community binary does nothing with the value either -- recording the name as
// ignored tells the user that plainly, where a silently kept value would not.
func (p atlasParser) parseMigrationRepo(block *hclsyntax.Block) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
		if attrName != "name" {
			if err := p.tolerateUnknownAttr("env.migration.repo", attrName, attr); err != nil {
				return err
			}
			continue
		}
		if _, err := p.stringAttr(attrName, attr); err != nil {
			return err
		}
		p.noteIgnored("attribute", attrName, attr.NameRange)
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

func (p atlasParser) parseLint(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
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
		if err := p.tolerateUnknownAttr("lint", attrName, attr); err != nil {
			return err
		}
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
		case "condrop":
			// Atlas's condrop analyzer owns the constraint-deletion family, and
			// it is a distinct analyzer from data_depend despite CE's decode
			// error naming datadepend's option struct. Measured on the pinned
			// community binary against a migration that drops a foreign key:
			//
			//	no lint block          -> exit 0, "1 version with warnings"
			//	condrop     error=true -> exit 1, "1 version with errors"
			//	destructive error=true -> exit 0, "1 version with warnings"
			//
			// so condrop, not destructive, escalates the diagnostic CE reports
			// as CD101. Ptah's CD family (CD101 foreign key, CD102 check, CD103
			// primary key) is the same family. DS105 rides with it because it is
			// Ptah's untyped fallback for the ANSI `DROP CONSTRAINT <name>` form
			// whose type the SQL does not reveal -- and that is precisely the
			// statement CE attributed to CD101 in the measurement above, so
			// leaving it out would let `condrop { error = false }` be accepted
			// and change nothing for the most common spelling.
			if err := p.parseSingleLintBlock(nested, seen, func() error {
				return p.parseLintAnalyzer(nested, cfg, "CD", "DS105")
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
			// Same in-loop rule as the attribute switches: returning here
			// would end the loop and skip every later block.
			if err := p.tolerateUnknownBlock("lint", nested); err != nil {
				return err
			}
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
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
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
			if err := p.tolerateUnknownAttr("lint.analyzer", attrName, attr); err != nil {
				return err
			}
		default:
			if err := p.tolerateUnknownAttr("lint.analyzer", attrName, attr); err != nil {
				return err
			}
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
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
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
			if err := p.tolerateUnknownAttr("lint.git", attrName, attr); err != nil {
				return err
			}
		}
	}
	if len(block.Body.Blocks) > 0 {
		return unsupportedBlock(block.Body.Blocks[0])
	}
	return nil
}

// atlasNestedParser parses one known child of a container block.
type atlasNestedParser func(*hclsyntax.Block, *Config) error

// parseContainerBlock parses an unlabeled block that carries no attributes of
// its own and a fixed set of nested blocks, each allowed once.
//
// Attributes are all unknown by definition and go to the tolerance path;
// unknown nested blocks do too. Neither `return`s from inside the loop on the
// tolerated path -- that would end the loop and silently skip every later
// name, which map iteration order would make intermittent.
func (p atlasParser) parseContainerBlock(
	scope string,
	block *hclsyntax.Block,
	cfg *Config,
	nested map[string]atlasNestedParser,
) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for _, name := range sortedAttributeNames(block.Body.Attributes) {
		if err := p.tolerateUnknownAttr(scope, name, block.Body.Attributes[name]); err != nil {
			return err
		}
	}
	seen := map[string]struct{}{}
	for _, child := range block.Body.Blocks {
		parse, known := nested[child.Type]
		if !known {
			if err := p.tolerateUnknownBlock(scope, child); err != nil {
				return err
			}
			continue
		}
		if _, duplicate := seen[child.Type]; duplicate {
			return unsupportedBlock(child)
		}
		seen[child.Type] = struct{}{}
		if err := parse(child, cfg); err != nil {
			return err
		}
	}
	return nil
}

func (p atlasParser) parseFormat(block *hclsyntax.Block, cfg *Config) error {
	return p.parseContainerBlock("format", block, cfg, map[string]atlasNestedParser{
		"migrate": p.parseMigrateFormat,
		"schema":  p.parseSchemaFormat,
	})
}

func (p atlasParser) parseMigrateFormat(block *hclsyntax.Block, cfg *Config) error {
	return p.parseFormatAttributes(block, "format.migrate", &cfg.presence, map[string]atlasFormatField{
		"apply":  {destination: &cfg.Format.Migrate.Apply, presence: fieldFormatMigrateApply},
		"diff":   {destination: &cfg.Format.Migrate.Diff, presence: fieldFormatMigrateDiff},
		"lint":   {destination: &cfg.Format.Migrate.Lint, presence: fieldFormatMigrateLint},
		"status": {destination: &cfg.Format.Migrate.Status, presence: fieldFormatMigrateStatus},
	})
}

func (p atlasParser) parseSchemaFormat(block *hclsyntax.Block, cfg *Config) error {
	return p.parseFormatAttributes(block, "format.schema", &cfg.presence, map[string]atlasFormatField{
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

// parseFormatAttributes decodes the four template names of one format.* block.
//
// Every other name goes to the tolerance path rather than being refused. The
// pinned community binary v1.3.0 decodes only the four each block lists and
// ignores the rest, including nested blocks -- measured in the `format` entry
// of [atlasEnvBodyStructure] -- so refusing them turned a project file that
// binary reads into an exit 1.
func (p atlasParser) parseFormatAttributes(
	block *hclsyntax.Block,
	scope string,
	presence *configPresence,
	fields map[string]atlasFormatField,
) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for _, attrName := range sortedAttributeNames(block.Body.Attributes) {
		attr := block.Body.Attributes[attrName]
		field, ok := fields[attrName]
		if !ok {
			// Not a `return` on the tolerated path: that would end the loop and
			// silently skip every later name, which map iteration order would
			// make intermittent.
			if err := p.tolerateUnknownAttr(scope, attrName, attr); err != nil {
				return err
			}
			continue
		}
		value, err := p.nonEmptyStringAttr(attrName, attr)
		if err != nil {
			return err
		}
		*field.destination = value
		presence.mark(field.presence)
	}
	for _, nested := range block.Body.Blocks {
		if err := p.tolerateUnknownBlock(scope, nested); err != nil {
			return err
		}
	}
	return nil
}

func (p atlasParser) parseDiff(block *hclsyntax.Block, cfg *Config) error {
	return p.parseContainerBlock("diff", block, cfg, map[string]atlasNestedParser{
		"skip":             p.parseDiffSkip,
		"concurrent_index": p.parseDiffConcurrentIndex,
	})
}

func (p atlasParser) parseDiffSkip(block *hclsyntax.Block, cfg *Config) error {
	if len(block.Labels) > 0 {
		return unsupportedBlock(block)
	}
	for attrName, attr := range block.Body.Attributes {
		switch attrName {
		case "drop_table", "drop_schema":
		default:
			if err := p.tolerateUnknownAttr("diff.skip", attrName, attr); err != nil {
				return err
			}
			continue
		}
		value, err := p.configBoolAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "drop_table":
			cfg.Diff.Skip.DropTable = value
		case "drop_schema":
			cfg.Diff.Skip.DropSchema = value
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
		switch attrName {
		case "create", "drop":
		default:
			if err := p.tolerateUnknownAttr("diff.concurrent_index", attrName, attr); err != nil {
				return err
			}
			continue
		}
		value, err := p.configBoolAttr(attrName, attr)
		if err != nil {
			return err
		}
		switch attrName {
		case "create":
			cfg.Diff.ConcurrentIndex.Create = value
		case "drop":
			cfg.Diff.ConcurrentIndex.Drop = value
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
		if variable.sensitive && p.sensitiveValues != nil {
			appendSensitiveStrings(p.sensitiveValues, value)
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

func appendSensitiveStrings(target *[]string, value cty.Value) {
	if value.IsNull() || !value.IsWhollyKnown() {
		return
	}
	if value.Type() == cty.String {
		if raw := value.AsString(); raw != "" {
			*target = append(*target, raw)
		}
		return
	}
	if !value.CanIterateElements() {
		return
	}
	iterator := value.ElementIterator()
	for iterator.Next() {
		key, element := iterator.Element()
		appendSensitiveStrings(target, key)
		appendSensitiveStrings(target, element)
	}
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
const atlasSupportedVariableTypes = "string, number, bool, list(string), and map(string)"

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
// Other constraints (object, tuple, set, ...) report not-ok so the caller
// rejects them with an error naming the supported set.
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
		if len(expr.Args) == 1 {
			if arg, ok := expr.Args[0].(*hclsyntax.ScopeTraversalExpr); ok && atlasTypeKeyword(arg) == "string" {
				switch expr.Name {
				case "list":
					return cty.List(cty.String), true
				case "map":
					return cty.Map(cty.String), true
				}
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
	case typ.IsMapType():
		return "map(string)"
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
		url, err := p.atlasLocalFileURL(value, pathAttr)
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
			url, err := p.atlasLocalFileURL(value, pathsAttr)
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

func selectAtlasEnvBlocks(envs []atlasEnvBlock, envName string) ([]atlasEnvBlock, error) {
	if envName != "" {
		labeled := make([]atlasEnvBlock, 0, 1)
		for _, env := range envs {
			if env.labeled() && env.name == envName {
				labeled = append(labeled, env)
			}
		}
		if len(labeled) > 0 {
			return labeled, nil
		}

		dynamic := make([]atlasEnvBlock, 0, 1)
		for _, env := range envs {
			if !env.labeled() && atlasEnvNameUsesSelection(env) {
				dynamic = append(dynamic, env)
			}
		}
		if len(dynamic) > 0 {
			return dynamic, nil
		}
		return nil, fmt.Errorf("atlas env %q not found", envName)
	}
	if len(envs) == 1 {
		return envs, nil
	}
	return nil, fmt.Errorf("atlas.hcl contains multiple env blocks; pass --env")
}

func atlasEnvNameUsesSelection(env atlasEnvBlock) bool {
	name, ok := env.block.Body.Attributes["name"]
	if !ok {
		return false
	}
	for _, traversal := range name.Expr.Variables() {
		if len(traversal) < 2 || traversal.RootName() != "atlas" {
			continue
		}
		attribute, ok := traversal[1].(hcl.TraverseAttr)
		if ok && attribute.Name == "env" {
			return true
		}
	}
	return false
}

func (p atlasParser) stringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return "", p.evaluationFailed(name, attr, diags)
	}
	if value.Type() != cty.String {
		return "", wrongValueType(name, attr, "a string")
	}
	return value.AsString(), nil
}

func (p atlasParser) nonEmptyStringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, err := p.stringAttr(name, attr)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(value) == "" {
		return "", emptyValue(name, attr)
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
		return false, wrongValueType(name, attr, "one of DENY, ALLOW")
	}
}

func (p atlasParser) identifierOrStringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if !diags.HasErrors() && value.Type() == cty.String {
		return value.AsString(), nil
	}
	traversal, ok := attr.Expr.(*hclsyntax.ScopeTraversalExpr)
	if !ok || len(traversal.Traversal) != 1 {
		return "", wrongValueType(name, attr, "a string or a bare identifier")
	}
	root, ok := traversal.Traversal[0].(hcl.TraverseRoot)
	if !ok {
		return "", wrongValueType(name, attr, "a string or a bare identifier")
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
		return "", wrongValueType(name, attr, "one of "+strings.Join(allowed, ", "))
	}
	root, ok := traversal.Traversal[0].(hcl.TraverseRoot)
	if !ok || !slices.Contains(allowed, root.Name) {
		return "", wrongValueType(name, attr, "one of "+strings.Join(allowed, ", "))
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
	if diags.HasErrors() {
		return false, p.evaluationFailed(name, attr, diags)
	}
	if value.Type() != cty.Bool {
		return false, wrongValueType(name, attr, "a bool")
	}
	return value.True(), nil
}

func (p atlasParser) stringOrStringListAttr(name string, attr *hclsyntax.Attribute) ([]string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, p.evaluationFailed(name, attr, diags)
	}
	if value.Type() == cty.String {
		return []string{value.AsString()}, nil
	}
	return stringListValue(name, attr, value)
}

func (p atlasParser) intAttr(name string, attr *hclsyntax.Attribute) (int, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return 0, p.evaluationFailed(name, attr, diags)
	}
	if value.Type() != cty.Number {
		return 0, wrongValueType(name, attr, "a number")
	}
	raw, accuracy := value.AsBigFloat().Int64()
	if accuracy != big.Exact {
		return 0, wrongValueType(name, attr, "a whole number")
	}
	return int(raw), nil
}

func (p atlasParser) stringListAttr(name string, attr *hclsyntax.Attribute) ([]string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, p.evaluationFailed(name, attr, diags)
	}
	return stringListValue(name, attr, value)
}

func stringListValue(name string, attr *hclsyntax.Attribute, value cty.Value) ([]string, error) {
	valueType := value.Type()
	if !value.CanIterateElements() || (!valueType.IsTupleType() && !valueType.IsListType()) {
		return nil, wrongValueType(name, attr, "a list of strings")
	}
	values := make([]string, 0, value.LengthInt())
	it := value.ElementIterator()
	for it.Next() {
		_, item := it.Element()
		if item.Type() != cty.String {
			return nil, wrongValueType(name, attr, "a list of strings")
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
			value := args[0].AsString()
			path, err := atlasSandboxedFSPath(fsys, value)
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
		if err := atlasCheckSandboxedFSPath(fsys, match); err != nil {
			return nil, err
		}
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
			if err := atlasCheckSandboxedFSPath(fsys, name); err != nil {
				return err
			}
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

// atlasFileSandboxHint names what to do instead, so a refusal is a redirection
// rather than a dead end. A value that genuinely lives outside the config
// directory -- a mounted secret, a shared CA bundle -- reaches atlas.hcl
// through the environment, which is what getenv() is for.
const atlasFileSandboxHint = "atlas.hcl file() and fileset() read only inside the directory holding atlas.hcl; " +
	"pass a value from outside it through getenv()"

// atlasDataSourcePathHint is the same redirection for data.hcl_schema paths,
// which need their own wording because their rule is not the file() sandbox.
// A data source path is only required to be relative -- "../sibling.hcl" is
// accepted here and refused by file() -- so promising a directory sandbox would
// be false, and pointing at getenv() would be useless: the value it returns is
// used as a path and would be just as absolute.
const atlasDataSourcePathHint = "give a path relative to the directory holding atlas.hcl"

// atlasSymlinkHopLimit is where this walk stops resolving and leaves the rest
// to the filesystem. A config argument that needs more than a few chained links
// to resolve is not something an author writes, and following further buys
// nothing: a rooted filesystem still refuses whatever leaves it, and a caller
// that supplied a filesystem without that protection has already chosen who
// enforces the boundary.
const atlasSymlinkHopLimit = 4

// atlasSandboxedFSPath resolves an atlas.hcl file()/fileset() argument to a
// path inside fsys, refusing every shape that reads outside the directory that
// holds atlas.hcl: an absolute path, parent traversal, and a symbolic link
// whose target leaves the directory.
func atlasSandboxedFSPath(fsys fs.FS, value string) (string, error) {
	path, err := atlasLocalFSPath(value)
	if err != nil {
		return "", err
	}
	if err := atlasCheckSandboxedFSPath(fsys, path); err != nil {
		return "", err
	}
	return path, nil
}

// atlasCheckSandboxedFSPath refuses name when a symbolic link on the way to it
// points out of fsys.
//
// A rooted filesystem (os.Root.FS()) refuses these reads on its own, and it,
// not this walk, is what makes the refusal reliable: it resolves each component
// in the kernel, so nothing can be swapped for a link between this check and
// the read. This walk exists for two other reasons -- it names the offending
// link and the sandbox rule in the error instead of leaving an "openat: path
// escapes from parent" for the user to interpret, and it holds the line for a
// caller that passes ParseAtlasFSWithOptions a filesystem with no such
// protection. Whatever it cannot resolve it leaves alone: the read that follows
// reports the real error.
func atlasCheckSandboxedFSPath(fsys fs.FS, name string) error {
	links, ok := fsys.(fs.ReadLinkFS)
	if !ok {
		return nil
	}
	pending := atlasPathComponents(name)
	resolved := "."
	via := ""
	for hops := 0; len(pending) > 0; {
		next := pathpkg.Join(resolved, pending[0])
		if next == ".." || strings.HasPrefix(next, "../") {
			return atlasSymlinkEscapeError(name, via)
		}
		info, err := links.Lstat(next)
		if err != nil {
			return nil
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			resolved, pending = next, pending[1:]
			continue
		}
		hops++
		if hops > atlasSymlinkHopLimit {
			return nil
		}
		target, err := links.ReadLink(next)
		if err != nil {
			return nil
		}
		if atlasTargetIsAbsolute(target) {
			return atlasSymlinkEscapeError(name, next)
		}
		resolved, via = pathpkg.Dir(next), next
		pending = append(atlasPathComponents(target), pending[1:]...)
	}
	return nil
}

// atlasPathComponents splits a slash path into the components a resolution walk
// consumes one at a time. Targets read back from a link may be spelled with
// either separator on Windows, so both are honored.
func atlasPathComponents(name string) []string {
	parts := strings.FieldsFunc(filepath.ToSlash(name), func(r rune) bool { return r == '/' })
	components := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "." {
			continue
		}
		components = append(components, part)
	}
	return components
}

// atlasTargetIsAbsolute reports whether a link target names a location from the
// filesystem root rather than from the link's own directory. A rooted
// filesystem refuses an absolute target outright -- even one that points back
// inside the root -- so this agrees with it rather than second-guessing it.
func atlasTargetIsAbsolute(target string) bool {
	return filepath.IsAbs(target) || strings.HasPrefix(filepath.ToSlash(target), "/")
}

func atlasSymlinkEscapeError(name, via string) error {
	if via == "" {
		return fmt.Errorf("path escapes atlas.hcl directory: %s: %s", name, atlasFileSandboxHint)
	}
	return fmt.Errorf("path escapes atlas.hcl directory: %s: %s is a symbolic link pointing outside it: %s",
		name, via, atlasFileSandboxHint)
}

func atlasLocalFSPath(value string) (string, error) {
	if err := validateAtlasLocalPathValue(value); err != nil {
		return "", err
	}
	rawPath := strings.TrimPrefix(value, "file://")
	localPath := filepath.Clean(filepath.FromSlash(rawPath))
	if localPath == ".." || strings.HasPrefix(localPath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes atlas.hcl directory: %s: %s", value, atlasFileSandboxHint)
	}
	return filepath.ToSlash(localPath), nil
}

func validateAtlasLocalPathValue(value string) error {
	return atlasLocalPathRule(value, atlasFileSandboxHint)
}

// atlasLocalPathRule reports which of the two local-path rules value breaks, or
// nil when it breaks neither.
//
// hint is appended to the absolute-path refusal only. A scheme is refused
// because ptah reads no remote schemas at all, and no hint about which directory
// to use explains that; an absolute path is refused because of where it points,
// which is exactly what a hint can redirect.
func atlasLocalPathRule(value, hint string) error {
	switch {
	case filepath.IsAbs(strings.TrimPrefix(value, "file://")):
		return fmt.Errorf("absolute paths are not supported: %s: %s", value, hint)
	case strings.Contains(value, "://") && !strings.HasPrefix(value, "file://"):
		return fmt.Errorf("unsupported URL scheme: %s", value)
	default:
		return nil
	}
}

// atlasLocalFileURL turns a data.hcl_schema path into a file:// URL, or reports
// why the path cannot be read.
//
// It carries the reason from validateAtlasLocalPathValue instead of replacing it
// with unsupportedAttr, the way atlasLocalFSPath above already does not replace
// it. "path" is a supported key; an absolute path and an unreadable URL scheme
// are two different rules it breaks, and reporting both as an unsupported
// construct names the one thing here that is not the problem.
func (p atlasParser) atlasLocalFileURL(value string, attr *hclsyntax.Attribute) (string, error) {
	if err := atlasLocalPathRule(value, atlasDataSourcePathHint); err != nil {
		return "", p.invalidValue(attr.Name, attr, err)
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

// The three failures below all used to be reported as "unsupported atlas.hcl
// construct", which made them indistinguishable to a reader and to a test.
//
// They are not the same thing, and the difference is load-bearing for the work
// in stokaro/ptah#1014. Atlas CE tolerates an unknown NAME while still failing
// on a bad VALUE and on an expression it cannot evaluate -- so an
// accept-and-ignore change must relax exactly the first and leave the other two
// alone. While one message covers all three, no test can tell whether a
// refusal came from the branch that is meant to relax or from a branch that is
// meant to stay, and a relaxation would silently convert real agreements with
// CE into coincidental ones.
//
// Exit codes are unchanged: all three still fail. Only the message tells them
// apart.

// evaluationFailed reports an expression that could not be evaluated -- an
// undefined reference, a failing function call. The HCL diagnostic is carried
// through because it names the offending sub-expression, which our own message
// cannot.
func (p atlasParser) evaluationFailed(name string, attr *hclsyntax.Attribute, diags hcl.Diagnostics) error {
	return fmt.Errorf("cannot evaluate atlas.hcl %q at %s:%d: %s",
		name, attr.NameRange.Filename, attr.NameRange.Start.Line,
		p.scrubSensitive(diags.Error()))
}

// scrubSensitive removes the values of `sensitive = true` variables from text
// that is about to be shown to the user.
//
// It is needed because HCL renders evaluated arguments into its own diagnostics
// -- `file(var.secret)` fails with `openat <the secret>: no such file or
// directory` -- so passing a diagnostic through verbatim publishes the value
// that `sensitive` exists to hide. The rest of this parser already refuses to
// put variable values in messages; this keeps that invariant when the message
// comes from HCL rather than from us.
func (p atlasParser) scrubSensitive(text string) string {
	if p.sensitiveValues == nil {
		return text
	}
	for _, secret := range *p.sensitiveValues {
		text = strings.ReplaceAll(text, secret, "(sensitive value)")
	}
	return text
}

// invalidValue reports a known key whose value is of the right type but breaks a
// rule this parser enforces -- an absolute path, a URL scheme it does not read.
// The rule's own error is carried through because it names both the rule and the
// offending value; "unsupported atlas.hcl construct" names neither, and blames
// the key, which is supported.
//
// The text is scrubbed for the same reason evaluationFailed scrubs it: the value
// may have come from a `sensitive = true` variable, and the rule's error quotes
// the value.
func (p atlasParser) invalidValue(name string, attr *hclsyntax.Attribute, err error) error {
	return fmt.Errorf("atlas.hcl %q at %s:%d: %s",
		name, attr.NameRange.Filename, attr.NameRange.Start.Line,
		p.scrubSensitive(err.Error()))
}

// wrongValueType reports a known key given a value of the wrong type. The key
// is supported; the value is not.
func wrongValueType(name string, attr *hclsyntax.Attribute, want string) error {
	return fmt.Errorf("atlas.hcl %q at %s:%d must be %s",
		name, attr.NameRange.Filename, attr.NameRange.Start.Line, want)
}

// emptyValue reports a known key given a value that is syntactically fine but
// carries nothing usable.
func emptyValue(name string, attr *hclsyntax.Attribute) error {
	return fmt.Errorf("atlas.hcl %q at %s:%d must not be empty",
		name, attr.NameRange.Filename, attr.NameRange.Start.Line)
}
