package projectconfig

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io/fs"
	"maps"
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

	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlasprojectpath"
)

// AtlasLoadOptions selects Atlas project config evaluation settings.
type AtlasLoadOptions struct {
	// Context governs data-source connections, runtime-variable reads, and
	// subprocesses. A nil context uses context.Background.
	Context context.Context
	EnvName string
	Vars    []string
	// RejectListMapForEach refuses dynamic env expansion over list and map
	// values. Atlas Community Edition accepts tuple, object and set values but
	// refuses list and map values; compatibility adapters select this option
	// when they need that exact boundary. The default retains Ptah's complete
	// dynamic-env capability.
	RejectListMapForEach bool
	// Verb names the command doing the load. See [LoadOptions.Verb].
	Verb string
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
	return singularAtlasConfig(configs, opts.EnvName, opts.Verb)
}

// LoadAtlasFileCollectionWithOptions loads every selected instance from an
// Atlas project config file with Atlas-compatible evaluation options. A
// missing file returns a collection containing one empty config.
func LoadAtlasFileCollectionWithOptions(
	path string,
	opts AtlasLoadOptions,
) ([]Config, error) {
	// A missing atlas.hcl returns an empty collection below without ever
	// reaching ParseAtlasFSCollectionWithOptions, so the resolve that entry
	// point makes would never run and a malformed [IgnoreEnvSchemasEnvVar] value
	// would be honored as its default on exactly the projects that have no
	// config file. Resolving here refuses it whatever the file system holds.
	if err := ValidateAtlasEnvironmentVariables(); err != nil {
		return nil, err
	}
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
	return singularAtlasConfig(configs, opts.EnvName, opts.Verb)
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
	return singularAtlasConfig(configs, opts.EnvName, opts.Verb)
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
	// The opt-out is resolved before the document is parsed, not when the parser
	// is built. hclsyntax.ParseConfig returns first on a document that does not
	// parse, so resolving after it would hide a malformed
	// [IgnoreEnvSchemasEnvVar] behind the syntax diagnostic: the operator would
	// fix the file and only then meet the environment error, on the next run.
	// This is the single entry point every ParseAtlas* API reaches, so one
	// resolve here covers all of them.
	ignoreSchemas, err := ignoreEnvSchemas.Resolve()
	if err != nil {
		return nil, err
	}
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse atlas project config: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse atlas project config: unsupported body type %T", file.Body)
	}

	p, err := newAtlasParser(opts.Context, fsys, opts.Vars, filename, opts.RejectListMapForEach, ignoreSchemas)
	if err != nil {
		return nil, err
	}
	return p.parseCollection(body, opts.EnvName)
}

// singularAtlasConfig narrows a selection to the one instance a verb can take.
//
// The refusal names the verb and the block, because the alternative was a
// sentence about a "collection-valued API" -- an internal detail an operator
// cannot act on, and one that read like a bug rather than like a limit of the
// verb they ran (stokaro/ptah#1696).
func singularAtlasConfig(configs []Config, envName, verb string) (Config, error) {
	switch len(configs) {
	case 1:
		return configs[0], nil
	case 0:
		return Config{}, fmt.Errorf("atlas env %q selected no project config instances", envName)
	default:
		return Config{}, fmt.Errorf(
			"%s cannot run against a for_each env: %s env %q expands to %d environments, "+
				"and this command takes one. Select a single environment, or run the command once per instance",
			verbOrCommand(verb),
			AtlasFileName,
			envName,
			len(configs),
		)
	}
}

// verbOrCommand names the caller for the refusal above, falling back to a
// general noun when the caller did not say.
func verbOrCommand(verb string) string {
	if strings.TrimSpace(verb) == "" {
		return "this command"
	}
	return verb
}

type atlasParser struct {
	runContext  context.Context
	fsys        fs.FS
	ctx         *hcl.EvalContext
	varOverride map[string]cty.Value
	// ignoreEnvSchemas carries the resolved [IgnoreEnvSchemasEnvVar] value,
	// read by the caller before the document is parsed rather than at the
	// `schemas` arm. The arm is only reached by a config that spells the
	// attribute in the selected environment, so resolving there made a malformed
	// value depend on the file under parse: the same broken environment failed
	// one project and was ignored by the next.
	ignoreEnvSchemas bool
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
	// migrationDirectories holds immutable data.template_dir filesystems by
	// their mem:// URL. The map is shared across parser copies and attached to
	// each selected Config after evaluation.
	migrationDirectories map[string]MigrationDirectorySource
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
	// sensitiveNames holds the NAMES of those variables.
	//
	// Scrubbing by value only removes the value as it was authored. A function
	// that transforms its argument defeats that: `file(jsonencode(var.token))`
	// on a token containing a backslash fails with the escaped spelling, which
	// does not contain the original as a substring and therefore survives the
	// scrubber. Knowing the names lets a diagnostic be withheld whenever the
	// expression touched a sensitive variable at all, which is decidable where
	// "did any function derive a value from it" is not.
	sensitiveNames *map[string]struct{}
	// ignored records the constructs tolerated under Atlas CE's
	// unknown-name policy, so the caller can report them. Pointer for the same
	// reason as sensitiveValues: the parser is passed by value.
	ignored     *[]IgnoredAtlasConstruct
	ignoredSeen map[ignoredAtlasConstructKey]struct{}
	// hclSchemaScopes records, per data "hcl_schema" name, the variable values
	// that block scopes to its files and the file:// URLs it minted, so
	// [atlasParser.schemaSourceVarScopes] can attribute an evaluated `src` list
	// back to the block it came from. A plain map: the parser is passed by value
	// but a map header is shared, and every write happens before any env body is
	// parsed.
	hclSchemaScopes map[string]hclSchemaVarScope
}

// hclSchemaVarScope is one data "hcl_schema" block's contribution to the
// variable scoping rule: the values it declares, and the source URLs it minted.
type hclSchemaVarScope struct {
	// values is the decoded `vars` map, nil when the block declares none. A nil
	// map still scopes: see [Config.SchemaSourceVars].
	values map[string]string
	// urls are the file:// URLs this block put into data.hcl_schema.<name>.url.
	urls []string
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
	runContext context.Context,
	fsys fs.FS,
	rawVars []string,
	filename string,
	rejectListMapForEach,
	ignoreSchemas bool,
) (atlasParser, error) {
	if runContext == nil {
		runContext = context.Background()
	}
	overrides, err := parseAtlasVarOverrides(rawVars)
	if err != nil {
		return atlasParser{}, err
	}
	// Addressable locals rather than &[]T{}: make's result is not
	// addressable, and new([]T) would point at a nil slice instead of an
	// empty one, which is a different value to every reader of these fields.
	sensitiveValues := make([]string, 0)
	sensitiveNames := make(map[string]struct{})
	ignored := make([]IgnoredAtlasConstruct, 0)
	return atlasParser{
		runContext:      runContext,
		fsys:            fsys,
		sensitiveValues: &sensitiveValues,
		sensitiveNames:  &sensitiveNames,
		ignored:         &ignored,
		ignoredSeen:     make(map[ignoredAtlasConstructKey]struct{}),
		ctx: &hcl.EvalContext{
			Variables: make(map[string]cty.Value),
			Functions: atlasProjectFunctions(fsys),
		},
		varOverride:          overrides,
		ignoreEnvSchemas:     ignoreSchemas,
		baseDir:              filepath.Dir(filename),
		rejectListMapForEach: rejectListMapForEach,
		externalSchemas:      make(map[string]externalSchemaDataSource),
		hclSchemaScopes:      make(map[string]hclSchemaVarScope),
		migrationDirectories: make(map[string]MigrationDirectorySource),
	}, nil
}

func (p atlasParser) parseCollection(body *hclsyntax.Body, envName string) ([]Config, error) {
	p.ctx.Variables["atlas"] = cty.ObjectVal(map[string]cty.Value{
		"env": cty.StringVal(envName),
	})

	base := Config{}
	blocks, err := p.collectAtlasTopBlocks(body.Blocks)
	if err != nil {
		return nil, err
	}
	if err := p.validateAtlasEnvStructures(blocks.envs); err != nil {
		return nil, err
	}
	var selected []atlasEnvBlock
	if len(blocks.envs) > 0 {
		selected, err = selectAtlasEnvBlocks(blocks.envs, envName)
		if err != nil {
			return nil, err
		}
	}
	if err := p.configureEvalContext(
		blocks.variables,
		blocks.locals,
		blocks.data,
		atlasEvaluationRoots(body.Attributes, blocks.globalDiff, blocks.globalLint, selected),
	); err != nil {
		return nil, err
	}

	// CE's tolerance covers unknown ATTRIBUTES as well as blocks -- measured,
	// and the point stokaro/ptah#1014 left open. Evaluation happens after the
	// selected data-source dependency graph is available, so a top-level value
	// can reference data without making every declaration eager.
	for _, name := range sortedAttributeNames(body.Attributes) {
		attr := body.Attributes[name]
		value, diags := attr.Expr.Value(p.ctx)
		if diags.HasErrors() {
			return nil, p.evaluationFailed(name, attr, diags)
		}
		if err := checkAtlasToleratedValue(atlasTopLevelScope, name, attr, value); err != nil {
			return nil, err
		}
		p.noteIgnored("attribute", name, attr.NameRange)
	}
	if err := p.parseSingleAtlasBlock(blocks.globalDiff, &base, p.parseDiff); err != nil {
		return nil, err
	}
	if err := p.parseSingleAtlasBlock(blocks.globalLint, &base, p.parseLint); err != nil {
		return nil, err
	}
	if len(blocks.envs) == 0 {
		base.migrationDirectories = cloneMigrationDirectories(p.migrationDirectories)
		base.IgnoredConstructs = p.ignoredConstructs()
		return []Config{base}, nil
	}

	configs := make([]Config, 0, len(selected))
	for _, env := range selected {
		instances, err := p.parseAtlasEnvInstances(env, envName)
		if err != nil {
			return nil, err
		}
		for _, instance := range instances {
			merged := Merge(base, instance)
			merged.migrationDirectories = cloneMigrationDirectories(p.migrationDirectories)
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
var ceEnforcedConstructs = make(map[string]struct{})

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
	// A name CE decodes is not tolerable in every shape. This is the right place
	// for that check rather than the structure validator: it runs once per
	// SELECTED env with `var`, `local` and `data` already in the context. See
	// [checkAtlasToleratedValue].
	if err := checkAtlasToleratedValue(scope, name, attr, value); err != nil {
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
	if err := p.evaluateIgnoredBody(scope+"."+block.Type, block.Body); err != nil {
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
// is being ignored, returning the first failure.
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
//
// The value rules run here too, for the same reason and one step further: an
// ignored block can CONTAIN a name CE decodes. `test` is the measured case --
// the block is dropped whole by both binaries, and CE still decodes `schema`
// and `migrate` inside it. Scope is threaded rather than dropped so those rules
// can be keyed by where the name sits, exactly as they are on the attribute
// path.
func (p atlasParser) evaluateIgnoredBody(scope string, body *hclsyntax.Body) error {
	if body == nil {
		return nil
	}
	for _, name := range sortedAttributeNames(body.Attributes) {
		attr := body.Attributes[name]
		value, diags := attr.Expr.Value(p.ctx)
		if diags.HasErrors() {
			return p.evaluationFailed(name, attr, diags)
		}
		if err := checkAtlasToleratedValue(scope, name, attr, value); err != nil {
			return err
		}
	}
	for _, block := range body.Blocks {
		if err := p.evaluateIgnoredBody(scope+"."+block.Type, block.Body); err != nil {
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
		if err := p.evaluateIgnoredBody(atlasTopLevelScope+"."+block.Type, block.Body); err != nil {
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

	seen := make(map[string]struct{})
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
			if err := p.parseSchemaSources(attr, cfg); err != nil {
				return err
			}
		default:
			if err := p.tolerateUnknownAttr("env.schema", attrName, attr); err != nil {
				return err
			}
		}
	}
	seen := make(map[string]struct{})
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
		return p.parseSchemaSources(attr, cfg)
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
		if p.ignoreEnvSchemas {
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
		if err := p.parseMigrationAttr(attrName, attr, &migration, cfg); err != nil {
			return err
		}
	}
	if err := p.parseMigrationBlocks(block); err != nil {
		return err
	}
	cfg.Migration = migration
	return nil
}

// parseMigrationAttr decodes one attribute of an env.migration block into
// migration, marking its presence on cfg. It is split out of
// [atlasParser.parseMigration] so the block's own bookkeeping -- the format
// defaults, the nested blocks -- stays readable next to a switch that grows a
// case every time a documented Atlas name is implemented.
func (p atlasParser) parseMigrationAttr(
	attrName string,
	attr *hclsyntax.Attribute,
	migration *MigrationConfig,
	cfg *Config,
) error {
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
	case "baseline":
		value, decoded, err := p.nullableStringAttr(attrName, attr)
		if err != nil {
			return err
		}
		if decoded {
			migration.Baseline = value
			cfg.presence.mark(fieldMigrationBaseline)
		}
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
		return p.tolerateUnknownAttr("env.migration", attrName, attr)
	}
	return nil
}

// parseMigrationBlocks handles the nested blocks of env.migration, which used
// to be refused wholesale. The pinned community binary v1.3.0 accepts every one
// of them -- see the `migration` entry in [atlasEnvBodyStructure] for the
// measurement -- so the whole set is tolerated and only `repo` is decoded.
func (p atlasParser) parseMigrationBlocks(block *hclsyntax.Block) error {
	seen := make(map[string]struct{})
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
	seen := make(map[string]struct{})
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
		cfg.Lint.RuleConfigs = make(map[string]LintRuleConfig)
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
	seen := make(map[string]struct{})
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

func (p atlasParser) configureVariables(blocks []*hclsyntax.Block) error {
	vars := make(map[string]cty.Value)
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
		if variable.sensitive {
			if p.sensitiveValues != nil {
				appendSensitiveStrings(p.sensitiveValues, value)
			}
			if p.sensitiveNames != nil {
				(*p.sensitiveNames)[name] = struct{}{}
			}
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
	vars := make(map[string]cty.Value)
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

func appendAtlasVarValue(existing, value cty.Value) cty.Value {
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
	name,
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
	name,
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
	// The scope map describes entries of that list, so it goes with it. An
	// external-schema env has no local schema files for a variable scope to
	// reach.
	cfg.schemaSourceVars = nil
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
		case "path", "paths", "vars":
		default:
			return cty.NilVal, unsupportedAttr(attrName, attr)
		}
	}
	values, err := p.hclSchemaVarsAttr(block.Body.Attributes["vars"])
	if err != nil {
		return cty.NilVal, err
	}
	urls, value, err := p.hclSchemaDataSourceURLs(block)
	if err != nil {
		return cty.NilVal, err
	}
	p.hclSchemaScopes[block.Labels[1]] = hclSchemaVarScope{values: values, urls: urls}
	return value, nil
}

// hclSchemaDataSourceURLs mints the data source's file:// URLs and the `url`
// object an expression reads them through, returning both so the caller can
// record the scope without re-deriving the list from the cty value.
func (p atlasParser) hclSchemaDataSourceURLs(block *hclsyntax.Block) ([]string, cty.Value, error) {
	pathAttr, hasPath := block.Body.Attributes["path"]
	pathsAttr, hasPaths := block.Body.Attributes["paths"]
	switch {
	case hasPath && hasPaths:
		return nil, cty.NilVal, unsupportedAttr("paths", pathsAttr)
	case hasPath:
		value, err := p.stringAttr("path", pathAttr)
		if err != nil {
			return nil, cty.NilVal, err
		}
		url, err := p.atlasLocalFileURL(value, pathAttr)
		if err != nil {
			return nil, cty.NilVal, err
		}
		return []string{url}, cty.ObjectVal(map[string]cty.Value{
			"url": cty.StringVal(url),
		}), nil
	case hasPaths:
		values, err := p.stringListAttr("paths", pathsAttr)
		if err != nil {
			return nil, cty.NilVal, err
		}
		urls := make([]string, 0, len(values))
		for _, value := range values {
			url, err := p.atlasLocalFileURL(value, pathsAttr)
			if err != nil {
				return nil, cty.NilVal, err
			}
			urls = append(urls, url)
		}
		return urls, cty.ObjectVal(map[string]cty.Value{
			"url": ctyStringList(urls),
		}), nil
	default:
		return nil, cty.NilVal, fmt.Errorf("atlas.hcl data.hcl_schema %q requires path or paths at %s:%d",
			block.Labels[1], block.TypeRange.Filename, block.TypeRange.Start.Line)
	}
}

// parseSchemaSources reads the desired-state sources of an env, from either
// spelling of the attribute -- `env.src` and `env.schema.src` are the same
// setting -- together with the variable scope each one carries.
//
// One function for both, so the scope cannot be attached on one spelling and
// forgotten on the other.
func (p atlasParser) parseSchemaSources(attr *hclsyntax.Attribute, cfg *Config) error {
	values, err := p.stringOrStringListAttr("src", attr)
	if err != nil {
		return err
	}
	scopes, err := p.schemaSourceVarScopes(attr, values)
	if err != nil {
		return err
	}
	cfg.SchemaSources = values
	cfg.schemaSourceVars = scopes
	cfg.presence.mark(fieldSchemaSources)
	return nil
}

// schemaSourceVarScopes attributes each evaluated `src` URL back to the
// data "hcl_schema" block that minted it, so the loader can hand that block's
// `vars` to those files and only those files.
//
// Attribution is the intersection of two facts, and needs both:
//
//   - which data sources the src EXPRESSION references, read off
//     `data.hcl_schema.<name>` traversals by [atlasParser.hclSchemaReferences],
//     which follows the branch a conditional takes rather than both;
//   - which URLs each of those data sources minted.
//
// The expression half is what keeps a declared-but-unreferenced data source out
// of it. A file with `data "hcl_schema" "app" { paths = ["s.hcl"] vars = {…} }`
// and `src = "file://s.hcl"` names the same file without going through the data
// source, and the pinned binary passes it `--var` rather than the block's vars,
// because nothing selected the block. Matching URLs alone would have handed it
// the vars.
//
// The URL half is what keeps two referenced data sources apart. When two of
// them mint the SAME url with different values there is no honest answer, so
// this refuses rather than picking one -- silently choosing would make the
// desired state depend on map order. That verdict is reached over the URLs the
// src EVALUATED to and no others: a URL no evaluated source names carries no
// scope, so two blocks minting it disagree about nothing.
//
// One case stays approximate and is called out rather than hidden: a src
// expression that references a data source AND repeats one of that source's own
// paths as a literal gets the block's vars for both copies. They are the same
// file, so the only way to tell them apart would be to track provenance through
// arbitrary HCL expressions.
func (p atlasParser) schemaSourceVarScopes(
	attr *hclsyntax.Attribute,
	urls []string,
) (map[string]map[string]string, error) {
	referenced := p.hclSchemaReferences(attr.Expr)
	if len(referenced) == 0 {
		return nil, nil
	}
	minted := make(map[string]string)
	scopes := make(map[string]map[string]string)
	// file records one spelling of one URL under the data source that minted
	// it, and refuses when a block already filed that spelling with different
	// values. reported is the URL the refusal names, which is always the
	// project file's own spelling even when the key is the resolved one.
	file := func(key, reported, owner string, values map[string]string) error {
		if previous, taken := minted[key]; taken && !maps.Equal(p.hclSchemaScopes[previous].values, values) {
			return fmt.Errorf(
				"atlas.hcl data.hcl_schema %q and %q both select %q with different vars at %s:%d",
				previous, owner, reported, attr.NameRange.Filename, attr.NameRange.Start.Line,
			)
		}
		minted[key] = owner
		scopes[key] = values
		return nil
	}
	for _, name := range referenced {
		scope, declared := p.hclSchemaScopes[name]
		if !declared {
			continue
		}
		for _, url := range scope.urls {
			// Ambiguity is decided among the URLs this src EVALUATED to, not
			// among every URL the referenced blocks could mint. A URL no
			// evaluated source names carries no scope, so two blocks minting it
			// have nothing to disagree about, and refusing on it would reject a
			// project whose desired state is perfectly determined.
			if !slices.Contains(urls, url) {
				continue
			}
			if err := file(url, url, name, scope.values); err != nil {
				return nil, err
			}
			// The same scope is filed a second time under the base-directory
			// resolved spelling, because two consumers reach it by two different
			// strings and both are legitimate. `env://src` expansion asks with
			// the value exactly as this file minted it; `schema apply`,
			// `schema diff` and `migrate diff` resolve the project's schema
			// sources against the atlas.hcl directory first and ask with the
			// absolute URL. Resolving through the SAME function those commands
			// use is what keeps the two keys from drifting -- an independent
			// filepath.Join here would answer differently the moment a symlink
			// or a `..` segment is involved.
			//
			// The resolved key goes through the same conflict test as the raw
			// one, and that is not belt and braces: two blocks can select ONE
			// file under two spellings -- `path = "s.hcl"` and
			// `path = "./s.hcl"` -- which the raw keys cannot see, and the
			// resolved key is the one every command asks with. Filing it
			// unchecked let the second block's values silently replace the
			// first's.
			resolved, err := atlasprojectpath.SchemaFileURL(url, p.baseDir)
			if err != nil {
				continue
			}
			if err := file(resolved, url, name, scope.values); err != nil {
				return nil, err
			}
		}
	}
	if len(scopes) == 0 {
		return nil, nil
	}
	return scopes, nil
}

// hclSchemaReferences reports the data "hcl_schema" names an expression reads,
// in a stable order so a refusal names the same pair on every run.
//
// A conditional is read as the branch it takes. [hclsyntax.Expression.Variables]
// reports both branches of `src = var.use_app ? data.hcl_schema.app.url :
// data.hcl_schema.other.url`, but the attribute evaluates to exactly one of
// them, and the pinned Atlas community binary v1.3.0 hands that block's vars to
// the file it names. Measured with two blocks selecting the same s.hcl with
// different vars, `schema apply --env local --dry-run`, exit codes read
// directly from unpiped invocations:
//
//	predicate true   0  DEFAULT 'acme'
//	predicate false  0  DEFAULT 'zzz'
//
// Reading both branches made every such project ambiguous, so this side refused
// both rows at exit 1 where that binary exits 0.
//
// The predicate is decided with the very context the attribute is evaluated
// with, including the `each` of a dynamic env instance, so nothing is settled
// here that HCL settles differently one step later. An undecidable predicate
// keeps both branches: that reading can only add a name, never drop one, so an
// expression this walk does not follow stays as conservative as it was.
func (p atlasParser) hclSchemaReferences(expr hclsyntax.Expression) []string {
	names := make([]string, 0, 1)
	p.appendHCLSchemaReferences(expr, &names)
	slices.Sort(names)
	return names
}

// appendHCLSchemaReferences walks the expression shapes that can select one
// data source out of several, and falls back to the flat variable list for
// everything else.
//
// The shapes it follows are the ones that carry a selected URL through
// unchanged: a list, a parenthesized expression, an index into one, and a
// relative traversal off one. Each can wrap a conditional -- `(var.pick ?
// data.hcl_schema.app.url : data.hcl_schema.other.url)[0]` is the shape that
// motivated the index arm -- and the flat variable list would report both
// branches again from inside them. Anything else lands in the default and stays
// conservative.
func (p atlasParser) appendHCLSchemaReferences(expr hclsyntax.Expression, names *[]string) {
	switch typed := expr.(type) {
	case *hclsyntax.ConditionalExpr:
		// The predicate's own references are kept. A data source read there
		// contributes no URL, but dropping it would narrow a refusal this
		// change is not measuring.
		p.appendHCLSchemaReferences(typed.Condition, names)
		for _, branch := range p.conditionalBranches(typed) {
			p.appendHCLSchemaReferences(branch, names)
		}
	case *hclsyntax.ParenthesesExpr:
		p.appendHCLSchemaReferences(typed.Expression, names)
	case *hclsyntax.TupleConsExpr:
		for _, item := range typed.Exprs {
			p.appendHCLSchemaReferences(item, names)
		}
	case *hclsyntax.IndexExpr:
		p.appendHCLSchemaReferences(typed.Collection, names)
		p.appendHCLSchemaReferences(typed.Key, names)
	case *hclsyntax.RelativeTraversalExpr:
		p.appendHCLSchemaReferences(typed.Source, names)
	default:
		appendHCLSchemaTraversalNames(expr.Variables(), names)
	}
}

// conditionalBranches returns the branch a conditional takes, or both branches
// when the predicate cannot be decided while the project file is parsed.
func (p atlasParser) conditionalBranches(expr *hclsyntax.ConditionalExpr) []hclsyntax.Expression {
	both := []hclsyntax.Expression{expr.TrueResult, expr.FalseResult}
	value, diags := expr.Condition.Value(p.ctx)
	if diags.HasErrors() {
		return both
	}
	decided, err := convert.Convert(value, cty.Bool)
	if err != nil || decided.IsNull() || !decided.IsKnown() {
		return both
	}
	if decided.True() {
		return []hclsyntax.Expression{expr.TrueResult}
	}
	return []hclsyntax.Expression{expr.FalseResult}
}

// appendHCLSchemaTraversalNames collects the data "hcl_schema" names of a flat
// traversal list, skipping duplicates.
func appendHCLSchemaTraversalNames(traversals []hcl.Traversal, names *[]string) {
	for _, traversal := range traversals {
		if len(traversal) < 3 || traversal.RootName() != "data" {
			continue
		}
		kind, ok := traversal[1].(hcl.TraverseAttr)
		if !ok || kind.Name != "hcl_schema" {
			continue
		}
		name, ok := traversal[2].(hcl.TraverseAttr)
		if !ok || slices.Contains(*names, name.Name) {
			continue
		}
		*names = append(*names, name.Name)
	}
}

// hclSchemaVarsAttr decodes `data "hcl_schema" { vars }` into the variable
// values the referenced schema files are parsed with. A nil attr -- the block
// declares no `vars` -- yields a nil map, which still closes the scope; see
// [Config.SchemaSourceVars].
//
// Measured on the pinned Atlas community binary v1.3.0 with
// `schema apply --env local --dry-run` against `s.hcl` declaring
// `variable "tenant" { type = string }` with no default, exit codes read
// directly from unpiped invocations:
//
//	vars = { tenant = "acme" }                -> 0  DEFAULT 'acme'
//	vars = { tenant = 42 }                    -> 0  DEFAULT '42'
//	vars = { tenant = true }                  -> 0  DEFAULT 'true'
//	vars = { tenant = "acme", count = 7 }     -> 0  DEFAULT 'acme'  (mixed member
//	                                                 types still decode)
//	vars = { tenant = "acme", frobnicate9 = "x" }
//	                                          -> 0  (an undeclared name is ignored)
//	vars = {}                                 -> 1  missing value for required
//	vars = null                               -> 1  variable "tenant" -- both read
//	                                                 as "no values given", which is
//	                                                 the control that the earlier
//	                                                 rows are carrying a value
//	vars = "acme"                             -> 1  Unsuitable value: map of any
//	vars = [1, 2]                             -> 1  single type required
//	vars = { tenant = [1, 2] }                -> 1  variable "tenant": string
//	                                                 required
//
// The last row is refused HERE rather than by the schema file, because Ptah
// carries the value as text. That is exit 1 on both binaries for a string-typed
// variable; for a variable declared `list(string)` the pinned binary may take a
// list where Ptah does not, which is a narrower surface in the safe direction
// and is not measured above.
func (p atlasParser) hclSchemaVarsAttr(attr *hclsyntax.Attribute) (map[string]string, error) {
	if attr == nil {
		return nil, nil
	}
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, p.evaluationFailed("vars", attr, diags)
	}
	if value.IsNull() {
		return nil, nil
	}
	valueType := value.Type()
	if !valueType.IsObjectType() && !valueType.IsMapType() {
		return nil, wrongValueType("vars", attr, "a map of values")
	}
	values := make(map[string]string)
	for it := value.ElementIterator(); it.Next(); {
		name, member := it.Element()
		if member.IsNull() {
			return nil, wrongValueType("vars."+name.AsString(), attr, "a string, a number, or a bool")
		}
		text, err := convert.Convert(member, cty.String)
		if err != nil {
			return nil, wrongValueType("vars."+name.AsString(), attr, "a string, a number, or a bool")
		}
		values[name.AsString()] = text.AsString()
	}
	return values, nil
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

// decodedAttrValue evaluates attr for a name Ptah DECODES and refuses the two
// shapes no such name takes: an expression that fails to evaluate, and a null.
//
// Null is refused here rather than in each type gate because a cty type does
// not separate a value from the absence of one. `cty.NullVal(cty.String)` --
// which is what a typed variable produces, as in
// `variable "s" { type = string, default = null }` followed by `dev = var.s` --
// answers cty.String to Type(), so it walks straight through a
// `value.Type() != cty.String` gate and panics in AsString(). A bare `null`
// literal carries cty.DynamicPseudoType instead and the same gate refuses it.
// The two spellings of one value therefore took two different paths: a bare
// `null` a location-aware refusal at exit 1, a typed null an internal error at
// exit 2. `want` is the same phrase the type gate would have used, so the
// message does not depend on which spelling arrived.
//
// Refusing is what the eight decoded names measured for this all already do for
// a bare `null` -- `dev`, `env.migration.dir`, `env.migration.tx_mode`,
// `env.migration.repo.name`, `env.schema.repo.name`, `env.schema.mode.tables`,
// `env.exclude` and `lint.latest` are each exit 1 with "must be a <type>" -- so
// on every string-valued and number-valued name this replaces an exit-2 crash
// with the exit 1 the bare spelling already produced. The BOOL-valued names
// move from 0 to 1, because a typed null never crashed there: cty.Value.True()
// answers false for one, so `mode { tables = var.b }` and
// `lint { destructive { error = var.b } }` used to read a null as "off" and
// disable table inspection, or destructive-change linting, in silence.
//
// Names Ptah only TOLERATES are unaffected: they are answered by
// [checkAtlasDecodedLeafAttribute], which returns before its type gate on a
// null because the pinned community binary accepts null for every one of them.
func (p atlasParser) decodedAttrValue(
	name string,
	attr *hclsyntax.Attribute,
	want string,
) (cty.Value, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return cty.NilVal, p.evaluationFailed(name, attr, diags)
	}
	if value.IsNull() {
		return cty.NilVal, wrongValueType(name, attr, want)
	}
	return value, nil
}

func (p atlasParser) stringAttr(name string, attr *hclsyntax.Attribute) (string, error) {
	value, err := p.decodedAttrValue(name, attr, "a string")
	if err != nil {
		return "", err
	}
	if value.Type() != cty.String {
		return "", wrongValueType(name, attr, "a string")
	}
	return value.AsString(), nil
}

// nullableStringAttr decodes a string-valued name whose null spelling the
// pinned community binary v1.3.0 reads as "no value given" rather than as an
// error. decoded is false for a null, and the caller must leave its field and
// its presence mark alone.
//
// It is separate from [atlasParser.stringAttr] because that helper goes through
// [atlasParser.decodedAttrValue], which refuses every null. That refusal is
// deliberate for the eight names it was measured against -- see the comment
// there -- but it is a divergence in the loud direction, and a name added after
// the fact should not inherit it. Measured with
// `migrate apply --env local --dry-run` against a hashed two-migration
// directory, exit codes read directly from unpiped invocations:
//
//	migration { baseline = null }              -> 0  "2 migrations in total"
//	migration { baseline = "" }                -> 0  "2 migrations in total"
//	migration { baseline = "20260719010000" }  -> 0  "from 20260719010000
//	                                                 (1 migrations in total)"
//	migration { baseline = "20200101000000" }  -> 1  baseline version
//	                                                 "20200101000000" not found
//	migration { baseline = [1, 2] }            -> 1  value of attr "baseline"
//	                                                 cannot be read as string
//
// The first row is the one this helper exists for, and the third is the control
// that keeps it honest: a binary that ignored `baseline` entirely would answer
// "2 migrations in total" to both.
func (p atlasParser) nullableStringAttr(
	name string,
	attr *hclsyntax.Attribute,
) (value string, decoded bool, err error) {
	evaluated, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return "", false, p.evaluationFailed(name, attr, diags)
	}
	if evaluated.IsNull() {
		return "", false, nil
	}
	if evaluated.Type() != cty.String {
		return "", false, wrongValueType(name, attr, "a string")
	}
	return evaluated.AsString(), true, nil
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
	// !value.IsNull() for the reason given on [atlasParser.decodedAttrValue]:
	// a typed null answers cty.String to Type() and panics in AsString(). This
	// arm cannot use that helper, because an evaluation failure is not fatal
	// here -- a bare identifier such as `sensitive = ALLOW` is an unresolvable
	// traversal that the fallback below reads as a word.
	if !diags.HasErrors() && !value.IsNull() && value.Type() == cty.String {
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
	// !value.IsNull() for the same reason as in
	// [atlasParser.identifierOrStringAttr].
	if !diags.HasErrors() && !value.IsNull() && value.Type() == cty.String {
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
	// A null is refused rather than reaching cty.Value.True(), which does not
	// panic on one -- it answers false. That is the quiet half of the same
	// defect: `mode { tables = var.b }` with a null bool used to disable table
	// inspection instead of saying anything.
	value, err := p.decodedAttrValue(name, attr, "a bool")
	if err != nil {
		return false, err
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
	// A null reaches [stringListValue], which refuses it as "a list of strings"
	// -- the phrase a bare `null` already produced here, since the string arm
	// never took it either.
	if !value.IsNull() && value.Type() == cty.String {
		return []string{value.AsString()}, nil
	}
	return stringListValue(name, attr, value)
}

func (p atlasParser) intAttr(name string, attr *hclsyntax.Attribute) (int, error) {
	value, err := p.decodedAttrValue(name, attr, "a number")
	if err != nil {
		return 0, err
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

// stringListValue reads a decoded list-of-strings attribute.
//
// The two null tests are the collection counterpart of the rule on
// [atlasParser.decodedAttrValue], and both are load-bearing:
//
//   - A null LIST answers true to CanIterateElements, because that answer comes
//     from the type. LengthInt() on it then panics. A bare `exclude = null`
//     never got that far, because cty.DynamicPseudoType is neither a tuple nor
//     a list; `list(string)` holding null does.
//   - A null ELEMENT answers cty.String to Type(), so it passed the element
//     gate and panicked in AsString(). The pinned community binary refuses one:
//     `exclude`, `schemas` and `src` fed a `list(string)` variable holding
//     ["public.t1", null] each answer exit 1 there,
//     `cannot read attribute … as string list: null value is not allowed`.
func stringListValue(name string, attr *hclsyntax.Attribute, value cty.Value) ([]string, error) {
	valueType := value.Type()
	if value.IsNull() || !value.CanIterateElements() ||
		(!valueType.IsTupleType() && !valueType.IsListType()) {
		return nil, wrongValueType(name, attr, "a list of strings")
	}
	values := make([]string, 0, value.LengthInt())
	it := value.ElementIterator()
	for it.Next() {
		_, item := it.Element()
		if item.IsNull() || item.Type() != cty.String {
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
	values := make([]string, 0)
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
	case leavesTheProjectDirectory(strings.TrimPrefix(value, "file://")):
		return fmt.Errorf("absolute paths are not supported: %s: %s", value, hint)
	case strings.Contains(value, "://") && !strings.HasPrefix(value, "file://"):
		return fmt.Errorf("unsupported URL scheme: %s", value)
	default:
		return nil
	}
}

// leavesTheProjectDirectory reports whether a path starts at a root rather than
// inside the project.
//
// filepath.IsAbs alone is not that question, and the difference is not
// cosmetic. On Windows "/tmp/secret.txt" has no volume name, so IsAbs answers
// false -- while the path still resolves to C:\tmp\secret.txt, outside every
// project. An atlas.hcl refused on Linux would have been read on Windows.
//
// The rule is deliberately the same on every operating system: what a project
// file is allowed to name must not depend on which machine opens it. A leading
// backslash counts too, which on Unix rules out a file literally named "\tmp"
// -- an acceptable loss for a rule whose whole job is to fail closed.
func leavesTheProjectDirectory(path string) bool {
	return filepath.IsAbs(path) ||
		strings.HasPrefix(path, "/") ||
		strings.HasPrefix(path, `\`) ||
		hasVolumeName(path)
}

// hasVolumeName reports whether path begins with a Windows drive letter.
//
// filepath.VolumeName answers "" for `C:\x` on Unix, so the check is written
// out rather than delegated: a project file naming a drive is refused
// everywhere, including on the host where that spelling happens to be an
// ordinary directory called "C:".
func hasVolumeName(path string) bool {
	if len(path) < 2 || path[1] != ':' {
		return false
	}
	drive := path[0]
	return (drive >= 'A' && drive <= 'Z') || (drive >= 'a' && drive <= 'z')
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
	if secret, found := p.sensitiveVariableIn(attr.Expr); found {
		return fmt.Errorf(
			"cannot evaluate atlas.hcl %q at %s:%d: the expression reads the sensitive variable %q and failed; "+
				"the underlying diagnostic is withheld because it quotes the evaluated argument, which a "+
				"function may have derived from that variable in a form scrubbing cannot recognize",
			name, attr.NameRange.Filename, attr.NameRange.Start.Line, secret)
	}
	return fmt.Errorf("cannot evaluate atlas.hcl %q at %s:%d: %s",
		name, attr.NameRange.Filename, attr.NameRange.Start.Line,
		p.scrubSensitive(diags.Error()))
}

// sensitiveVariableIn reports the first `sensitive = true` variable expr reads.
//
// The traversals are what the expression NAMES, so this stays correct however
// the value is transformed on the way to the failing call -- which is the half
// [atlasParser.scrubSensitive] cannot cover.
func (p atlasParser) sensitiveVariableIn(expr hcl.Expression) (string, bool) {
	if p.sensitiveNames == nil || expr == nil || len(*p.sensitiveNames) == 0 {
		return "", false
	}
	for _, traversal := range expr.Variables() {
		if len(traversal) < 2 || traversal.RootName() != "var" {
			continue
		}
		attr, ok := traversal[1].(hcl.TraverseAttr)
		if !ok {
			continue
		}
		if _, sensitive := (*p.sensitiveNames)[attr.Name]; sensitive {
			return attr.Name, true
		}
	}
	return "", false
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
// invalidValue reports a value that evaluated successfully and then failed a
// Ptah rule -- an unsupported URL scheme, a malformed duration.
//
// It withholds only when the value was DERIVED from a sensitive variable, and
// scrubs otherwise. The distinction is what keeps the message useful. A bare
// `path = var.secret` reaches the error with the variable's own bytes, so
// scrubbing replaces them and the operator still learns what was wrong with the
// value. `path = upper(var.secret)` does not: it puts `S3://SECRET` in the
// error while the scrubber looks for `s3://secret`, which is measured and is
// the reason this branch exists (stokaro/ptah#1810).
//
// [atlasParser.evaluationFailed] withholds for a bare reference too, and that
// asymmetry is deliberate: an HCL diagnostic quotes whatever sub-expression it
// blames, so there is no equivalent guarantee that the bytes survived.
func (p atlasParser) invalidValue(name string, attr *hclsyntax.Attribute, err error) error {
	if secret, found := p.derivedSensitiveVariableIn(attr.Expr); found {
		return fmt.Errorf(
			"atlas.hcl %q at %s:%d: the expression reads the sensitive variable %q and its value was "+
				"refused; the underlying reason is withheld because it quotes the evaluated value, which "+
				"a function may have derived from that variable in a form scrubbing cannot recognize",
			name, attr.NameRange.Filename, attr.NameRange.Start.Line, secret)
	}
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

// atlasProjectFunctions is the function set an atlas.hcl is evaluated with.
//
// It is deliberately NOT the schema evaluator's set, and the reason is
// disclosure rather than scope. A `sensitive = true` variable is redacted by
// replacing its literal bytes in a diagnostic, so a function that PRESERVES
// those bytes stays safe -- `file(format("%s-x", var.token))` reports
// `(sensitive value)-x` -- while one that transforms them does not:
// `file(upper(var.token))` reported the secret uppercased, which a CI log then
// keeps.
//
// Every function here is byte-preserving for its argument. Widening the set to
// match the schema evaluator needs the redaction to follow a value rather than
// a string, which cty marks are for; until then the wider set would trade a
// missing function name for a leaked credential (stokaro/ptah#1696).
// atlasProjectFunctions is the schema evaluator's function set with the three
// names this file binds itself overlaid on top.
//
// The set used to be eight names written out here, which meant `atlas.hcl`
// refused expressions a schema file evaluates -- `join(",", var.schemas)` among
// them, in the block most likely to assemble a list of schemas
// (stokaro/ptah#1810). Sharing it rather than lengthening it is what keeps the
// two from drifting again.
//
// The overlay direction is load-bearing: `file` and `fileset` here read the
// PROJECT filesystem, and a shared entry of either name would read some other
// directory while looking entirely correct.
func atlasProjectFunctions(fsys fs.FS) map[string]function.Function {
	return atlashcl.WithProjectBoundFunctions(
		atlashcl.ProjectFunctions(),
		atlasProjectBoundFunctions(fsys),
	)
}

// atlasProjectBoundFunctions are the three whose meaning depends on where the
// project file lives; see [atlashcl.ProjectBoundFunctionNames].
func atlasProjectBoundFunctions(fsys fs.FS) map[string]function.Function {
	return map[string]function.Function{
		"file":    atlasFileFunc(fsys),
		"fileset": atlasFilesetFunc(fsys),
		"getenv":  atlasGetenvFunc(),
	}
}

// derivedSensitiveVariableIn reports the first sensitive variable an expression
// TRANSFORMS, as opposed to naming directly.
//
// A bare traversal -- `var.secret` -- reaches a message unchanged, which is
// what makes scrubbing sufficient for it. Anything else may have passed the
// value through a function, and a function's output is not something byte
// replacement can find.
func (p atlasParser) derivedSensitiveVariableIn(expr hclsyntax.Expression) (string, bool) {
	if _, bare := expr.(*hclsyntax.ScopeTraversalExpr); bare {
		return "", false
	}
	return p.sensitiveVariableIn(expr)
}
