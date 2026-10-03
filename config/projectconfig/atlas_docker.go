package projectconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/internal/devdocker"
)

// A top-level `docker "<type>" "<name>"` block declares a dev database built
// from a container image, and `docker.<type>.<name>.url` names it
// (stokaro/ptah#4041). The block is lazy, as a data source is: it is read only
// when a selected env, a local or a data source references its url, and a
// block nothing references stays ignored, as the pinned community binary
// v1.3.0 ignores every docker block. That binary also refuses the reference,
// with `Unsupported attribute; This object does not have an attribute named
// "url"`; [AtlasLoadOptions.IgnoreDockerBlocks] selects that answer.
//
// The reference evaluates to a `docker+<driver>://` URL, so every command that
// takes a dev URL provisions it through internal/devdocker. What the URL cannot
// carry -- a build, the baseline, container environment, a readiness timeout --
// is recorded with [devdocker.Declare] under the URL's fragment.
//
// The attributes follow the block as Atlas documents it, read from its public
// configuration reference rather than measured: `image` (required), `schema`,
// `database` on PostgreSQL, `baseline`, `env`, `timeout`, and a `build` block
// with `context` (required), `dockerfile`, `dockerfile_inline`, `target`,
// `args` and `platform`. Every other attribute or block the reference lists is
// refused by name rather than accepted and ignored.

// atlasDockerBlockKey addresses one docker block by its two labels.
type atlasDockerBlockKey struct {
	typ  string
	name string
}

func (k atlasDockerBlockKey) String() string {
	return "docker." + k.typ + "." + k.name
}

// atlasDockerAttributes are the attributes a docker block may carry, keyed by
// the engines it may start.
var atlasDockerAttributes = map[string][]string{
	"postgres": {"image", "schema", "database", "baseline", "env", "timeout"},
	"mysql":    {"image", "schema", "baseline", "env", "timeout"},
	"mariadb":  {"image", "schema", "baseline", "env", "timeout"},
}

// atlasDockerBuildAttributes are the attributes of a docker block's build.
var atlasDockerBuildAttributes = []string{"context", "dockerfile", "dockerfile_inline", "target", "args", "platform"}

func (e *atlasEvaluator) collectDockerBlocks(blocks []*hclsyntax.Block) error {
	for _, block := range blocks {
		if len(block.Labels) != 2 {
			e.dockerUnaddressed = append(e.dockerUnaddressed, block)
			continue
		}
		key := atlasDockerBlockKey{typ: block.Labels[0], name: block.Labels[1]}
		if _, ok := e.dockerBlocks[key]; ok {
			return fmt.Errorf(
				"duplicate atlas.hcl docker %q %q at %s:%d",
				key.typ,
				key.name,
				block.TypeRange.Filename,
				block.TypeRange.Start.Line,
			)
		}
		e.dockerBlocks[key] = block
		e.dockerOrder = append(e.dockerOrder, key)
	}
	if e.parser.ignoreDockerBlocks {
		// The community binary's answer: the object exists, and has no url.
		for _, key := range e.dockerOrder {
			e.setDockerValue(key, cty.EmptyObjectVal)
		}
	}
	return nil
}

// ignoreUnresolvedDockerBlocks treats every docker block nothing resolved as
// the community binary treats all of them: its body is evaluated, so a broken
// reference inside it still fails, and it is reported as ignored.
func (e *atlasEvaluator) ignoreUnresolvedDockerBlocks() error {
	blocks := slices.Clone(e.dockerUnaddressed)
	for _, key := range e.dockerOrder {
		if e.dockerState[key] != atlasEvalComplete {
			blocks = append(blocks, e.dockerBlocks[key])
		}
	}
	for _, block := range blocks {
		if err := e.parser.evaluateIgnoredBody(atlasTopLevelScope+"."+block.Type, block.Body); err != nil {
			return err
		}
		e.parser.noteIgnored("block", block.Type, block.TypeRange)
	}
	return nil
}

func (e *atlasEvaluator) resolveDockerBlock(key atlasDockerBlockKey) error {
	block, ok := e.dockerBlocks[key]
	if !ok || e.parser.ignoreDockerBlocks {
		// Left to the evaluation, which reports the reference in HCL's words.
		return nil
	}
	switch e.dockerState[key] {
	case atlasEvalComplete:
		return nil
	case atlasEvalVisiting:
		return fmt.Errorf("atlas.hcl evaluation cycle involving %s", key)
	}
	e.dockerState[key] = atlasEvalVisiting
	for _, expr := range atlasBodyExpressions(block.Body) {
		if err := e.resolveExpressionDependencies(expr); err != nil {
			return err
		}
	}
	rawURL, err := e.parser.dockerBlockURL(key, block)
	if err != nil {
		return err
	}
	e.setDockerValue(key, cty.ObjectVal(map[string]cty.Value{"url": cty.StringVal(rawURL)}))
	e.dockerState[key] = atlasEvalComplete
	return nil
}

func (e *atlasEvaluator) setDockerValue(key atlasDockerBlockKey, value cty.Value) {
	names := e.dockerValues[key.typ]
	if names == nil {
		names = make(map[string]cty.Value)
		e.dockerValues[key.typ] = names
	}
	names[key.name] = value
	types := make(map[string]cty.Value, len(e.dockerValues))
	for typ, values := range e.dockerValues {
		types[typ] = cty.ObjectVal(values)
	}
	e.parser.ctx.Variables["docker"] = cty.ObjectVal(types)
}

// dockerBlockURL validates block, records its declaration, and returns the URL
// a reference to it evaluates to.
func (p atlasParser) dockerBlockURL(key atlasDockerBlockKey, block *hclsyntax.Block) (string, error) {
	allowed, ok := atlasDockerAttributes[key.typ]
	if !ok {
		return "", unsupported("docker."+key.typ, block.TypeRange)
	}
	for _, name := range sortedAttributeNames(block.Body.Attributes) {
		if !slices.Contains(allowed, name) {
			return "", unsupportedAttr(key.String()+"."+name, block.Body.Attributes[name])
		}
	}
	var build *devdocker.Build
	for _, nested := range block.Body.Blocks {
		if nested.Type != "build" || len(nested.Labels) > 0 || build != nil {
			return "", unsupported(key.String()+"."+nested.Type, nested.TypeRange)
		}
		parsed, err := p.dockerBuild(key, nested)
		if err != nil {
			return "", err
		}
		build = &parsed
	}
	values, err := p.dockerStrings(block, "image", "schema", "database", "baseline", "timeout")
	if err != nil {
		return "", err
	}
	if err := requireDockerValue(key.String(), block, values, "image"); err != nil {
		return "", err
	}
	declaration := devdocker.Declaration{Build: build, Baseline: values["baseline"]}
	if attr, ok := block.Body.Attributes["env"]; ok {
		if declaration.Env, err = p.stringListAttr("env", attr); err != nil {
			return "", err
		}
	}
	if values["timeout"] != "" {
		timeout, err := time.ParseDuration(values["timeout"])
		if err != nil || timeout <= 0 {
			attr := block.Body.Attributes["timeout"]
			return "", fmt.Errorf("atlas.hcl %s timeout %q at %s:%d is not a positive duration",
				key, values["timeout"], attr.NameRange.Filename, attr.NameRange.Start.Line)
		}
		declaration.ReadyTimeout = timeout
	}
	name, err := dockerDeclarationName(key, values, declaration)
	if err != nil {
		return "", err
	}
	devdocker.Declare(name, declaration)
	return dockerImageURL(key.typ, values["image"], values["database"], values["schema"], name), nil
}

// dockerBuild reads a docker block's build, resolving its context against the
// atlas.hcl directory as a relative data.external_schema working_dir is.
func (p atlasParser) dockerBuild(key atlasDockerBlockKey, block *hclsyntax.Block) (devdocker.Build, error) {
	for _, name := range sortedAttributeNames(block.Body.Attributes) {
		if !slices.Contains(atlasDockerBuildAttributes, name) {
			return devdocker.Build{}, unsupportedAttr(key.String()+".build."+name, block.Body.Attributes[name])
		}
	}
	if len(block.Body.Blocks) > 0 {
		return devdocker.Build{}, unsupportedBlock(block.Body.Blocks[0])
	}
	values, err := p.dockerStrings(block, "context", "dockerfile", "dockerfile_inline", "target", "platform")
	if err != nil {
		return devdocker.Build{}, err
	}
	if err := requireDockerValue(key.String()+".build", block, values, "context"); err != nil {
		return devdocker.Build{}, err
	}
	context, err := filepath.Abs(p.resolveExternalSchemaWorkingDir(values["context"]))
	if err != nil {
		return devdocker.Build{}, err
	}
	build := devdocker.Build{
		Context:          context,
		Dockerfile:       values["dockerfile"],
		DockerfileInline: values["dockerfile_inline"],
		Target:           values["target"],
		Platform:         values["platform"],
	}
	if attr, ok := block.Body.Attributes["args"]; ok {
		if build.Args, err = p.dockerStringMap("args", attr); err != nil {
			return devdocker.Build{}, err
		}
	}
	return build, nil
}

// requireDockerValue refuses block when its attribute name is absent or empty.
func requireDockerValue(scope string, block *hclsyntax.Block, values map[string]string, name string) error {
	attr, ok := block.Body.Attributes[name]
	if !ok {
		return fmt.Errorf("atlas.hcl %s at %s:%d requires %s",
			scope, block.TypeRange.Filename, block.TypeRange.Start.Line, name)
	}
	if values[name] == "" {
		return emptyValue(name, attr)
	}
	return nil
}

// dockerStrings evaluates the string attributes of block that names lists,
// leaving an absent one empty.
func (p atlasParser) dockerStrings(block *hclsyntax.Block, names ...string) (map[string]string, error) {
	values := make(map[string]string, len(names))
	for _, name := range names {
		attr, ok := block.Body.Attributes[name]
		if !ok {
			continue
		}
		value, err := p.stringAttr(name, attr)
		if err != nil {
			return nil, err
		}
		values[name] = value
	}
	return values, nil
}

// dockerStringMap evaluates a map of strings, such as build arguments.
func (p atlasParser) dockerStringMap(name string, attr *hclsyntax.Attribute) (map[string]string, error) {
	value, diags := attr.Expr.Value(p.ctx)
	if diags.HasErrors() {
		return nil, p.evaluationFailed(name, attr, diags)
	}
	if value.IsNull() || (!value.Type().IsObjectType() && !value.Type().IsMapType()) {
		return nil, wrongValueType(name, attr, "a map of strings")
	}
	result := make(map[string]string)
	for it := value.ElementIterator(); it.Next(); {
		key, element := it.Element()
		if element.IsNull() || element.Type() != cty.String {
			return nil, wrongValueType(name, attr, "a map of strings")
		}
		result[key.AsString()] = element.AsString()
	}
	return result, nil
}

// dockerDeclarationName names a declaration by its block and its content, so a
// block whose content differs between two loads in one process is not given
// the other load's declaration.
func dockerDeclarationName(key atlasDockerBlockKey, values map[string]string, declaration devdocker.Declaration) (string, error) {
	content, err := json.Marshal(struct {
		Values      map[string]string
		Declaration devdocker.Declaration
	}{values, declaration})
	if err != nil {
		return "", fmt.Errorf("record atlas.hcl %s: %w", key, err)
	}
	sum := sha256.Sum256(content)
	return key.String() + "." + hex.EncodeToString(sum[:6]), nil
}

// dockerImageURL is the `docker+<driver>://` URL a docker block's reference
// evaluates to. The image always carries a tag or a digest, so the URL's last
// path segment is never read as a database by mistake; the daemon resolves an
// untagged image to `latest` either way. On PostgreSQL the database is always
// written, `postgres` when the block names none, and the schema is the URL's
// search_path; on the MySQL family the schema is the database.
func dockerImageURL(typ, image, database, schema, name string) string {
	if last := image[strings.LastIndex(image, "/")+1:]; !strings.ContainsAny(last, ":@") {
		image += ":latest"
	}
	segments := []string{image}
	query := url.Values{}
	switch typ {
	case "postgres":
		if database == "" {
			database = "postgres"
		}
		segments = append(segments, database)
		if schema != "" {
			query.Set("search_path", schema)
		}
	default:
		if schema != "" {
			segments = append(segments, schema)
		}
	}
	return (&url.URL{
		Scheme:   "docker+" + typ,
		Host:     "_",
		Path:     "/" + strings.Join(segments, "/"),
		RawQuery: query.Encode(),
		Fragment: name,
	}).String()
}

// atlasDockerReference returns the block a docker traversal addresses.
func atlasDockerReference(traversal hcl.Traversal) (atlasDockerBlockKey, bool) {
	typ, typeOK := atlasTraversalAttribute(traversal, 1)
	name, nameOK := atlasTraversalAttribute(traversal, 2)
	return atlasDockerBlockKey{typ: typ, name: name}, typeOK && nameOK
}
