package gotoolchain

import (
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

// setupGo is the action a step has to be using for this gate to judge it.
const setupGo = "actions/setup-go@"

// ParseManifest reads one workflow or action file.
//
// It walks the YAML node tree rather than the text, so a quoted key, a quoted
// value, a folded scalar and a flow mapping are all just values. Every node
// carries its line, which is what a GitHub annotation needs.
func ParseManifest(root, path string) (Manifest, error) {
	body, err := os.ReadFile(root + "/" + path) //#nosec G304 -- a path this repository's own checker enumerated
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{Path: path, Inputs: make(map[string]Input)}
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.Contains(strings.ToLower(line), setupGo) {
			manifest.Mentions++
		}
	}

	var document yaml.Node
	if err := yaml.Unmarshal(body, &document); err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", path, err)
	}
	walk(&document, &manifest)
	readInputs(&document, &manifest)
	return manifest, nil
}

// walk finds every mapping whose `uses` names setup-go.
func walk(node *yaml.Node, manifest *Manifest) {
	if node == nil {
		return
	}
	if node.Kind == yaml.MappingNode {
		if uses, _ := field(node, "uses"); uses != nil && strings.Contains(strings.ToLower(uses.Value), setupGo) {
			manifest.Steps = append(manifest.Steps, readStep(node, uses, manifest.Path))
		}
	}
	for _, child := range node.Content {
		walk(child, manifest)
	}
}

// readStep reads the two version keys out of a step's `with` mapping.
func readStep(step, uses *yaml.Node, path string) SetupGoStep {
	found := SetupGoStep{File: path, Line: uses.Line}
	with, _ := field(step, "with")
	if with == nil {
		return found
	}
	if version, _ := field(with, "go-version"); version != nil {
		found.HasVersion, found.Version, found.VersionLine = true, version.Value, version.Line
	}
	if file, _ := field(with, "go-version-file"); file != nil {
		found.HasVersionFile, found.VersionFile, found.VersionFileLine = true, file.Value, file.Line
	}
	return found
}

// readInputs reads a composite action's `inputs` mapping.
func readInputs(document *yaml.Node, manifest *Manifest) {
	if len(document.Content) == 0 {
		return
	}
	inputs, _ := field(document.Content[0], "inputs")
	if inputs == nil || inputs.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(inputs.Content); i += 2 {
		key, value := inputs.Content[i], inputs.Content[i+1]
		input := Input{Name: key.Value, DeclaredLine: key.Line}
		if def, _ := field(value, "default"); def != nil {
			input.HasDefault, input.Default, input.DefaultLine = true, def.Value, def.Line
		}
		manifest.Inputs[key.Value] = input
	}
}

// field returns a mapping's value for one key, and the key node.
//
// The key is compared after the parser has removed its quoting, which is what
// a hand-written scan has to reimplement and gets wrong: `uses:` and `"uses":`
// are one key.
func field(node *yaml.Node, name string) (value, key *yaml.Node) {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == name {
			return node.Content[i+1], node.Content[i]
		}
	}
	return nil, nil
}

// Document is a parsed YAML file kept as nodes, for the checks that read a key
// this gate does not model as a step or an input.
type Document struct {
	Path string
	Root *yaml.Node
}

// ParseManifestFile parses one YAML file by absolute path.
func ParseManifestFile(path string) (Document, error) {
	body, err := os.ReadFile(path) //#nosec G304 -- a fixed path in this repository
	if err != nil {
		return Document{}, err
	}
	var document yaml.Node
	if err := yaml.Unmarshal(body, &document); err != nil {
		return Document{}, fmt.Errorf("%s: %w", path, err)
	}
	return Document{Path: path, Root: &document}, nil
}

// runGo returns golangci-lint's `run.go` value and its line.
func (d Document) runGo() (string, int) {
	if d.Root == nil || len(d.Root.Content) == 0 {
		return "", 0
	}
	run, _ := field(d.Root.Content[0], "run")
	value, _ := field(run, "go")
	if value == nil {
		return "", 0
	}
	return value.Value, value.Line
}
