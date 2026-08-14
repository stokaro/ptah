// Package workflowimagepins inventories container images that GitHub Actions
// workflows actually start.
package workflowimagepins

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/shlex"
	"go.yaml.in/yaml/v3"
)

var (
	booleanDockerRunOptions = []string{
		"-d", "-i", "-t", "--detach", "--init", "--interactive",
		"--privileged", "--read-only", "--rm", "--tty",
	}
	valueDockerRunOptions = []string{
		"-e", "-p", "-u", "-v", "-w", "--add-host", "--env",
		"--env-file", "--hostname", "--label", "--name", "--network",
		"--platform", "--publish", "--user", "--volume", "--workdir",
	}
)

// Directory returns the images started by YAML workflows directly below dir.
func Directory(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.y*ml"))
	if err != nil {
		return nil, fmt.Errorf("find workflows: %w", err)
	}
	slices.Sort(paths)
	var images []string
	for _, path := range paths {
		workflowImages, err := File(path)
		if err != nil {
			return nil, err
		}
		images = append(images, workflowImages...)
	}
	return images, nil
}

// File returns the images started by one GitHub Actions workflow.
func File(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read workflow %s: %w", path, err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("parse workflow %s: %w", path, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s: workflow root must be a mapping", path)
	}
	jobs, ok, err := mappingValue(document.Content[0], "jobs", path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nodeError(path, document.Content[0], "workflow has no jobs")
	}
	if jobs.Kind != yaml.MappingNode {
		return nil, nodeError(path, jobs, "jobs must be a mapping")
	}
	if err := validateUniqueMappingKeys(jobs, path); err != nil {
		return nil, err
	}

	var images []string
	for index := 0; index < len(jobs.Content); index += 2 {
		jobName := jobs.Content[index].Value
		job := jobs.Content[index+1]
		if job.Kind != yaml.MappingNode {
			return nil, nodeError(path, job, "job "+jobName+" must be a mapping")
		}
		jobImages, err := imagesFromJob(path, jobName, job)
		if err != nil {
			return nil, err
		}
		images = append(images, jobImages...)
	}
	return images, nil
}

func imagesFromJob(path, name string, job *yaml.Node) ([]string, error) {
	// A skipped job starts nothing: not its container, not its services and
	// not its steps. Inventorying them would let a dead job keep a stale
	// Compose pin green, which is the bypass this package exists to close.
	disabled, err := staticallyDisabled(path, job, "job "+name)
	if err != nil {
		return nil, err
	}
	if disabled {
		return nil, nil
	}
	var images []string
	container, ok, err := mappingValue(job, "container", path)
	if err != nil {
		return nil, err
	}
	if ok {
		image, err := containerImage(path, "job "+name+" container", container)
		if err != nil {
			return nil, err
		}
		images = append(images, image)
	}
	services, ok, err := mappingValue(job, "services", path)
	if err != nil {
		return nil, err
	}
	if ok {
		serviceImages, err := imagesFromServices(path, services)
		if err != nil {
			return nil, err
		}
		images = append(images, serviceImages...)
	}
	steps, ok, err := mappingValue(job, "steps", path)
	if err != nil {
		return nil, err
	}
	if ok {
		stepImages, err := imagesFromSteps(path, steps)
		if err != nil {
			return nil, err
		}
		images = append(images, stepImages...)
	}
	return images, nil
}

func containerImage(path, context string, node *yaml.Node) (string, error) {
	if node.Kind == yaml.ScalarNode {
		return nonemptyScalar(path, context+" image", node)
	}
	if node.Kind != yaml.MappingNode {
		return "", nodeError(path, node, context+" must be a string or mapping")
	}
	image, ok, err := mappingValue(node, "image", path)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nodeError(path, node, context+" has no image")
	}
	return nonemptyScalar(path, context+" image", image)
}

func imagesFromServices(path string, services *yaml.Node) ([]string, error) {
	if services.Kind != yaml.MappingNode {
		return nil, nodeError(path, services, "services must be a mapping")
	}
	if err := validateUniqueMappingKeys(services, path); err != nil {
		return nil, err
	}
	images := make([]string, 0, len(services.Content)/2)
	for index := 0; index < len(services.Content); index += 2 {
		name := services.Content[index].Value
		service := services.Content[index+1]
		if service.Kind != yaml.MappingNode {
			return nil, nodeError(path, service, "service "+name+" must be a mapping")
		}
		imageNode, ok, err := mappingValue(service, "image", path)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nodeError(path, service, "service "+name+" has no image")
		}
		image, err := nonemptyScalar(path, "service "+name+" image", imageNode)
		if err != nil {
			return nil, err
		}
		images = append(images, image)
	}
	return images, nil
}

func imagesFromSteps(path string, steps *yaml.Node) ([]string, error) {
	if steps.Kind != yaml.SequenceNode {
		return nil, nodeError(path, steps, "steps must be a sequence")
	}
	var images []string
	for _, step := range steps.Content {
		if step.Kind != yaml.MappingNode {
			return nil, nodeError(path, step, "step must be a mapping")
		}
		disabled, err := staticallyDisabled(path, step, "step")
		if err != nil {
			return nil, err
		}
		if disabled {
			continue
		}
		run, ok, err := mappingValue(step, "run", path)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		script, err := nonemptyScalar(path, "run", run)
		if err != nil {
			return nil, err
		}
		for command := range logicalDockerRunCommands(script) {
			image, err := dockerRunImage(command, path, run.Line)
			if err != nil {
				return nil, err
			}
			images = append(images, image)
		}
	}
	return images, nil
}

// staticallyDisabled reports whether the if condition of a job or a step is
// provably false. Both carry the same condition grammar and GitHub Actions
// skips both the same way, so one reader answers for both; a dynamic condition
// stays inventoried because nothing here can decide it.
func staticallyDisabled(path string, node *yaml.Node, context string) (bool, error) {
	condition, ok, err := mappingValue(node, "if", path)
	if err != nil || !ok {
		return false, err
	}
	if condition.Kind != yaml.ScalarNode {
		return false, nodeError(path, condition, context+" if must be a scalar")
	}
	if condition.Tag == "!!bool" {
		return condition.Value == "false", nil
	}
	value := strings.TrimSpace(condition.Value)
	if value == "false" {
		return true, nil
	}
	inner, wrapped := strings.CutPrefix(value, "${{")
	if !wrapped {
		return false, nil
	}
	inner, wrapped = strings.CutSuffix(inner, "}}")
	return wrapped && strings.TrimSpace(inner) == "false", nil
}

func logicalDockerRunCommands(script string) func(func(string) bool) {
	return func(yield func(string) bool) {
		var command string
		for line := range strings.SplitSeq(script, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			if command != "" {
				command += " "
			}
			command += trimmed
			if continued, ok := strings.CutSuffix(command, "\\"); ok {
				command = strings.TrimSpace(continued)
				continue
			}
			fields := strings.Fields(command)
			if len(fields) >= 2 && fields[0] == "docker" && fields[1] == "run" {
				if !yield(command) {
					return
				}
			}
			command = ""
		}
	}
}

func dockerRunImage(command, path string, line int) (string, error) {
	tokens, err := shlex.Split(command)
	if err != nil {
		return "", fmt.Errorf("%s:%d: parse docker run command: %w", path, line, err)
	}
	index := 2
	for index < len(tokens) {
		token := tokens[index]
		if token == "--" {
			index++
			break
		}
		if !strings.HasPrefix(token, "-") {
			break
		}
		name, _, attached := strings.Cut(token, "=")
		switch {
		case slices.Contains(booleanDockerRunOptions, name):
			if attached {
				return "", fmt.Errorf("%s:%d: boolean docker run option cannot carry a value: %q", path, line, token)
			}
			index++
		case combinedBooleanDockerRunOption(name):
			if attached {
				return "", fmt.Errorf("%s:%d: boolean docker run option cannot carry a value: %q", path, line, token)
			}
			index++
		case slices.Contains(valueDockerRunOptions, name):
			if attached {
				index++
				continue
			}
			if index+1 >= len(tokens) {
				return "", fmt.Errorf("%s:%d: docker run option %q has no value", path, line, token)
			}
			index += 2
		default:
			return "", fmt.Errorf("%s:%d: unsupported docker run option %q", path, line, token)
		}
	}
	if index >= len(tokens) {
		return "", fmt.Errorf("%s:%d: docker run command has no image operand", path, line)
	}
	return tokens[index], nil
}

func combinedBooleanDockerRunOption(name string) bool {
	if len(name) < 3 || name[0] != '-' || name[1] == '-' {
		return false
	}
	for _, option := range name[1:] {
		if !strings.ContainsRune("dit", option) {
			return false
		}
	}
	return true
}

func mappingValue(mapping *yaml.Node, key, path string) (*yaml.Node, bool, error) {
	var found *yaml.Node
	for index := 0; index < len(mapping.Content); index += 2 {
		candidate := mapping.Content[index]
		if candidate.Value == "<<" {
			return nil, false, nodeError(path, candidate, "YAML merge keys are not supported")
		}
		if candidate.Kind != yaml.ScalarNode || candidate.Value != key {
			continue
		}
		if found != nil {
			return nil, false, nodeError(path, candidate, "duplicate "+key+" key")
		}
		found = mapping.Content[index+1]
	}
	return found, found != nil, nil
}

func validateUniqueMappingKeys(mapping *yaml.Node, path string) error {
	seen := make(map[string]struct{}, len(mapping.Content)/2)
	for index := 0; index < len(mapping.Content); index += 2 {
		key := mapping.Content[index]
		if key.Kind != yaml.ScalarNode {
			return nodeError(path, key, "mapping key must be a scalar")
		}
		if key.Value == "<<" {
			return nodeError(path, key, "YAML merge keys are not supported")
		}
		if _, ok := seen[key.Value]; ok {
			return nodeError(path, key, "duplicate "+key.Value+" key")
		}
		seen[key.Value] = struct{}{}
	}
	return nil
}

func nonemptyScalar(path, context string, node *yaml.Node) (string, error) {
	if node.Kind != yaml.ScalarNode || strings.TrimSpace(node.Value) == "" {
		return "", nodeError(path, node, context+" must be a nonempty string")
	}
	return node.Value, nil
}

func nodeError(path string, node *yaml.Node, message string) error {
	return fmt.Errorf("%s:%d: %s", path, node.Line, message)
}
