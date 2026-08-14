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
		commands, err := logicalDockerRunCommands(script)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, run.Line, err)
		}
		for _, command := range commands {
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

// logicalDockerRunCommands returns the logical `docker run` commands a step's
// run value executes. Physical lines join into one command while a backslash
// continues them or a quote holds them open, and the lines a heredoc feeds to
// a command are its payload: the shell passes them as data, so a `docker run`
// written inside one starts nothing and must not reach the inventory.
func logicalDockerRunCommands(script string) ([]string, error) {
	lines := strings.Split(script, "\n")
	// The trailing empty line ends a command a continuation left open at the
	// end of the script, as the end of its input ends one for the shell.
	lines = append(lines, "")
	var commands []string
	var command string
	var open bool
	for index := 0; index < len(lines); index++ {
		trimmed := strings.TrimSpace(lines[index])
		skipped := !open && (trimmed == "" || strings.HasPrefix(trimmed, "#"))
		switch {
		case skipped && command == "":
			continue
		case skipped:
			// A blank or comment line does not continue a command. The shell
			// removes the escaped newline before it reads this line, so a
			// continuation finds nothing or a comment here and ends; joining
			// the next physical command to it would hand Docker an operand no
			// shell ever passes.
		case open:
			// A newline inside a quote is data, so the line joins unchanged.
			command += "\n" + lines[index]
		case command == "":
			command = trimmed
		default:
			command += " " + trimmed
		}
		scan, err := scanShellCommand(command)
		if err != nil {
			return nil, err
		}
		open = scan.open
		if open {
			continue
		}
		command = strings.TrimSpace(command[:scan.end])
		if continued, ok := strings.CutSuffix(command, "\\"); ok {
			command = strings.TrimSpace(continued)
			continue
		}
		// A backslash-continued command opens its payloads after its last
		// physical line, because the shell removes the escaped newlines
		// before it reads the here-document.
		last, err := skipHeredocPayloads(lines, index+1, scan.heredocs)
		if err != nil {
			return nil, err
		}
		index = last
		fields := strings.Fields(command)
		if len(fields) >= 2 && fields[0] == "docker" && fields[1] == "run" {
			commands = append(commands, command)
		}
		command = ""
	}
	if command != "" {
		return nil, fmt.Errorf("shell command is never closed: %q", command)
	}
	return commands, nil
}

// skipHeredocPayloads returns the index of the last line consumed by the
// payloads a command opened, which is the line before its first command again.
// A heredoc whose terminator never arrives is refused rather than guessed at:
// treating the remainder of the script as payload would silently drop every
// command after it, and the workflow that wrote it would not run either.
func skipHeredocPayloads(lines []string, next int, heredocs []heredocRedirection) (int, error) {
	for _, heredoc := range heredocs {
		terminator := -1
		for index := next; index < len(lines); index++ {
			if !heredoc.terminates(lines[index]) {
				continue
			}
			terminator = index
			break
		}
		if terminator < 0 {
			return 0, fmt.Errorf("heredoc delimiter %q is never terminated", heredoc.delimiter)
		}
		next = terminator + 1
	}
	return next - 1, nil
}

// heredocRedirection is one here-document a logical command opens: the word
// that ends its payload, and whether `<<-` asked the shell to strip the leading
// tabs of every payload line, the terminator included.
type heredocRedirection struct {
	delimiter string
	stripTabs bool
}

// terminates reports whether a payload line ends the here-document. The shell
// compares the whole line with the delimiter, and `<<-` differs only in
// stripping leading tabs, so a line the shell keeps as payload -- a space-
// indented " EOF", say -- must not end the payload here either. Accepting one
// would resume scanning payload the shell never executes.
func (h heredocRedirection) terminates(line string) bool {
	if h.stripTabs {
		return strings.TrimLeft(line, "\t") == h.delimiter
	}
	return line == h.delimiter
}

// shellScan is the structure one logical command's text carries.
type shellScan struct {
	// heredocs are the here-documents the command opens, in the order their
	// payloads follow it.
	heredocs []heredocRedirection
	// end is where a comment begins, or the length of the text: the shell runs
	// only what precedes it.
	end int
	// open reports that the text does not end at a shell boundary, so the next
	// physical line belongs to this command whatever that line looks like.
	open bool
}

// scanShellCommand reads one logical command's text the way the shell does. A
// quoted span is data, an arithmetic expansion carries a left shift rather than
// a redirection, and a `#` that starts a word ends the executable text. What
// remains may open here-documents: a `<<` inside a quoted word is text and
// `<<<` is a herestring whose word travels on the same line, so neither opens a
// payload.
func scanShellCommand(command string) (shellScan, error) {
	scan := shellScan{end: len(command)}
	for index := 0; index < len(command); index++ {
		char := command[index]
		if char == '\\' {
			index++
			continue
		}
		if char == '\'' || char == '"' {
			closing := endOfQuotedSpan(command, index)
			if closing < 0 {
				// The span continues past this text, so every later line
				// belongs to it as data rather than as a command of its own.
				scan.open = true
				return scan, nil
			}
			index = closing
			continue
		}
		if char == '#' && (index == 0 || isShellSpace(command[index-1])) {
			// The rest of the line is a comment, so a `<<` in it redirects
			// nothing. Reading one would demand a terminator the workflow
			// never writes and refuse a step the shell runs happily.
			scan.end = index
			return scan, nil
		}
		if char == '$' && strings.HasPrefix(command[index+1:], "((") {
			closing := endOfArithmeticExpansion(command, index+1)
			if closing < 0 {
				// The expansion continues past this text, so the command does
				// not end here and neither does the arithmetic.
				scan.open = true
				return scan, nil
			}
			index = closing
			continue
		}
		if char != '<' || index+1 == len(command) || command[index+1] != '<' {
			continue
		}
		if index+2 < len(command) && command[index+2] == '<' {
			index += 2
			continue
		}
		heredoc, consumed := heredocRedirectionOf(command[index+2:])
		if heredoc.delimiter == "" {
			return shellScan{}, fmt.Errorf("heredoc redirection has no delimiter: %q", command)
		}
		scan.heredocs = append(scan.heredocs, heredoc)
		index += consumed + 1
	}
	return scan, nil
}

// endOfQuotedSpan returns the index of the quote that closes the span opened at
// start, or -1 when the text ends first. Inside double quotes a backslash
// escapes the byte after it; a single-quoted span ends at its next quote
// whatever it holds.
func endOfQuotedSpan(command string, start int) int {
	quote := command[start]
	for index := start + 1; index < len(command); index++ {
		if command[index] == '\\' && quote == '"' {
			index++
			continue
		}
		if command[index] == quote {
			return index
		}
	}
	return -1
}

// endOfArithmeticExpansion returns the index of the parenthesis that closes the
// `$((` expansion whose first parenthesis sits at start, or -1 when the text
// ends before it. Bash's left shift operator lives inside one, so a `<<` there
// redirects nothing.
func endOfArithmeticExpansion(command string, start int) int {
	depth := 0
	for index := start; index < len(command); index++ {
		switch command[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func isShellSpace(char byte) bool {
	return char == ' ' || char == '\t' || char == '\n'
}

// heredocRedirectionOf reads the here-document that follows a `<<` operator and
// reports how many bytes of the operator's remainder it consumed. Quoting a
// delimiter only suppresses expansion inside the payload, so the terminator is
// the word with its quoting removed either way.
func heredocRedirectionOf(rest string) (heredocRedirection, int) {
	consumed := 0
	heredoc := heredocRedirection{stripTabs: strings.HasPrefix(rest, "-")}
	if heredoc.stripTabs {
		consumed++
	}
	for consumed < len(rest) && (rest[consumed] == ' ' || rest[consumed] == '\t') {
		consumed++
	}
	word := rest[consumed:]
	if end := strings.IndexAny(word, " \t;&|<>()"); end >= 0 {
		word = word[:end]
	}
	consumed += len(word)
	for _, quoting := range []string{`\`, `'`, `"`} {
		word = strings.ReplaceAll(word, quoting, "")
	}
	heredoc.delimiter = word
	return heredoc, consumed
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
