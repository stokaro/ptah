package docsindex

import (
	"strings"
)

// Passage is one heading-scoped span of a document: the smallest unit an answer
// can cite. A whole page spends a model's context on the part it did not ask
// about, and a bare paragraph gives a person nothing to check it against, so a
// passage carries both its text and where in the document it sits.
type Passage struct {
	// Path is the document's repository-relative path.
	Path string
	// Heading is the trail of headings the passage sits under, joined with
	// " > ". It is empty for text above the first heading.
	Heading string
	// Text is the passage body with its heading line removed.
	Text string
}

// splitPassages cuts a markdown document at its ATX headings.
//
// Three things in a markdown file look like a heading and are not, and each one
// produced a passage boundary in the wrong place before this handled them: a
// `#` inside a fenced code block (every shell transcript in this repository
// starts a comment that way), the `---` frontmatter block every site page opens
// with, and a `#` that is not followed by a space, which is a fragment link or
// an issue reference rather than a heading.
func splitPassages(path, content string) []Passage {
	var passages []Passage
	lines := strings.Split(content, "\n")
	lines = dropFrontmatter(lines)

	var stack []string
	var body []string
	heading := ""
	inFence := false

	flush := func() {
		text := strings.TrimSpace(strings.Join(body, "\n"))
		body = body[:0]
		if text == "" {
			return
		}
		passages = append(passages, Passage{Path: path, Heading: heading, Text: text})
	}

	for _, line := range lines {
		if isFenceDelimiter(line) {
			inFence = !inFence
			body = append(body, line)
			continue
		}
		level, title, ok := headingOf(line)
		if inFence || !ok {
			body = append(body, line)
			continue
		}
		flush()
		stack = pushHeading(stack, level, title)
		heading = strings.Join(stack, " > ")
	}
	flush()
	return passages
}

// dropFrontmatter removes the YAML block a Starlight page opens with. Its keys
// are document metadata, not prose, and indexing them made every page match the
// word "title".
func dropFrontmatter(lines []string) []string {
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return lines
	}
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			return lines[i+1:]
		}
	}
	return lines
}

func isFenceDelimiter(line string) bool {
	trimmed := strings.TrimSpace(line)
	return strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~")
}

// headingOf reads an ATX heading, reporting its level and title.
//
// The space after the hashes is required: `#2123` is an issue reference and
// `#usage` is a fragment link, and both appear in this repository's prose.
func headingOf(line string) (int, string, bool) {
	if !strings.HasPrefix(line, "#") {
		return 0, "", false
	}
	level := len(line) - len(strings.TrimLeft(line, "#"))
	if level > 6 || len(line) == level || line[level] != ' ' {
		return 0, "", false
	}
	title := strings.TrimSpace(strings.Trim(line[level:], "#"))
	if title == "" {
		return 0, "", false
	}
	return level, title, true
}

// pushHeading replaces the trail from the given level down, so a level-2
// heading following a level-3 one drops the deeper entry rather than nesting
// under it.
func pushHeading(stack []string, level int, title string) []string {
	if level > len(stack) {
		return append(stack, title)
	}
	stack = stack[:level-1]
	return append(stack, title)
}
