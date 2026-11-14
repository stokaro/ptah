package atlasmigrateimport

import (
	"strings"
)

// SourceLines maps each line of the entry's converted SQL to the line of its
// source file it was read from: element i is the 1-based source line of
// converted line i+1. It is nil for an entry with no source file.
//
// A conversion keeps the lines it reads in source order and changes nothing
// inside them but the whitespace around the whole body. It drops lines -- blank
// separators, section and changeset directives, the down section -- and it may
// write some, such as the Atlas transaction directive that stands for a source
// directive and the blank line after it. So each converted line with text is
// matched, by its trimmed text, to the first source line after the previous
// match that reads the same. A blank converted line takes the first blank
// source line between the matches around it, so a blank line the conversion
// wrote cannot take one further down and pull the matches after it past their
// statements. A line nothing matches takes the line of the next converted line
// that matched, since it stands at the start of what follows it, or the last
// source line where nothing follows.
func (e Entry) SourceLines() []int {
	if e.Source == "" {
		return nil
	}
	converted := splitLines(string(e.Data))
	source := splitLines(string(e.SourceData))
	lines := make([]int, len(converted))
	next := 0
	for i, line := range converted {
		want := strings.TrimSpace(line)
		if want == "" {
			continue
		}
		for j := next; j < len(source); j++ {
			if strings.TrimSpace(source[j]) == want {
				lines[i] = j + 1
				next = j + 1
				break
			}
		}
	}
	previous := 0
	for i, line := range converted {
		if lines[i] != 0 {
			previous = lines[i]
			continue
		}
		if strings.TrimSpace(line) != "" {
			continue
		}
		limit := followingMatch(lines, i, len(source)+1)
		for j := previous; j+1 < limit; j++ {
			if strings.TrimSpace(source[j]) == "" {
				lines[i] = j + 1
				previous = j + 1
				break
			}
		}
	}
	following := len(source)
	for i := len(lines) - 1; i >= 0; i-- {
		if lines[i] == 0 {
			lines[i] = max(following, 1)
			continue
		}
		following = lines[i]
	}
	return lines
}

// followingMatch is the source line of the first matched converted line after
// index i, or end where none is matched.
func followingMatch(lines []int, i, end int) int {
	for _, line := range lines[i+1:] {
		if line != 0 {
			return line
		}
	}
	return end
}

// splitLines splits text into lines without their line breaks, leaving out
// the empty line after a final newline.
func splitLines(text string) []string {
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}
	return lines
}
