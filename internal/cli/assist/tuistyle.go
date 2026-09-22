//go:build !js

package assist

import (
	"regexp"
	"strings"

	"charm.land/glamour/v2"
	"charm.land/glamour/v2/ansi"
	"charm.land/glamour/v2/styles"
	"charm.land/lipgloss/v2"
)

// The colors the interactive surface uses.
//
// The vocabulary is Ptah's own, the one the documentation and ptah.run carry:
// one accent for what the program is saying about itself, amber reserved for
// something the reader has to act on, and a muted gray for what is read beside
// the text rather than as text. Nothing else is colored, so the model's answer
// stays the plain foreground and is still readable when it is pasted somewhere
// that has no color at all.
//
// Every color is an ANSI 256 index rather than a hex value. A hex color is
// rendered by lipgloss as a truecolor escape, which a terminal that has been
// themed for a light background renders at the brightness the theme was never
// asked about; an indexed color is resolved by the terminal's own palette, so
// it follows whatever the reader has already chosen. lipgloss degrades an
// index to the nearest basic color where the profile has fewer.
var (
	// The accent: the prompt marker, and the name of a tool that ran.
	accent = lipgloss.Color("39")
	// Read beside the text: the footer, the trace, the hint under the spinner.
	muted = lipgloss.Color("245")
	// Reserved for what has to be acted on: a refusal, a failure, an approval.
	amber = lipgloss.Color("214")
	// A question, echoed back above its answer.
	echoed = lipgloss.Color("252")
)

var (
	// logoStyle and subtitleStyle draw the heading the session opens on. The
	// wordmark takes the accent and the line under it the text color, so the
	// screen leads with the name rather than with the paths below it.
	logoStyle     = lipgloss.NewStyle().Foreground(accent)
	subtitleStyle = lipgloss.NewStyle().Foreground(echoed).Bold(true)
	// promptStyle draws the `>` that marks where typing goes.
	promptStyle = lipgloss.NewStyle().Foreground(accent).Bold(true)
	// echoStyle draws the question above the answer it produced.
	echoStyle = lipgloss.NewStyle().Foreground(echoed)
	// footerStyle draws the provenance lines, which are about the run rather
	// than part of the answer.
	footerStyle = lipgloss.NewStyle().Foreground(muted)
	// traceOKStyle and traceFailStyle draw one tool call each.
	traceOKStyle   = lipgloss.NewStyle().Foreground(accent)
	traceFailStyle = lipgloss.NewStyle().Foreground(amber)
	// noticeStyle draws what the surface itself says: canceled, declined, a
	// directive that is not one.
	noticeStyle = lipgloss.NewStyle().Foreground(amber)
	// spinnerStyle draws the spinner while the model is working.
	spinnerStyle = lipgloss.NewStyle().Foreground(accent)
	// hintStyle draws the line under the spinner that says how to get out.
	hintStyle = lipgloss.NewStyle().Foreground(muted)
	// unverifiedStyle draws the one footer line that is a warning rather than
	// a fact: that no tool answered, so nothing was checked.
	unverifiedStyle = lipgloss.NewStyle().Foreground(amber)
)

// promptMarker is what the textarea draws to the left of each line: the marker
// on the first, and an indent of the same width on the rest, so a question that
// wraps or spans lines stays aligned under itself.
//
// It is given to the component rather than printed in front of it. The cursor
// is positioned by the component in its own coordinates, so a marker this file
// drew separately would leave the cursor two columns to the left of the text --
// on an empty prompt it sat on the marker and hid it.
func promptMarker(line int) string {
	if line == 0 {
		return promptStyle.Render("> ")
	}
	return "  "
}

// promptWidth is the width promptMarker occupies, which the component needs in
// order to wrap and to place the cursor. It is the rendered width rather than
// the byte length, so the styling cannot change it.
var promptWidth = lipgloss.Width(promptMarker(0))

// blank reports whether a rendered line shows nothing: the renderer pads with
// indent and a colour reset, so an empty line is rarely an empty string.
func blank(line string) bool {
	return strings.TrimSpace(ansiPattern.ReplaceAllString(line, "")) == ""
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[A-Za-z]`)

// trimBlank drops the blank lines a render begins and ends with.
func trimBlank(lines []string) []string {
	start, end := 0, len(lines)
	for start < end && blank(lines[start]) {
		start++
	}
	for end > start && blank(lines[end-1]) {
		end--
	}
	return lines[start:end]
}

// dim renders a whole block in the muted color, line by line, so a block that
// is queued for the scrollback keeps its color when the terminal re-wraps it.
func dim(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, footerStyle.Render(line))
	}
	return out
}

// answerStyle is the dark style with one change: inline code is padded with an
// ordinary space rather than a non-breaking one.
//
// The upstream style uses U+00A0 to keep a line break out of the middle of a
// short code span. Wrapping is off here, so nothing is going to break the line
// anyway, and the character costs more than it buys: a terminal or a font
// without it draws a replacement glyph on both sides of every piece of inline
// code, which is most of what an answer about a schema contains.
func answerStyle() ansi.StyleConfig {
	style := styles.DarkStyleConfig
	style.Code.Prefix = " "
	style.Code.Suffix = " "
	return style
}

// answerWidth is how wide an answer may be rendered.
//
// A terminal that has not reported its size yet reads as zero, and so does a
// pipe. Both want the fallback rather than a wrap at nothing, which glamour
// reads as "do not wrap".
func answerWidth(width int) int {
	if width < minAnswerWidth {
		return defaultAnswerWidth
	}
	return width
}

const (
	minAnswerWidth     = 20
	defaultAnswerWidth = 96
)

// renderAnswer turns the model's Markdown into what a terminal shows: bold is
// bold, a list is bullets, a fenced block is syntax-highlighted.
//
// The answer arrives as Markdown -- every model writes it -- and a terminal
// that prints it verbatim shows `**What is configured**` with the asterisks,
// which is the one place this surface was harder to read than the chat window
// the same model is used from.
//
// Word wrapping is off, deliberately, and that is the whole reason this is
// configured rather than taken as it comes. Glamour pads every wrapped line
// out to the wrap width with styled spaces: the same short answer measured
// 3139 bytes with wrapping at 60 columns and 154 bytes with wrapping off, for
// identical styling. Off, the terminal wraps instead, which also means a
// window resized after the answer was printed reflows it rather than leaving
// it hard-wrapped to a width nobody has any more.
//
// A failure returns the Markdown unchanged. An answer that cannot be styled is
// still the answer, and losing it to a rendering error would be the worse
// outcome by a wide margin.
func renderAnswer(markdown string, width int) []string {
	if strings.TrimSpace(markdown) == "" {
		return nil
	}
	// Wrapped to the terminal rather than left to it. With wrapping off the
	// renderer emits lines longer than the screen, and what happens to them
	// depends on where they go: the scrollback lets the terminal break them,
	// which it does mid-word, and the live view is clipped to the width, which
	// loses characters off a line the reader is watching arrive.
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStyles(answerStyle()),
		glamour.WithWordWrap(answerWidth(width)),
	)
	if err != nil {
		return strings.Split(strings.TrimRight(markdown, "\n"), "\n")
	}
	rendered, err := renderer.Render(markdown)
	if err != nil {
		return strings.Split(strings.TrimRight(markdown, "\n"), "\n")
	}
	// Glamour separates the blocks inside one render, but a streamed answer is
	// rendered in several: a block that has settled is printed before the next
	// one exists. So the spacing between two renders is this function's, and
	// without it a heading sat on the line under the paragraph above it and
	// the whole answer read as one wall.
	//
	// Exactly one blank line, which means trimming what the renderer already
	// put at each end first. Those lines are not empty strings: they carry
	// indent and a colour reset, so they have to be recognised by what they
	// show rather than by their length.
	return append(trimBlank(strings.Split(rendered, "\n")), "")
}

// renderProse renders lines that are part of a block still being written.
//
// Same renderer, without the blank line [renderAnswer] puts after a finished
// block: these lines continue a paragraph that has not ended, and a blank
// after each one would turn every line of it into a paragraph of its own.
func renderProse(markdown string, width int) []string {
	rendered := renderAnswer(markdown, width)
	return trimBlank(rendered)
}

// splitRenderable divides streamed Markdown into the part that can be rendered
// and printed now, and the part that has to wait for more text.
//
// This is what makes the answer appear as it arrives rather than all at once
// when it finishes. Rendering the whole buffer on every fragment would do the
// same thing on screen and cost too much to do: a short answer renders in
// about a millisecond, but 8 KB takes 30, and at that size every frame would
// miss. Flushing what is settled keeps the live part small, so the cost does
// not grow with the answer.
//
// A cut is safe at a blank line, which is what separates one Markdown block
// from the next -- but only outside a fenced code block, where a blank line is
// just a blank line, and only where it does not split a list. A list whose
// items are separated by blank lines is one block: cutting inside it restarts
// the numbering at 1 in the second half.
//
// Returns empty `settled` when nothing can be cut yet, in which case the
// caller keeps the whole buffer live.
func splitRenderable(pending string) (settled, rest string) {
	lines := strings.Split(pending, "\n")
	fenced := false
	cut := -1

	// The last element is not a line of the document when the buffer ends with
	// a newline: `strings.Split("row\n", "\n")` is `["row", ""]`, and that
	// empty string is an artifact of splitting rather than a blank line. Read
	// as one, it ends a block every time a line is completed -- which flushed a
	// table one row at a time, and a single row with no header renders as
	// literal pipes. A blank line only separates blocks when something follows
	// it, so the final element is never a candidate.
	for i, line := range lines[:max(0, len(lines)-1)] {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			fenced = !fenced
			continue
		}
		if fenced || trimmed != "" {
			continue
		}
		// A blank line at the top level. It ends a block unless the blocks on
		// both sides of it are list items, which is one loose list.
		if listContinues(lines, i) {
			continue
		}
		// The first such line, not the last. Cutting at the last one holds
		// every completed block until the one after it completes too, so a
		// four-block answer reaches the screen in two arrivals and reads as a
		// dump rather than as text being written. Cutting at the first sends
		// each block on as it finishes.
		cut = i
		break
	}

	if cut < 0 {
		return "", pending
	}
	return strings.Join(lines[:cut], "\n"), strings.Join(lines[cut+1:], "\n")
}

// splitProse divides streamed Markdown after the last completed line, when
// every line in it is ordinary prose.
//
// This is what puts an answer on the screen as it is written rather than a
// block at a time. Waiting for a blank line means a paragraph appears all at
// once when it ends, and showing the unfinished part in a pane of its own
// means the same text is drawn twice in two places -- once where the pane is
// and again, laid out differently, where the scrollback puts it. The jump
// between the two is the thing a reader notices.
//
// Only prose. A list item rendered on its own is a one-item list, and an
// ordered one restarts at 1; a table row without its header renders as literal
// pipes; a line inside a fence is not Markdown at all. Those wait for
// [splitRenderable] to find the end of their block.
//
// Returns empty `settled` when the buffer holds no completed prose line.
func splitProse(pending string) (settled, rest string) {
	lines := strings.Split(pending, "\n")
	// The last element is what has arrived of the line still being written, so
	// it is never complete. With nothing before it there is nothing to send.
	complete := lines[:max(0, len(lines)-1)]
	if len(complete) == 0 {
		return "", pending
	}
	for _, line := range complete {
		if !isProse(line) && !isBulletItem(line) {
			return "", pending
		}
	}
	return strings.Join(complete, "\n"), lines[len(lines)-1]
}

// isBulletItem reports whether a line is an item of an unordered list.
//
// Unordered ones can be sent on as they finish. Rendering `- one` by itself
// produces the same line as rendering it inside its list, because nothing about
// a bullet depends on the items around it. An ordered item does: rendered
// alone, `2. second` comes out as `1.`, so those wait for the end of the block.
//
// This is what makes a streamed answer stream at all. A model asked for three
// bullets writes a tight list with no blank line in it until the list ends, so
// a rule that only cuts at blank lines has nothing to cut until the answer is
// finished -- which is why an answer that took eight seconds to write appeared
// in one piece at the end of them.
func isBulletItem(line string) bool {
	trimmed := strings.TrimSpace(line)
	for _, marker := range []string{"- ", "* ", "+ ", "• "} {
		if strings.HasPrefix(trimmed, marker) {
			return true
		}
	}
	return false
}

// isProse reports whether a line can be rendered without the lines around it.
func isProse(line string) bool {
	trimmed := strings.TrimSpace(line)
	switch {
	case trimmed == "":
		return false
	case strings.HasPrefix(trimmed, "```"), strings.HasPrefix(trimmed, "~~~"):
		return false
	case strings.HasPrefix(trimmed, "|"):
		return false
	case strings.HasPrefix(trimmed, ">"):
		return false
	case isListItem(trimmed):
		return false
	}
	return true
}

// listContinues reports whether the blank line at `at` sits inside one list
// rather than between two blocks.
func listContinues(lines []string, at int) bool {
	return isListItem(previousNonEmpty(lines, at)) && isListItem(nextNonEmpty(lines, at))
}

func previousNonEmpty(lines []string, at int) string {
	for i := at - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return lines[i]
		}
	}
	return ""
}

func nextNonEmpty(lines []string, at int) string {
	for i := at + 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != "" {
			return lines[i]
		}
	}
	return ""
}

// isListItem recognizes the bullet and ordered markers a model writes. An
// indented continuation of an item counts too, so a wrapped item does not read
// as the end of the list.
func isListItem(line string) bool {
	if line == "" {
		return false
	}
	if strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "\t") {
		return true
	}
	trimmed := strings.TrimSpace(line)
	for _, marker := range []string{"- ", "* ", "+ "} {
		if strings.HasPrefix(trimmed, marker) {
			return true
		}
	}
	digits := 0
	for digits < len(trimmed) && trimmed[digits] >= '0' && trimmed[digits] <= '9' {
		digits++
	}
	return digits > 0 && digits < len(trimmed) &&
		(trimmed[digits] == '.' || trimmed[digits] == ')')
}
