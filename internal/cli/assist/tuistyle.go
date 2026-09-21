package assist

import (
	"strings"

	"charm.land/glamour/v2"
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

// dim renders a whole block in the muted color, line by line, so a block that
// is queued for the scrollback keeps its color when the terminal re-wraps it.
func dim(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, footerStyle.Render(line))
	}
	return out
}

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
func renderAnswer(markdown string) []string {
	if strings.TrimSpace(markdown) == "" {
		return nil
	}
	renderer, err := glamour.NewTermRenderer(
		glamour.WithStandardStyle("dark"),
		glamour.WithWordWrap(0),
	)
	if err != nil {
		return strings.Split(strings.TrimRight(markdown, "\n"), "\n")
	}
	rendered, err := renderer.Render(markdown)
	if err != nil {
		return strings.Split(strings.TrimRight(markdown, "\n"), "\n")
	}
	// Glamour opens with a blank line and closes with one; the surface puts
	// its own spacing around a block, so both would double it.
	return strings.Split(strings.Trim(rendered, "\n"), "\n")
}
