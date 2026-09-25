//go:build !js

package assist

// White-box testing required: the approval arrives at the interactive surface
// as an unexported message, and the model that draws it is unexported too.
// Driving the built binary under a pseudo-terminal would reach the same view,
// but it cannot set the terminal's width per case, which is the input here.

import (
	"regexp"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	qt "github.com/frankban/quicktest"
)

// approvalMessage has the shape the server sends: two digests longer than a
// narrow line, and the closing sentence the old one-line title lost first.
var approvalMessage = "Apply patch to migrations: add 1700000900_x.up.sql and 1700000900_x.down.sql. " +
	"Base digest sha256:" + strings.Repeat("a", 64) + " result digest sha256:" + strings.Repeat("b", 64) +
	". This approval covers exactly the patch previewed as p-123 and nothing else."

var ansiSequence = regexp.MustCompile(`\x1b\[[0-9;:?]*[A-Za-z]`)

// flat is text with styling and every kind of break removed, so a message
// wrapped at any width compares with the message it came from.
func flat(text string) string {
	return strings.Join(strings.Fields(ansiSequence.ReplaceAllString(text, "")), "")
}

// widest is the widest line of a view, in terminal cells.
func widest(view string) int {
	width := 0
	for line := range strings.SplitSeq(view, "\n") {
		width = max(width, lipgloss.Width(line))
	}
	return width
}

// approvalView opens an approval on a terminal of the given width and returns
// what the surface draws.
func approvalView(width int) string {
	model := newTUIModel(nil, false)
	model.Update(tea.WindowSizeMsg{Width: width, Height: 40})
	model.Update(&approvalRequest{message: approvalMessage, reply: make(chan string, 1)})
	return model.View().Content
}

// TestApprovalView_FitsTheTerminalAndKeepsTheWholeRequest pins the approval
// prompt to the terminal it is drawn on. It was drawn 80 columns wide whatever
// the terminal was, with the request as a one-line title cut at the edge, so
// the digests and the sentence saying what the approval covers never reached
// the person deciding.
func TestApprovalView_FitsTheTerminalAndKeepsTheWholeRequest(t *testing.T) {
	tests := []struct {
		name  string
		width int
	}{
		{name: "narrower than a digest line", width: 60},
		{name: "wider than 80 columns", width: 160},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			view := approvalView(test.width)
			c.Assert(widest(view) <= test.width, qt.IsTrue,
				qt.Commentf("widest line %d at width %d:\n%s", widest(view), test.width, view))
			c.Assert(flat(view), qt.Contains, flat(approvalMessage))
		})
	}
}

// TestApprovalView_FollowsAResize checks a window resized while the prompt is
// open: the form learns sizes only from the messages it is handed, so a
// resize the surface kept to itself left it at the old width.
func TestApprovalView_FollowsAResize(t *testing.T) {
	c := qt.New(t)
	model := newTUIModel(nil, false)
	model.Update(tea.WindowSizeMsg{Width: 160, Height: 40})
	model.Update(&approvalRequest{message: approvalMessage, reply: make(chan string, 1)})

	model.Update(tea.WindowSizeMsg{Width: 60, Height: 40})

	view := model.View().Content
	c.Assert(widest(view) <= 60, qt.IsTrue, qt.Commentf("widest line %d after resizing to 60:\n%s", widest(view), view))
	c.Assert(flat(view), qt.Contains, flat(approvalMessage))
}
