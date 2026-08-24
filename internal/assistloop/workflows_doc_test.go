package assistloop_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/assistloop"
)

// TestWorkflowsDocumentQuotesTheRunLimits binds the table in
// docs/agent-workflows.md to the constants it describes.
//
// The document names every bound a run can hit and what each one is for. A
// number quoted in prose is stale the moment the constant moves, and a reader
// checking the code against the intent would be checking it against the wrong
// intent.
func TestWorkflowsDocumentQuotesTheRunLimits(t *testing.T) {
	tests := []struct {
		name  string
		quote string
	}{
		{name: "turns", quote: fmt.Sprintf("| turn limit | %d turns |", assistloop.DefaultMaxTurns)},
		{name: "tool calls", quote: fmt.Sprintf("| tool call limit | %d calls |", assistloop.DefaultMaxToolCalls)},
		{name: "repeats", quote: fmt.Sprintf("| repeated tool call | %d identical calls |", assistloop.DefaultMaxRepeats)},
		{
			name:  "tool output",
			quote: fmt.Sprintf("| output limit | %d KiB per tool result |", assistloop.DefaultMaxToolOutputBytes>>10),
		},
	}

	document := workflowsDocument(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(document, qt.Contains, tt.quote)
		})
	}
}

// TestWorkflowsDocumentNamesEveryStopReason pins the other direction: a run can
// end for five reasons and the table has to carry all five, so a new one cannot
// be added without the document noticing.
func TestWorkflowsDocumentNamesEveryStopReason(t *testing.T) {
	reasons := []assistloop.StopReason{
		assistloop.StoppedWithAnswer,
		assistloop.StoppedAtTurnLimit,
		assistloop.StoppedAtToolCallLimit,
		assistloop.StoppedRepeating,
		assistloop.StoppedTruncated,
	}

	document := workflowsDocument(t)
	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(document, qt.Contains, "| "+string(reason)+" |")
		})
	}
}

// workflowsDocument reads the document that quotes the run limits.
func workflowsDocument(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "agent-workflows.md"))
	qt.New(t).Assert(err, qt.IsNil)
	return string(body)
}
