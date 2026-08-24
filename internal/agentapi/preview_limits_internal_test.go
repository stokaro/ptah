package agentapi

// White-box testing required: previewLifetime and maxLivePreviews are unexported
// and docs/agent-workflows.md quotes both by value. A document that quotes a
// number is stale the moment the number moves, and this is the only place the
// two can be compared.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestWorkflowsDocumentQuotesThePreviewLimits binds the prose to the constants
// it describes.
func TestWorkflowsDocumentQuotesThePreviewLimits(t *testing.T) {
	tests := []struct {
		name  string
		quote string
	}{
		{name: "the token lifetime", quote: "`previewLifetime` is 15 minutes"},
		{name: "the live-token bound", quote: "`maxLivePreviews` is 32"},
		{name: "the lifetime in the state table", quote: "15 minutes passed"},
		{name: "the bound in the state table", quote: "a 33rd preview evicted it"},
	}

	document := workflowsDocument(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(document, qt.Contains, tt.quote)
		})
	}
}

// TestPreviewLimitsAreWhatTheDocumentSays is the other half: the quotes above
// are only worth checking while the constants still hold those values.
func TestPreviewLimitsAreWhatTheDocumentSays(t *testing.T) {
	c := qt.New(t)

	c.Assert(previewLifetime.Minutes(), qt.Equals, float64(15))
	c.Assert(maxLivePreviews, qt.Equals, 32)
}

// workflowsDocument reads the document that quotes the two constants.
func workflowsDocument(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "docs", "agent-workflows.md"))
	qt.New(t).Assert(err, qt.IsNil)
	return strings.ReplaceAll(string(body), "\n", " ")
}
