package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func TestSQLAnalysisRequiresCompleteLosslessFraming(t *testing.T) {
	const source = "\nDROP TABLE x;\n"
	for _, test := range []struct {
		name   string
		change func(*protocol.SQLAnalysis)
	}{
		{"incomplete", func(r *protocol.SQLAnalysis) { r.Complete = false }},
		{"missing file", func(r *protocol.SQLAnalysis) { r.Files = nil }},
		{"nil file", func(r *protocol.SQLAnalysis) { r.Files[0] = nil }},
		{"missing statement", func(r *protocol.SQLAnalysis) { r.Files[0].Statements = nil }},
		{"nil statement", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0] = nil }},
		{"lost prefix", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Start = 2 }},
		{"lost suffix", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].End = 10 }},
		{"past source", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].End = 100 }},
		{"empty span", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].End = 1 }},
		{"overlapping", func(r *protocol.SQLAnalysis) {
			r.Files[0].Statements = append(r.Files[0].Statements, r.Files[0].Statements[0])
		}},
		{"line", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Line = 1 }},
		{"tokens", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Words = nil }},
		{"source tokens", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].SourceWords = nil }},
		{"canonical", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Canonical = "" }},
		{"unknown disposition", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Safety = 99 }},
		{"missing reason", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Safety = protocol.SQLStatement_UNKNOWN }},
		{"unexpected reason", func(r *protocol.SQLAnalysis) { r.Files[0].Statements[0].Reason = "native" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.AnalyzeSQLRequest{Engine: "test", Sources: []string{source}}
			result := &protocol.SQLAnalysis{Complete: true, Files: []*protocol.SQLFile{{Statements: []*protocol.SQLStatement{{Start: 1, End: 14, Line: 2, Words: []string{"DROP", "TABLE", "X"}, SourceWords: []string{"DROP", "TABLE", "x"}, Canonical: "DROP TABLE X", Safety: protocol.SQLStatement_COMMON_DDL}}}}}
			c.Assert(provider.ValidateSQLAnalysis(request, result), qt.IsNil)
			test.change(result)
			c.Assert(provider.ValidateSQLAnalysis(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
