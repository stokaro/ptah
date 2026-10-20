package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/lintext"
	"ptah.run/core/schemaext"
)

// SplitStatements uses only the selected owner's framing and safety facts.
func (t *target) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	if request.Target != t.engine.GetId() {
		return lintext.SplitResult{}, fmt.Errorf("provider SQL analysis target changed")
	}
	sources := make([]string, 0, len(request.Sources))
	for _, source := range request.Sources {
		sources = append(sources, source.SQL)
	}
	analysis, err := t.host.client.AnalyzeSQL(ctx, &protocol.AnalyzeSQLRequest{Engine: t.engine.GetId(), Sources: sources})
	if err != nil {
		return lintext.SplitResult{}, err
	}
	result := lintext.SplitResult{Judged: []lintext.Fact{lintext.FactSafetyEffect}, Files: make([][]lintext.Statement, len(sources))}
	for i, file := range analysis.GetFiles() {
		for _, statement := range file.GetStatements() {
			value := lintext.Statement{Start: int(statement.GetStart()), End: int(statement.GetEnd()), Line: int(statement.GetLine()),
				SQL:       sources[i][statement.GetStart():statement.GetEnd()],
				Canonical: statement.GetCanonical(), Words: slices.Clone(statement.GetWords()), SourceWords: slices.Clone(statement.GetSourceWords())}
			if statement.GetSafety() != protocol.SQLStatement_COMMON_DDL {
				impact := schemaext.Destructive
				if statement.GetSafety() == protocol.SQLStatement_BEHAVIORAL {
					impact = schemaext.Behavioral
				}
				value.Effects = []lintext.Effect{{Fact: lintext.FactSafetyEffect, Impact: impact, Reason: statement.GetReason()}}
			}
			result.Files[i] = append(result.Files[i], value)
		}
	}
	return result, nil
}
