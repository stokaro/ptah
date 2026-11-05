// Package main judges migration files by who opens their transaction, over
// RPC. Its one rule reports a file's BEGIN, which would nest inside the
// transaction of a migrator that wraps the file and is the file's only
// transaction under a tool that opens none.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-sdk-lint-transaction-owner"
	description.Features = append(description.Features, provider.LintFeature, provider.LintTransactionOwnerFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.LintFeature, provider.LintTransactionOwnerFeature)
	engine.LintRules = []*protocol.LintRule{{Code: "FX301", Title: "BEGIN nests inside the migrator's transaction", Severity: protocol.LintRule_WARNING}}
	owner := &backend.Provider{}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, Linter: transactionLinter{}}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// transactionLinter reads the owner the check names. Its other answers are
// the fixture's.
type transactionLinter struct {
	backend.Linter
}

func (transactionLinter) CheckLintMigrations(ctx context.Context, request *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.CheckedLint{Complete: true}
	if request.GetTransactionOwner() == protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_FILE {
		return result, nil
	}
	for file, source := range request.GetFiles() {
		for statement, value := range source.GetStatements() {
			if strings.HasPrefix(strings.ToUpper(value.GetSql()), "BEGIN") {
				result.Findings = append(result.Findings, &protocol.LintFinding{Code: "FX301", File: int32(file), Statement: int32(statement), // #nosec G115 -- a request holds far fewer files and statements than the type's range
					Message: "BEGIN nests inside the transaction the migrator opens around this file"})
			}
		}
	}
	return result, nil
}
