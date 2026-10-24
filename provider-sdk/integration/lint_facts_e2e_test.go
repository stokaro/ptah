//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

// effectLines renders each statement's effects as one line per effect.
func effectLines(file *protocol.LintFile) [][]string {
	lines := make([][]string, 0, len(file.GetStatements()))
	for _, statement := range file.GetStatements() {
		var effects []string
		for _, effect := range statement.GetEffects() {
			effects = append(effects, fmt.Sprintf("%s %s %q %q %q %q", effect.GetFact(), effect.GetStatementClass(), effect.GetKind(),
				effect.GetObject(), effect.GetTable(), effect.GetCapability()))
		}
		lines = append(lines, effects)
	}
	return lines
}

// A provider that judges the statement facts reports them to a host that
// lists lint.statement-facts, and the same process splits as contract 25.0
// for a host that does not: the facts need both sides.
func TestIndependentProviderJudgesStatementFactsOnlyForAHostThatListsThem(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "lint-facts")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	sources := []*protocol.LintSource{{Name: "1.sql", Up: true,
		Sql: "CREATE TABLE a (id INTEGER, price DECIMAL(10,2));\nUPSERT INTO a VALUES (1);\nDROP TABLE a;"}}
	drop := `LINT_FACT_STATEFUL_DROP LINT_STATEMENT_CLASS_UNSPECIFIED "table" "" "" ""`
	for _, row := range []struct {
		name     string
		features []string
		judged   []protocol.LintFact
		effects  [][]string
	}{
		{name: "a host that lists the facts", features: []string{provider.LintStatementFactsFeature},
			judged: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP, protocol.LintFact_LINT_FACT_STATEMENT_KIND,
				protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY},
			effects: [][]string{
				{
					`LINT_FACT_STATEMENT_KIND LINT_STATEMENT_CLASS_CREATE_TABLE "CREATE TABLE" "a" "" ""`,
					`LINT_FACT_MISSING_PRIMARY_KEY LINT_STATEMENT_CLASS_UNSPECIFIED "" "" "a" ""`,
					`LINT_FACT_REQUIRED_CAPABILITY LINT_STATEMENT_CLASS_UNSPECIFIED "COLUMN" "price" "a" "parameterized_decimal"`,
				},
				{`LINT_FACT_STATEMENT_KIND LINT_STATEMENT_CLASS_OTHER "UPSERT" "" "" ""`},
				{drop, `LINT_FACT_STATEMENT_KIND LINT_STATEMENT_CLASS_SCHEMA "DROP TABLE" "a" "" ""`},
			}},
		{name: "a host that does not", judged: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP},
			effects: [][]string{nil, nil, {drop}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
				RequiredFeatures: []string{provider.LintFeature}, HostFeatures: row.features})

			split, err := client.SplitLintStatements(c.Context(), &protocol.SplitLintRequest{Engine: "fixture", Sources: sources})

			c.Assert(err, qt.IsNil)
			c.Assert(split.GetJudged(), qt.DeepEquals, row.judged)
			c.Assert(effectLines(split.GetFiles()[0]), qt.DeepEquals, row.effects)
		})
	}
}
