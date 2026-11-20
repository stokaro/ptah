package providerhost_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintnames"
	"ptah.run/provider-sdk/provider"
)

// The host reads a rule name through lintext.CanonicalName and the provider SDK
// through provider.ValidLintRuleName. The two are written apart, since the SDK
// is its own module and lintext must not link it, so this test is what holds
// them to one grammar: a name one accepts and the other refuses is a provider
// the host starts and then fails on.
func TestRuleNameGrammarMatchesTheSDK(t *testing.T) {
	for _, name := range []string{
		"locking/add-foreign-key", "design/xy101", "a/b", "online/on-1", "ydb/add-column-form",
		"XY101", "", "locking", "locking/", "/add", "a/b/c", "Locking/add", "locking/Add", "locking/add-",
		"locking/-add", "locking/add--key", "locking/1add", "1lock/add", "locking/add_key", " locking/add", "locking/add ",
		"locking/add-1", "l-1/a-b-c", "locking//add", "DS102", "PG301P",
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintext.CanonicalName(name), qt.Equals, provider.ValidLintRuleName(name))
			c.Assert(lintnames.WellFormed(name), qt.Equals, provider.ValidLintRuleName(name))
		})
	}
}
