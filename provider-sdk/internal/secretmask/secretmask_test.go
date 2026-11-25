package secretmask_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/provider-sdk/internal/secretmask"
)

// ruleCorpus is the corpus both copies of the rule are tested against: this
// package and the host's ptah.run/internal/secretmask, which reads this file
// from the repository. No row holds a secret in another letter case, which is
// the one place the two copies read text differently.
type ruleCorpus struct {
	Replacement string `json:"replacement"`
	Mask        []struct {
		Name    string   `json:"name"`
		Secrets []string `json:"secrets"`
		Users   []string `json:"users"`
		Text    string   `json:"text"`
		Want    string   `json:"want"`
	} `json:"mask"`
	SecretParameters []struct {
		Key    string `json:"key"`
		Value  string `json:"value"`
		Secret bool   `json:"secret"`
	} `json:"secretParameters"`
}

// The corpus floors: a corpus that stopped decoding into its fields would
// otherwise run no row and pass.
const (
	maskRowsFloor      = 66
	parameterRowsFloor = 46
)

func readRuleCorpus(c *qt.C) ruleCorpus {
	data, err := os.ReadFile(filepath.Join("testdata", "rule.json"))
	c.Assert(err, qt.IsNil)
	var corpus ruleCorpus
	c.Assert(json.Unmarshal(data, &corpus), qt.IsNil)
	return corpus
}

// Every row of the shared corpus reads here as it reads in the host's rule.
func TestMask_FollowsTheSharedRule(t *testing.T) {
	c := qt.New(t)
	corpus := readRuleCorpus(c)
	c.Assert(len(corpus.Mask) >= maskRowsFloor, qt.IsTrue, qt.Commentf("%d rows", len(corpus.Mask)))

	for _, row := range corpus.Mask {
		t.Run(row.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.Mask(row.Text, row.Secrets, row.Users, corpus.Replacement), qt.Equals, row.Want)
		})
	}
}

func TestIsSecretParameter_FollowsTheSharedRule(t *testing.T) {
	c := qt.New(t)
	corpus := readRuleCorpus(c)
	c.Assert(len(corpus.SecretParameters) >= parameterRowsFloor, qt.IsTrue, qt.Commentf("%d rows", len(corpus.SecretParameters)))

	for _, row := range corpus.SecretParameters {
		t.Run(row.Key+"="+row.Value, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.IsSecretParameter(row.Key, row.Value), qt.Equals, row.Secret)
		})
	}
}

// A secret is read in any ASCII letter case, where it is long enough to take
// out anywhere and where it stands as a credential.
func TestMask_TakesASecretInAnyLetterCase(t *testing.T) {
	tests := []struct {
		name    string
		secrets []string
		text    string
		want    string
	}{
		{name: "a long secret in capitals", secrets: []string{"Pa55wordX9"}, text: "login with PA55WORDX9 failed", want: "login with *** failed"},
		{name: "the canary in capitals", secrets: []string{"ptah-conformance-canary-3f9c1a"}, text: "user PTAH-CONFORMANCE-CANARY-3F9C1A refused", want: "user *** refused"},
		{name: "a short credential in capitals", secrets: []string{"ab"}, text: "connect to db://app:AB@h/x", want: "connect to db://app:***@h/x"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.Mask(test.text, test.secrets, []string{"app"}, "***"), qt.Equals, test.want)
		})
	}
}

// The control: a short secret in another case anywhere but where it stands as a
// credential keeps its letters, as it does in the case it was given.
func TestMask_KeepsTheLettersOfAShortSecretInAnyLetterCase(t *testing.T) {
	c := qt.New(t)
	c.Assert(secretmask.Mask("AB-testing failed for ab", []string{"ab"}, []string{"app"}, "***"), qt.Equals, "AB-testing failed for ab")
}
