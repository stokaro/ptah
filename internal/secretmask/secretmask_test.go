package secretmask_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/secretmask"
)

// ruleCorpus is the corpus both copies of the rule are tested against: this
// package and provider-sdk/internal/secretmask. It lives in the SDK module, so
// the published SDK carries the corpus its own test reads.
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
	data, err := os.ReadFile(filepath.Join("..", "..", "provider-sdk", "internal", "secretmask", "testdata", "rule.json"))
	c.Assert(err, qt.IsNil)
	var corpus ruleCorpus
	c.Assert(json.Unmarshal(data, &corpus), qt.IsNil)
	return corpus
}

// Every row of the shared corpus reads here as it reads in the SDK's copy.
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

// The host compares a secret's letters as they are written. The SDK's copy
// reads any ASCII letter case, and this is the row that tells the two apart.
func TestMask_KeepsASecretInAnotherLetterCase(t *testing.T) {
	c := qt.New(t)
	c.Assert(secretmask.Mask("login with PA55WORDX9 failed", []string{"Pa55wordX9"}, nil, "***"), qt.Equals, "login with PA55WORDX9 failed")
}

// A piece of a secret is taken out where it stands as a word, from three
// characters, and kept inside a longer word and below three characters.
func TestMaskPieces(t *testing.T) {
	tests := []struct {
		name   string
		pieces []string
		text   string
		want   string
	}{
		{name: "a piece that stands as a word", pieces: []string{"shop"}, text: "database=shop failed", want: "database=*** failed"},
		{name: "a piece of three characters", pieces: []string{"Xk9"}, text: "host Xk9 unknown", want: "host *** unknown"},
		{name: "a piece inside a longer word", pieces: []string{"shop"}, text: "database=shopfront failed", want: "database=shopfront failed"},
		{name: "a piece of two characters", pieces: []string{"db"}, text: "database db failed", want: "database db failed"},
		// #nosec G101 -- a made-up address; the piece "db" is the point, not a credential
		{name: "a short piece where it stands as a credential", pieces: []string{"db"}, text: "postgres://app:db@h/x", want: "postgres://app:db@h/x"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.MaskPieces(test.text, nil, test.pieces, []string{"app"}, "***"), qt.Equals, test.want)
		})
	}
}

// A whole secret is taken out before its pieces, so the longer spelling goes in
// one piece.
func TestMaskPieces_TakesTheWholeSecretFirst(t *testing.T) {
	c := qt.New(t)
	got := secretmask.MaskPieces("dial shop/Xk9Lm@db and shop", []string{"shop/Xk9Lm"}, []string{"shop", "Xk9Lm"}, nil, "***")
	c.Assert(got, qt.Equals, "dial ***@db and ***")
}

func TestMasksAnywhere(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "x", want: false},
		{value: "ab", want: false},
		{value: "ptah", want: false},
		{value: "äöüß", want: false},
		{value: "Xk9Lm", want: true},
		{value: "äöüßé", want: true},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.MasksAnywhere(test.value), qt.Equals, test.want)
		})
	}
}

func TestContainsWord(t *testing.T) {
	tests := []struct {
		name string
		text string
		word string
		want bool
	}{
		{name: "a word of its own", text: "open x: file does not exist", word: "x", want: true},
		{name: "a quoted word", text: `cannot convert "ab" to number`, word: "ab", want: true},
		{name: "a word after its letters inside another", text: "lab gateway ab", word: "ab", want: true},
		{name: "a word with no letters", text: "absolute paths are not supported: /x: read", word: "/x", want: true},
		{name: "letters inside a word", text: "file does not exist", word: "x", want: false},
		{name: "letters before an underscore", text: "ab_test failed", word: "ab", want: false},
		{name: "a word that is not there", text: "open shop: failed", word: "db", want: false},
		{name: "an empty word", text: "open x", word: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.ContainsWord(test.text, test.word), qt.Equals, test.want)
		})
	}
}

func TestIsUserParameter(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{key: "user", want: true},
		{key: "User Id", want: true},
		{key: "UID", want: true},
		{key: "PGUSER", want: true},
		{key: "MYSQL_USER", want: true},
		{key: "username", want: true},
		{key: "password", want: false},
		{key: "users_table", want: false},
		{key: "uidx", want: false},
	}
	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretmask.IsUserParameter(test.key), qt.Equals, test.want)
		})
	}
}
