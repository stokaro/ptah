package sourcecontent_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sourcecontent"
)

type fixtureFile struct {
	Path       string `json:"path"`
	PathBase64 string `json:"pathBase64"`
	Base64     string `json:"base64"`
}

type generate struct {
	Count      int    `json:"count"`
	PathFormat string `json:"pathFormat"`
	Data       string `json:"data"`
}

type fixtureCase struct {
	Name     string        `json:"name"`
	Files    []fixtureFile `json:"files"`
	Generate *generate     `json:"generate"`
	Manifest string        `json:"manifest"`
	Digest   string        `json:"digest"`
	Refusal  string        `json:"refusal"`
}

type fixtures struct {
	Accepted  []fixtureCase `json:"accepted"`
	Generated []fixtureCase `json:"generated"`
	Refused   []fixtureCase `json:"refused"`
}

// readFixtures reads the operator's cases. A table that stopped matching
// would pass every row it no longer has, so it holds the counts to a floor.
func readFixtures(c *qt.C) fixtures {
	body, err := os.ReadFile("testdata/cases.json")
	c.Assert(err, qt.IsNil)
	var cases fixtures
	c.Assert(json.Unmarshal(body, &cases), qt.IsNil)
	c.Assert(len(cases.Accepted) >= 10, qt.IsTrue, qt.Commentf("%d accepted cases", len(cases.Accepted)))
	c.Assert(len(cases.Generated) >= 1, qt.IsTrue, qt.Commentf("%d generated cases", len(cases.Generated)))
	c.Assert(len(cases.Refused) >= 14, qt.IsTrue, qt.Commentf("%d refused cases", len(cases.Refused)))
	return cases
}

// files decodes one case. A path that is not valid UTF-8 is given base64
// encoded; every other path is given as text, so the decoded path is the
// concatenation of the two fields, one of which is empty.
func files(c *qt.C, fixture fixtureCase) []sourcecontent.File {
	decoded := make([]sourcecontent.File, 0, len(fixture.Files))
	for _, file := range fixture.Files {
		data, err := base64.StdEncoding.DecodeString(file.Base64)
		c.Assert(err, qt.IsNil)
		rawPath, err := base64.StdEncoding.DecodeString(file.PathBase64)
		c.Assert(err, qt.IsNil)
		decoded = append(decoded, sourcecontent.File{Path: file.Path + string(rawPath), Data: data})
	}
	return decoded
}

func generated(spec *generate) []sourcecontent.File {
	out := make([]sourcecontent.File, spec.Count)
	for i := range out {
		out[i] = sourcecontent.File{Path: fmt.Sprintf(spec.PathFormat, i), Data: []byte(spec.Data)}
	}
	return out
}

func TestDigest_HappyPath(t *testing.T) {
	cases := readFixtures(qt.New(t))
	for _, fixture := range cases.Accepted {
		t.Run(fixture.Name, func(t *testing.T) {
			c := qt.New(t)
			input := files(c, fixture)

			manifest, err := sourcecontent.Manifest(input)
			c.Assert(err, qt.IsNil)
			c.Assert(string(manifest), qt.Equals, fixture.Manifest)
			digest, err := sourcecontent.Digest(input)
			c.Assert(err, qt.IsNil)
			c.Assert(digest, qt.Equals, fixture.Digest)

			// The order files arrive in is not part of the content.
			slices.Reverse(input)
			reversed, err := sourcecontent.Digest(input)
			c.Assert(err, qt.IsNil)
			c.Assert(reversed, qt.Equals, fixture.Digest)
		})
	}
	for _, fixture := range cases.Generated {
		t.Run(fixture.Name, func(t *testing.T) {
			c := qt.New(t)
			digest, err := sourcecontent.Digest(generated(fixture.Generate))
			c.Assert(err, qt.IsNil)
			c.Assert(digest, qt.Equals, fixture.Digest)
		})
	}
}

// The refusals each case names, by the rule's name in cases.json.
var rules = map[string]error{
	"empty":          sourcecontent.ErrEmpty,
	"too-many-files": sourcecontent.ErrTooManyFiles,
	"invalid-path":   sourcecontent.ErrInvalidPath,
	"duplicate":      sourcecontent.ErrDuplicate,
	"path-conflict":  sourcecontent.ErrPathConflict,
}

func TestDigest_FailurePath(t *testing.T) {
	cases := readFixtures(qt.New(t))
	for _, fixture := range cases.Refused {
		t.Run(fixture.Name, func(t *testing.T) {
			c := qt.New(t)
			rule := rules[fixture.Refusal]
			c.Assert(rule, qt.IsNotNil, qt.Commentf("cases.json names a rule this test does not know: %q", fixture.Refusal))
			input := files(c, fixture)
			input = append(input, generatedOrNone(fixture.Generate)...)

			manifest, err := sourcecontent.Manifest(input)
			c.Assert(err, qt.ErrorIs, rule)
			c.Assert(manifest, qt.IsNil)
			digest, err := sourcecontent.Digest(input)
			c.Assert(err, qt.ErrorIs, rule)
			c.Assert(digest, qt.Equals, "")
		})
	}
}

// generatedOrNone expands a generated refusal, and gives nothing for a case
// that lists its files.
func generatedOrNone(spec *generate) []sourcecontent.File {
	if spec == nil {
		return nil
	}
	return generated(spec)
}
