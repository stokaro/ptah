package schemaext_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// acceptedDefinitions are texts CanonicalJSON accepts, in the spellings the
// registration check reads differently from the encoder: whitespace, escaped
// keys, nesting at the limit and numbers kept as written.
var acceptedDefinitions = map[string]string{
	"compact":            `{"type":"object"}`,
	"spaced":             " \n\t{ \"b\" : [ 1 , 2 ] , \"a\" : { } }\r\n",
	"html":               `{"a":"<&>"}`,
	"escaped distinct":   `{"a":1,"b":2}`,
	"escaped separator":  `{"a":"  \n \"quoted\" \\"}`,
	"number precision":   `{"n":1.50e+10,"m":-0,"k":12345678901234567890123}`,
	"empty values":       `{"a":[],"b":{},"c":"","d":null,"e":true,"f":false}`,
	"escape in a value":  `{"a":"a😀"}`,
	"same key elsewhere": `{"a":{"x":1},"b":{"x":2},"c":[{"x":1},{"x":2}]}`,
	"deepest allowed":    `{"a":` + strings.Repeat(`[`, 255) + `1` + strings.Repeat(`]`, 255) + `}`,
}

// malformedDefinitions are texts CanonicalJSON refuses; registration must
// refuse each with CanonicalJSON's own error.
var malformedDefinitions = map[string]string{
	"empty":                 ``,
	"truncated":             `{"a":`,
	"trailing value":        `{"a":1} {}`,
	"trailing text":         `{"a":1} x`,
	"duplicate key":         `{"a":1,"a":2}`,
	"duplicate escaped key": `{"a":1,"\u0061":2}`,
	"duplicate nested key":  `{"x":{"a":1,"b":2,"a":3}}`,
	"duplicate after many":  `{"a":1,"b":1,"c":1,"d":1,"e":1,"f":1,"g":1,"h":1,"i":1,"j":1,"a":2}`,
	"unpaired surrogate":    `{"a":"\ud800"}`,
	"lone low surrogate":    `{"a":"\udc00"}`,
	"invalid UTF-8":         "{\"a\":\"\xff\"}",
	"too deep":              `{"a":` + strings.Repeat(`[`, 256) + `1` + strings.Repeat(`]`, 256) + `}`,
}

// nonObjectDefinitions are texts CanonicalJSON accepts that are not a nonempty
// object.
var nonObjectDefinitions = map[string]string{
	"array":               `[1]`,
	"scalar":              `"object"`,
	"empty object":        `{}`,
	"empty object spaced": " { \n } ",
	"null":                `null`,
}

func definitionOf(text string) (schemaext.Registry, error) {
	codec := widgetCodec(widgetKind, schemaext.Desired)
	codec.Definition = json.RawMessage(text)
	return schemaext.NewRegistry(owned(codec))
}

func TestNewRegistry_DefinitionDigest_HappyPath(t *testing.T) {
	for name, text := range acceptedDefinitions {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			canonical, err := schemaext.CanonicalJSON([]byte(text))
			c.Assert(err, qt.IsNil)
			sum := sha256.Sum256(canonical)
			registry, err := definitionOf(text)
			c.Assert(err, qt.IsNil)
			identities := registry.Definitions()
			c.Assert(identities, qt.HasLen, 1)
			c.Assert(identities[0].Definition, qt.Equals, "sha256:"+hex.EncodeToString(sum[:]))
			identity, found := registry.Identity(widgetKind, schemaext.Desired)
			c.Assert(found, qt.IsTrue)
			c.Assert(identity.Definition, qt.Equals, identities[0].Definition)
		})
	}
}

func TestNewRegistry_DefinitionDigest_FailurePath(t *testing.T) {
	for name, text := range malformedDefinitions {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			_, canonicalErr := schemaext.CanonicalJSON([]byte(text))
			c.Assert(canonicalErr, qt.IsNotNil)
			registry, err := definitionOf(text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err.Error(), qt.Contains, "definition: "+canonicalErr.Error())
			c.Assert(registry.Definitions(), qt.HasLen, 0)
		})
	}
	for name, text := range nonObjectDefinitions {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			_, canonicalErr := schemaext.CanonicalJSON([]byte(text))
			c.Assert(canonicalErr, qt.IsNil)
			registry, err := definitionOf(text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
			c.Assert(err, qt.ErrorMatches, `.*definition must be a nonempty object`)
			c.Assert(registry.Definitions(), qt.HasLen, 0)
		})
	}
}

// definitionSalt makes every generated definition unique, so the process-wide
// digest memo never answers for a measured call.
var definitionSalt atomic.Int64

// largeDefinition is an object of 2000 members whose canonical form costs well
// over 10000 allocations.
func largeDefinition() string {
	var text strings.Builder
	fmt.Fprintf(&text, `{"salt":%d`, definitionSalt.Add(1))
	for i := range 2000 {
		fmt.Fprintf(&text, `,"member%d":{"type":"object","index":%d}`, i, i)
	}
	text.WriteString(`}`)
	return text.String()
}

// mallocsOf is the number of allocations call makes. The test binary runs these
// tests one at a time, so nothing else allocates in between.
func mallocsOf(call func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	call()
	runtime.ReadMemStats(&after)
	return after.Mallocs - before.Mallocs
}

// Registering a codec checks its definition and leaves the digest to the first
// caller that needs it. Digesting every definition of every codec when the
// engine runtime is assembled costs megabytes at startup that a drift or a plan
// never uses. Canonicalizing largeDefinition takes more than 10000 allocations;
// the bounds sit far below that and far above a registration's own few
// hundred.
func TestNewRegistry_DefinitionDigestIsLazy(t *testing.T) {
	c := qt.New(t)
	definition := largeDefinition()
	var registry schemaext.Registry
	var registerErr error
	registered := mallocsOf(func() { registry, registerErr = definitionOf(definition) })
	c.Assert(registerErr, qt.IsNil)
	c.Assert(registered < 2000, qt.IsTrue, qt.Commentf("registration allocated %d times", registered))

	var owner string
	var found bool
	asked := mallocsOf(func() { owner, found = registry.Owner(widgetKind, schemaext.Desired) })
	c.Assert(found, qt.IsTrue)
	c.Assert(owner, qt.Not(qt.Equals), "")
	c.Assert(asked < 10, qt.IsTrue, qt.Commentf("Owner allocated %d times", asked))

	listed := mallocsOf(func() { _ = registry.Definitions() })
	c.Assert(listed > 10000, qt.IsTrue, qt.Commentf("the first listing allocated %d times", listed))
}
