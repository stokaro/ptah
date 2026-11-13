package migrator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// operationID is the reference Ptah Operator passes: its operation ID.
const operationID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestValidateRunReference_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		reference string
	}{
		{name: "none", reference: ""},
		{name: "an operator's operation ID", reference: operationID},
		{name: "the longest the column holds", reference: strings.Repeat("r", migrator.MaxRunReferenceLength)},
		{name: "every visible ASCII character", reference: "!\"#$%&'()*+,-./09:;<=>?@AZ[\\]^_`az{|}~"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrator.ValidateRunReference(test.reference)

			c.Assert(err, qt.IsNil)
		})
	}
}

// A reference the log would store differently, or not at all, is refused before
// any run starts: a shortened reference names another run, and a character an
// engine's charset may replace makes it one.
func TestValidateRunReference_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		wantError string
	}{
		{
			name:      "one character longer than the column",
			reference: strings.Repeat("r", migrator.MaxRunReferenceLength+1),
			wantError: "invalid run attribution: a run reference is at most 255 characters, and this one has 256",
		},
		{
			name:      "a space",
			reference: "run 7",
			wantError: `invalid run attribution: a run reference holds visible ASCII characters only, and this one has byte 0x20 at offset 3`,
		},
		{
			name:      "a newline",
			reference: "run\n7",
			wantError: `invalid run attribution: a run reference holds visible ASCII characters only, and this one has byte 0x0a at offset 3`,
		},
		{
			name:      "a letter outside ASCII",
			reference: "léger",
			wantError: `invalid run attribution: a run reference holds visible ASCII characters only, and this one has byte 0xc3 at offset 1`,
		},
		{
			name:      "DEL",
			reference: "run\x7f",
			wantError: `invalid run attribution: a run reference holds visible ASCII characters only, and this one has byte 0x7f at offset 3`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrator.ValidateRunReference(test.reference)

			c.Assert(err, qt.ErrorIs, migrator.ErrInvalidRunAttribution)
			c.Assert(err.Error(), qt.Equals, test.wantError)
		})
	}
}

func TestValidateActorOrigin_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		origin string
	}{
		{name: "none", origin: ""},
		{name: "one word", origin: "admission"},
		{name: "a digit first", origin: "2fa"},
		{name: "every separator", origin: "k8s.io:service-account_token"},
		{name: "the longest the column holds", origin: strings.Repeat("o", migrator.MaxActorOriginLength)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrator.ValidateActorOrigin(test.origin)

			c.Assert(err, qt.IsNil)
		})
	}
}

// An origin is one spelling of one word, so a reader can group entries by it:
// "Admission" beside "admission" would read as two origins.
func TestValidateActorOrigin_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		origin    string
		wantError string
	}{
		{
			name:      "one character longer than the column",
			origin:    strings.Repeat("o", migrator.MaxActorOriginLength+1),
			wantError: "invalid run attribution: an actor origin is at most 64 characters, and this one has 65",
		},
		{
			name:      "an upper-case letter",
			origin:    "Admission",
			wantError: `invalid run attribution: an actor origin is one lowercase word of letters, digits, '.', '_', ':' and '-' that starts with a letter or a digit, and "Admission" is not`,
		},
		{
			name:      "a separator first",
			origin:    "-admission",
			wantError: `invalid run attribution: an actor origin is one lowercase word of letters, digits, '.', '_', ':' and '-' that starts with a letter or a digit, and "-admission" is not`,
		},
		{
			name:      "two words",
			origin:    "admission webhook",
			wantError: `invalid run attribution: an actor origin is one lowercase word of letters, digits, '.', '_', ':' and '-' that starts with a letter or a digit, and "admission webhook" is not`,
		},
		{
			name:      "a slash",
			origin:    "ci/cd",
			wantError: `invalid run attribution: an actor origin is one lowercase word of letters, digits, '.', '_', ':' and '-' that starts with a letter or a digit, and "ci/cd" is not`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrator.ValidateActorOrigin(test.origin)

			c.Assert(err, qt.ErrorIs, migrator.ErrInvalidRunAttribution)
			c.Assert(err.Error(), qt.Equals, test.wantError)
		})
	}
}
