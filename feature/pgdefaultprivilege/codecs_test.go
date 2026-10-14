package pgdefaultprivilege_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestCodecs_CanonicalRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Value
		wire  string
	}{
		{name: "declared absence", codec: pgdefaultprivilege.Codecs()[0], value: &pgdefaultprivilege.Desired{}, wire: `{}`},
		{name: "observed builtin default", codec: pgdefaultprivilege.Codecs()[1], value: &pgdefaultprivilege.Observed{}, wire: `{}`},
		{name: "grantable wins", codec: pgdefaultprivilege.Codecs()[0], value: &pgdefaultprivilege.Desired{
			Privileges: []pgdefaultprivilege.Grant{{Privilege: "select"}, {Privilege: "UPDATE"}, {Privilege: "SELECT", WithOption: true}},
			Revoked:    []string{"delete", "DELETE"}, Comment: "Read and update"},
			wire: `{"privileges":[{"privilege":"SELECT","with_option":true},{"privilege":"UPDATE"}],"revoked":["DELETE"],"comment":"Read and update"}`},
		{name: "revoked builtin privilege", codec: pgdefaultprivilege.Codecs()[1], value: &pgdefaultprivilege.Observed{Revoked: []string{"execute"}},
			wire: `{"revoked":["EXECUTE"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := test.value.Clone()
			encoded, err := test.codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.wire)
			decoded, err := test.codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.(schemaext.Value).Equal(test.value), qt.IsTrue)
			c.Assert(test.value.Equal(before), qt.IsTrue)
		})
	}
}

func TestCodecs_RefuseInvalidWire(t *testing.T) {
	tests := []struct{ name, wire string }{
		{name: "unknown key", wire: `{"owner":"alice"}`},
		{name: "key case", wire: `{"Revoked":["SELECT"]}`},
		{name: "empty list", wire: `{"privileges":[]}`},
		{name: "null list", wire: `{"revoked":null}`},
		{name: "null member", wire: `{"revoked":[null]}`},
		{name: "null grant", wire: `{"privileges":[null]}`},
		{name: "empty privilege", wire: `{"privileges":[{"privilege":" "}]}`},
		{name: "unknown nested key", wire: `{"privileges":[{"privilege":"SELECT","cascade":true}]}`},
		{name: "nested key case", wire: `{"privileges":[{"Privilege":"SELECT"}]}`},
		{name: "false optional field", wire: `{"privileges":[{"privilege":"SELECT","with_option":false}]}`},
		{name: "duplicate key", wire: `{"revoked":["SELECT"],"revoked":["UPDATE"]}`},
		{name: "nested duplicate", wire: `{"privileges":[{"privilege":"SELECT","privilege":"UPDATE"}]}`},
		{name: "conflicting states", wire: `{"privileges":[{"privilege":"select"}],"revoked":["SELECT"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			for _, codec := range pgdefaultprivilege.Codecs() {
				value, err := codec.Decode(json.RawMessage(test.wire))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
				c.Assert(value, qt.IsNil)
			}
		})
	}
}

func TestCodecs_RejectSourceCommentInObservation(t *testing.T) {
	c := qt.New(t)
	value, err := pgdefaultprivilege.Codecs()[1].Decode(json.RawMessage(`{"comment":"source text"}`))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.IsNil)
}

func TestModelCopies_IsolateEveryList(t *testing.T) {
	c := qt.New(t)
	desired := &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"DELETE"}, Comment: "source"}
	copied := desired.Copy()
	copied.Privileges[0].Privilege = "UPDATE"
	copied.Revoked[0] = "TRUNCATE"
	observed := desired.Observed()
	observed.Privileges[0].Privilege = "INSERT"
	observed.Revoked[0] = "REFERENCES"
	c.Assert(desired, qt.DeepEquals, &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []string{"DELETE"}, Comment: "source"})
	restored := observed.Desired()
	copiedObservation := observed.Copy()
	restored.Privileges[0].Privilege = "SELECT"
	restored.Revoked[0] = "DELETE"
	copiedObservation.Privileges[0].Privilege = "UPDATE"
	copiedObservation.Revoked[0] = "TRUNCATE"
	c.Assert(observed, qt.DeepEquals, &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}}, Revoked: []string{"REFERENCES"}})
	c.Assert(restored.Comment, qt.Equals, "")
}
