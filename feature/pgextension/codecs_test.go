package pgextension_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

func TestCodecs_HappyPath(t *testing.T) {
	desired, observed := pgextension.Codecs()[0], pgextension.Codecs()[1]
	tests := []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Payload
		wire  string
	}{
		{name: "a bare declaration", codec: desired, value: &pgextension.DesiredExtension{}, wire: `{}`},
		{
			name: "a full declaration", codec: desired,
			value: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true},
			wire:  `{"schema":"ext","version":"1.6","comment":"trigrams","if_not_exists":true}`,
		},
		{
			name: "a declaration a read carried members into", codec: desired,
			value: &pgextension.DesiredExtension{Schema: "public", IfNotExists: true, Provides: []string{"isbn", "ean13", "isbn"}},
			wire:  `{"schema":"public","if_not_exists":true,"provides":["ean13","isbn"]}`,
		},
		{
			name: "an observation without a comment", codec: observed,
			value: &pgextension.ObservedExtension{Schema: "public", Version: "1.0"},
			wire:  `{"schema":"public","version":"1.0","relocatable":false}`,
		},
		{
			name: "an observation with an empty comment", codec: observed,
			value: &pgextension.ObservedExtension{Schema: "public", Version: "1.0", Comment: new("")},
			wire:  `{"schema":"public","version":"1.0","relocatable":false,"comment":""}`,
		},
		{
			name: "an observation with members", codec: observed,
			value: &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Relocatable: true, Provides: []string{"similarity", "gin_trgm_ops"}},
			wire:  `{"schema":"ext","version":"1.6","relocatable":true,"provides":["gin_trgm_ops","similarity"]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded := must.Must(test.codec.Encode(test.value))
			decoded := must.Must(test.codec.Decode(encoded))

			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(decoded.(schemaext.Value).Equal(test.value.(schemaext.Value)), qt.IsTrue)
		})
	}
}

func TestCodecs_FailurePath(t *testing.T) {
	desired, observed := pgextension.Codecs()[0], pgextension.Codecs()[1]
	tests := []struct {
		name  string
		codec schemaext.Codec
		wire  string
	}{
		{name: "an empty version spelled out", codec: desired, wire: `{"version":""}`},
		{name: "IF NOT EXISTS spelled false", codec: desired, wire: `{"if_not_exists":false}`},
		{name: "a key in another case", codec: desired, wire: `{"Schema":"ext"}`},
		{name: "an unknown key", codec: desired, wire: `{"cascade":true}`},
		{name: "an empty declared member list spelled out", codec: desired, wire: `{"provides":[]}`},
		{name: "an empty declared member", codec: desired, wire: `{"provides":[""]}`},
		{name: "an observation without its version", codec: observed, wire: `{"schema":"public","relocatable":false}`},
		{name: "an empty member list spelled out", codec: observed, wire: `{"schema":"public","version":"1.0","relocatable":false,"provides":[]}`},
		{name: "an empty member", codec: observed, wire: `{"schema":"public","version":"1.0","relocatable":false,"provides":[""]}`},
		{name: "a null comment", codec: observed, wire: `{"schema":"public","version":"1.0","relocatable":false,"comment":null}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := test.codec.Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
			c.Assert(decoded, qt.IsNil)
		})
	}
}

func TestCoverage_HappyPath(t *testing.T) {
	c := qt.New(t)

	complete := must.Must(pgextension.CompleteCoverage(schemaext.Observed))

	c.Assert(complete.Lookup(pgextension.ExtensionKind, pgextension.Ref("pg_trgm")).State, qt.Equals, schemaext.Complete)
	c.Assert(complete.KindRecords(), qt.HasLen, 1)
	c.Assert(complete.KindRecords()[0].Model.Owner, qt.Equals, pgextension.Owner)
}
