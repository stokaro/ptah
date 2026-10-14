package pgdefaultprivilege_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestChangeCodec_RetainsBeforeAndRestoration(t *testing.T) {
	c := qt.New(t)
	value := &pgdefaultprivilege.Change{Before: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "select", WithOption: true}}},
		Restore: &pgdefaultprivilege.Observed{Revoked: []string{"update"}}}
	codec := pgdefaultprivilege.ChangeCodec()
	encoded, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"before":{"privileges":[{"privilege":"SELECT","with_option":true}]},"restore":{"revoked":["UPDATE"]}}`)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, &pgdefaultprivilege.Change{Before: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}, Restore: &pgdefaultprivilege.Observed{Revoked: []string{"UPDATE"}}})
	value.Before.Privileges[0].Privilege = "DELETE"
	value.Restore.Revoked[0] = "INSERT"
	c.Assert(decoded.(*pgdefaultprivilege.Change).Before.Privileges[0].Privilege, qt.Equals, "SELECT")
	c.Assert(decoded.(*pgdefaultprivilege.Change).Restore.Revoked, qt.DeepEquals, []string{"UPDATE"})
}

func TestChangeCodec_RefusesAmbiguousOrMalformedChanges(t *testing.T) {
	tests := []string{
		`{}`,
		`{"before":null,"granted":[{"privilege":"SELECT"}]}`,
		`{"before":{"privileges":null},"granted":[{"privilege":"SELECT"}]}`,
		`{"before":{"privileges":[{"privilege":"SELECT","with_option":false}]},"granted":[{"privilege":"SELECT"}]}`,
		`{"granted":[{"privilege":"SELECT","PRIVILEGE":"UPDATE"}]}`,
		`{"granted":[{"privilege":"SELECT","privilege":"UPDATE"}]}`,
		`{"restore":{},"granted":[{"privilege":"SELECT"}]}`,
		`{"restore":{},"unknown_before":true}`,
		`{"before":{},"unknown_before":true,"granted":[{"privilege":"ALL"}]}`,
		`{"unknown_before":true,"granted":[{"privilege":"SELECT"}]}`,
		`{"granted":[],"grant_options_added":["SELECT"]}`,
		`{"granted":[{"privilege":"SELECT"}],"unknown_before":false}`,
	}
	for _, wire := range tests {
		t.Run(wire, func(t *testing.T) {
			c := qt.New(t)
			_, err := pgdefaultprivilege.ChangeCodec().Decode(json.RawMessage(wire))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestOperationCodec_RefusesInjectedPrivilegeAndUnknownIdentityKey(t *testing.T) {
	tests := []string{
		`{"identity":{"grantor":"owner","schema":"app","object_type":"TABLES","grantee":"reader"},"action":"grant","privileges":["SELECT; DROP ROLE owner"]}`,
		`{"identity":{"grantor":"owner","schema":"app","object_type":"TABLES","grantee":"reader","role":"someone"},"action":"grant","privileges":["SELECT"]}`,
		`{"identity":{"grantor":"owner","schema":"app","object_type":"TABLES","grantee":"reader"},"action":"revoke","privileges":["SELECT"],"with_option":true}`,
	}
	for _, wire := range tests {
		t.Run(wire, func(t *testing.T) {
			c := qt.New(t)
			_, err := pgdefaultprivilege.OperationCodec().Decode(json.RawMessage(wire))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}
