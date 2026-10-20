package pgdefaultprivilegesource_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestHCL_RefusesInvalidDeclarations(t *testing.T) {
	tests := []struct{ name, body, wantErr string }{
		{name: "missing grantor", body: "object_type = \"TABLES\"\nto = \"reader\"\nprivileges = [SELECT]", wantErr: "requires for_role"},
		{name: "missing grantee", body: "object_type = \"TABLES\"\nfor_role = \"owner\"\nprivileges = [SELECT]", wantErr: "requires to"},
		{name: "missing class", body: "for_role = \"owner\"\nto = \"reader\"\nprivileges = [SELECT]", wantErr: "requires object_type"},
		{name: "empty intent", body: hclIdentity, wantErr: "requires privileges or revoked"},
		{name: "ungranted option", body: hclIdentity + "privileges = [SELECT]\ngrantable = [INSERT]", wantErr: `grantable privilege "INSERT" is not in privileges`},
		{name: "contradictory all", body: hclIdentity + "privileges = [ALL]\nrevoked = [SELECT]", wantErr: "both granted and revoked"},
		{name: "unknown attribute", body: hclIdentity + "privileges = [SELECT]\nwibble = true", wantErr: `unsupported default_privilege attribute "wibble"`},
		{name: "nested block", body: hclIdentity + "privileges = [SELECT]\nextra {}", wantErr: "unsupported.*extra"},
		{name: "wrong role type", body: strings.ReplaceAll(hclIdentity, "role.reader", "table.reader") + "privileges = [SELECT]", wantErr: "must name one global role"},
		{name: "qualified role", body: strings.ReplaceAll(hclIdentity, "role.reader", "role.app.reader") + "privileges = [SELECT]", wantErr: "must name one global role"},
		{name: "role list", body: strings.ReplaceAll(hclIdentity, "role.reader", "[role.reader]") + "privileges = [SELECT]", wantErr: "must name one global role"},
		{name: "wrong schema type", body: hclIdentity + "schema = role.app\nprivileges = [SELECT]", wantErr: "must be a string template or exact schema reference"},
		{name: "numeric list element", body: hclIdentity + "privileges = [123]", wantErr: "must contain only known strings or identifiers"},
		{name: "nested list", body: hclIdentity + "privileges = [[SELECT]]", wantErr: "must contain only known strings or identifiers"},
		{name: "null list", body: hclIdentity + "privileges = null", wantErr: "must be a known string or list of strings"},
		{name: "unknown variable", body: hclIdentity + "privileges = var.missing", wantErr: "unknown variable"},
		{name: "unknown class", body: strings.ReplaceAll(hclIdentity, "TABLES", "TABLSE") + "privileges = [SELECT]", wantErr: "unsupported default privilege object type"},
		{name: "wrong privilege class", body: hclIdentity + "privileges = [EXECUTE]", wantErr: "EXECUTE.*TABLES"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseHCL(t.Context(), hclBlock(test.body))
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.wantErr+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}

func TestHCL_RefusesLabelsAndConflictingBlocks(t *testing.T) {
	tests := []struct{ name, text, wantErr string }{
		{name: "label", text: strings.Replace(hclBlock(hclIdentity+"privileges = [SELECT]"), "default_privilege {", `default_privilege "named" {`, 1), wantErr: "block does not accept labels"},
		{name: "contradictory rights", text: hclBlock(hclIdentity+"privileges = [SELECT]") + hclBlock(hclIdentity+"revoked = [SELECT]"), wantErr: "both granted and revoked"},
		{name: "conflicting comments", text: hclBlock(hclIdentity+"privileges = [SELECT]\ncomment = \"first\"") + hclBlock(hclIdentity+"privileges = [INSERT]\ncomment = \"second\""), wantErr: "conflicting default privilege comments"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseHCL(t.Context(), test.text)
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.wantErr+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}
