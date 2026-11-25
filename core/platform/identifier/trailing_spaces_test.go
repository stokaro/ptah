package identifier_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
)

func TestTrailingSpacesIgnored_PreservesThePrincipalRuleInSnapshots(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlserver")

	encoded, err := json.Marshal(semantics)
	c.Assert(err, qt.IsNil)
	var decoded identifier.Semantics
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)

	c.Assert(semantics.TrailingSpacesIgnored, qt.IsTrue)
	c.Assert(identifier.ForSQLServerCatalog("Latin1_General_CS_AS").TrailingSpacesIgnored, qt.IsTrue)
	c.Assert(string(encoded), qt.Contains, `"trailing_spaces_ignored":true`)
	c.Assert(decoded.Normalize("sqlserver").Equal(semantics), qt.IsTrue)
	c.Assert(decoded.StoredName("reader "), qt.Equals, "reader")
	c.Assert((identifier.Semantics{TrailingSpacesIgnored: true}).IsZero(), qt.IsFalse)
	without := semantics
	without.TrailingSpacesIgnored = false
	c.Assert(semantics.Equal(without), qt.IsFalse)
}
