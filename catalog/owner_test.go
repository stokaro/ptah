package catalog_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

func TestOwnerLoginKnowledgeSurvivesJSON(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *bool
	}{{"unknown", nil}, {"disabled", new(false)}, {"enabled", new(true)}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := catalog.ObjectOwner{Kind: "table", Name: "items", Owner: "app", OwnerCanLogin: test.value}
			encoded, err := json.Marshal(owner)
			c.Assert(err, qt.IsNil)
			var decoded catalog.ObjectOwner
			c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, owner)
		})
	}
}

func TestOwnerCloneOwnsItsLoginKnowledge(t *testing.T) {
	c := qt.New(t)
	owner := catalog.ObjectOwner{Owner: "app", OwnerCanLogin: new(true)}
	clone := owner.Clone()
	*clone.OwnerCanLogin = false
	c.Assert(*owner.OwnerCanLogin, qt.IsTrue)
	c.Assert((catalog.ObjectOwner{}).Clone().OwnerCanLogin, qt.IsNil)
}
