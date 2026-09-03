package catalog_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
)

func TestRolePasswordStateJSONRoundTripHappyPath(t *testing.T) {
	tests := []struct {
		name  string
		state catalog.RolePasswordState
		want  string
	}{
		{name: "unknown", state: catalog.RolePasswordUnknown, want: `"password_state":"unknown"`},
		{name: "absent", state: catalog.RolePasswordAbsent, want: `"password_state":"absent"`},
		{name: "present", state: catalog.RolePasswordPresent, want: `"password_state":"present"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := json.Marshal(catalog.Role{Name: "app_user", PasswordState: test.state})
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Contains, test.want)

			var decoded catalog.Role
			c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
			c.Assert(decoded.PasswordState, qt.Equals, test.state)
		})
	}
}

func TestRolePasswordStateTextRoundTripHappyPath(t *testing.T) {
	var zeroValue catalog.RolePasswordState
	tests := []struct {
		name  string
		state catalog.RolePasswordState
		want  string
	}{
		{name: "zero value is unknown", state: zeroValue, want: "unknown"},
		{name: "absent", state: catalog.RolePasswordAbsent, want: "absent"},
		{name: "present", state: catalog.RolePasswordPresent, want: "present"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := test.state.MarshalText()
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want)

			var decoded catalog.RolePasswordState
			c.Assert(decoded.UnmarshalText(encoded), qt.IsNil)
			c.Assert(decoded, qt.Equals, test.state)
		})
	}
}

func TestRolePasswordStateInvalidValueFailurePath(t *testing.T) {
	t.Run("marshal text", func(t *testing.T) {
		c := qt.New(t)
		_, err := catalog.RolePasswordState("not-a-state").MarshalText()
		c.Assert(err, qt.ErrorMatches, `invalid role password state "not-a-state"`)
	})

	t.Run("unmarshal text", func(t *testing.T) {
		c := qt.New(t)
		var decoded catalog.RolePasswordState
		err := decoded.UnmarshalText([]byte("not-a-state"))
		c.Assert(err, qt.ErrorMatches, `invalid role password state "not-a-state"`)
	})

	t.Run("marshal", func(t *testing.T) {
		c := qt.New(t)
		_, err := json.Marshal(catalog.Role{
			Name:          "app_user",
			PasswordState: catalog.RolePasswordState("not-a-state"),
		})
		c.Assert(err, qt.ErrorMatches, `json: error calling MarshalText for type \*catalog.RolePasswordState: invalid role password state "not-a-state"`)
	})

	t.Run("unmarshal", func(t *testing.T) {
		c := qt.New(t)
		var decoded catalog.Role
		err := json.Unmarshal([]byte(`{"name":"app_user","password_state":"not-a-state"}`), &decoded)
		c.Assert(err, qt.ErrorMatches, `invalid role password state "not-a-state"`)
	})
}

func TestRolePasswordStateRemainsDistinctInValueAndJSON(t *testing.T) {
	c := qt.New(t)
	unknown := catalog.Role{Name: "app_user", PasswordState: catalog.RolePasswordUnknown}
	absent := catalog.Role{Name: "app_user", PasswordState: catalog.RolePasswordAbsent}
	present := catalog.Role{Name: "app_user", PasswordState: catalog.RolePasswordPresent}

	c.Assert(unknown, qt.Not(qt.Equals), absent)
	c.Assert(unknown, qt.Not(qt.Equals), present)
	c.Assert(absent, qt.Not(qt.Equals), present)

	unknownJSON, err := json.Marshal(unknown)
	c.Assert(err, qt.IsNil)
	absentJSON, err := json.Marshal(absent)
	c.Assert(err, qt.IsNil)
	presentJSON, err := json.Marshal(present)
	c.Assert(err, qt.IsNil)

	c.Assert(string(unknownJSON), qt.Not(qt.Equals), string(absentJSON))
	c.Assert(string(unknownJSON), qt.Not(qt.Equals), string(presentJSON))
	c.Assert(string(absentJSON), qt.Not(qt.Equals), string(presentJSON))
}
