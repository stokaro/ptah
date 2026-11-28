package logingrants_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/logingrants"
)

// TestReport_HappyPath writes the note for the grants to logins a read holds,
// counting each grant and naming each login once, and nothing for a read
// whose grants are all to roles.
func TestReport_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		grants []catalog.Grant
		want   string
	}{
		{name: "grants to roles", grants: []catalog.Grant{{Role: "reader", Privilege: "SELECT"}}},
		{
			name:   "one grant to a login",
			grants: []catalog.Grant{{Role: "reader", Privilege: "SELECT"}, {Role: "app", Privilege: "SELECT", Login: true}},
			want:   "note: grants to logins are not captured, since no statement here creates a login: 1 grant to app\n",
		},
		{
			name: "grants to two logins",
			grants: []catalog.Grant{
				{Role: "lgx", Privilege: "SELECT", Login: true}, {Role: "app", Privilege: "SELECT", Login: true},
				{Role: "app", Privilege: "INSERT", Login: true},
			},
			want: "note: grants to logins are not captured, since no statement here creates a login: 3 grants to app, lgx\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			logingrants.Report(&out, &catalog.Database{Grants: test.grants})

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReport_NoStream writes nothing without a stream or a schema.
func TestReport_NoStream(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	schema := &catalog.Database{Grants: []catalog.Grant{{Role: "app", Privilege: "SELECT", Login: true}}}

	logingrants.Report(nil, schema)
	logingrants.Report(&out, nil)

	c.Assert(out.String(), qt.Equals, "")
}
