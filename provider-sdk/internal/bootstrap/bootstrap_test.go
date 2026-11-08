package bootstrap_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/provider-sdk/internal/bootstrap"
)

func TestValidateEnvironment_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := bootstrap.ValidateEnvironment([]string{"PTAH_FIXTURE_LOG=provider.log", "LANG=C", "EMPTY="})

	c.Assert(err, qt.IsNil)
}

func TestValidateEnvironment_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		entries []string
	}{
		{name: "no equals sign", entries: []string{"HOME"}},
		{name: "no name", entries: []string{"=value"}},
		{name: "a line break", entries: []string{"NAME=a\nb"}},
		{name: "a NUL", entries: []string{"NAME=a\x00b"}},
		{name: "a go-plugin name", entries: []string{"PLUGIN_CLIENT_CERT=x"}},
		{name: "a bootstrap name in another case", entries: []string{"ptah_provider_parent_pid=1"}},
		{name: "a name repeated in another case", entries: []string{"NAME=a", "name=b"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := bootstrap.ValidateEnvironment(test.entries)

			c.Assert(err, qt.ErrorIs, bootstrap.ErrEnvironment)
		})
	}
}
