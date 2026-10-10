package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestValidText_HappyPath(t *testing.T) {
	for _, value := range []string{"", "orders", "Größe", " spaced "} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaext.ValidText("table name", value), qt.IsNil)
		})
	}
}

func TestValidText_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "invalid UTF-8", value: "or\xffders", wantErr: `invalid feature value: table name is not valid UTF-8`},
		{name: "a NUL byte", value: "or\x00ders", wantErr: `invalid feature value: table name contains a NUL byte`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaext.ValidText("table name", test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
