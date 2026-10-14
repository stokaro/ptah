package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// connecting is a descriptor of a dialect a connection opens with driver and
// whose driver reads scheme in place of its own.
func connecting(driver, scheme string) descriptor.Dialect {
	described := head("alpha")
	described.Driver = driver
	described.WireScheme = scheme
	return described
}

func TestNewRegistry_ConnectionFields_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect descriptor.Dialect
	}{
		{name: "a dialect Ptah does not connect to", dialect: connecting("", "")},
		{name: "a driver that reads the dialect's own scheme", dialect: connecting("alpha-driver", "")},
		{name: "a driver that reads another scheme", dialect: connecting("wire", "beta+tcp")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Driver, qt.Equals, test.dialect.Driver)
			c.Assert(got.WireScheme, qt.Equals, test.dialect.WireScheme)
		})
	}
}

func TestNewRegistry_ConnectionFields_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect descriptor.Dialect
		wantErr string
	}{
		{name: "a driver with a space", dialect: connecting("my driver", ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": driver "my driver" must be .*`},
		{name: "an upper-case driver", dialect: connecting("PGX", ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": driver "PGX" must be .*`},
		{name: "a wire scheme with a colon", dialect: connecting("wire", "postgres:"),
			wantErr: `invalid dialect descriptor: dialect "alpha": wire scheme "postgres:" must be .*`},
		{name: "a wire scheme beginning with a digit", dialect: connecting("wire", "9p"),
			wantErr: `invalid dialect descriptor: dialect "alpha": wire scheme "9p" must be .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}
