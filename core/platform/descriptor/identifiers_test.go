package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// spelled is a descriptor whose identifiers are written by quoting and whose
// column keys are named by naming.
func spelled(quoting descriptor.Quoting, naming descriptor.ColumnKeyNaming) descriptor.Dialect {
	described := head("alpha")
	described.Quoting = quoting
	described.ColumnKeys = naming
	return described
}

func TestNewRegistry_IdentifierFields_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect descriptor.Dialect
	}{
		{name: "the standard spelling", dialect: spelled(descriptor.Quoting{}, "")},
		{name: "every declared value", dialect: spelled(descriptor.Quoting{
			Style: descriptor.QuoteYQL, Bare: descriptor.BareNever, Keywords: []string{"select"}, Paths: true,
		}, descriptor.ColumnKeyTableColumnKey)},
		{name: "bare names everywhere", dialect: spelled(descriptor.Quoting{
			Style: descriptor.QuoteBracket, Bare: descriptor.BareEverywhere,
		}, descriptor.ColumnKeyAfterColumn)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Quoting, qt.DeepEquals, test.dialect.Quoting)
			c.Assert(got.ColumnKeys, qt.Equals, test.dialect.ColumnKeys)
		})
	}
}

func TestNewRegistry_IdentifierFields_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect descriptor.Dialect
		wantErr string
	}{
		{name: "an undeclared quote style", dialect: spelled(descriptor.Quoting{Style: "single"}, ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": quote style "single" is not one of .*`},
		{name: "an undeclared bare policy", dialect: spelled(descriptor.Quoting{Bare: "sometimes"}, ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": bare names "sometimes" is not one of .*`},
		{name: "an empty keyword", dialect: spelled(descriptor.Quoting{Keywords: []string{"select", ""}}, ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty keyword`},
		{name: "an undeclared column key naming", dialect: spelled(descriptor.Quoting{}, "after-table"),
			wantErr: `invalid dialect descriptor: dialect "alpha": column key naming "after-table" is not one of .*`},
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
