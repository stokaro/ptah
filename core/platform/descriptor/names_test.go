package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

func TestNewRegistry_SchemaFileFields_FailurePath(t *testing.T) {
	with := func(change func(*descriptor.Dialect)) descriptor.Dialect {
		described := head("alpha")
		change(&described)
		return described
	}
	tests := []struct {
		name    string
		dialect descriptor.Dialect
		wantErr string
	}{
		{name: "an undeclared unquoted fold", dialect: with(func(d *descriptor.Dialect) { d.Names.Unquoted = "upper" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": unquoted names comparison "upper" is not one of .*`},
		{name: "an undeclared role fold", dialect: with(func(d *descriptor.Dialect) { d.Names.Roles = "catalog_resolved" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": roles names comparison "catalog_resolved" is not one of .*`},
		{name: "an undeclared lookup fold", dialect: with(func(d *descriptor.Dialect) { d.Names.Lookup = "fold" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": lookup names comparison "fold" is not one of .*`},
		{name: "a negative index name length", dialect: with(func(d *descriptor.Dialect) { d.IndexNaming.MaxBytes = -1 }),
			wantErr: `invalid dialect descriptor: dialect "alpha": index naming max bytes -1 is negative`},
		{name: "an undeclared constraint naming", dialect: with(func(d *descriptor.Dialect) { d.ConstraintNaming = "numbered" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": constraint naming "numbered" is not "table-columns-suffix"`},
		{name: "a table option without a property", dialect: with(func(d *descriptor.Dialect) {
			d.TableOptions = []descriptor.TableOption{{Option: "STRICT"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": table option .* names no option or no property`},
		{name: "an undeclared check naming", dialect: with(func(d *descriptor.Dialect) { d.CheckNaming = "numbered" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": check naming "numbered" is not one of .*`},
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
