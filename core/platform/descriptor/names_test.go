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
		{name: "an undeclared foreign key naming", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = "numbered"
			d.IndexNaming.MaxBytes = 64
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key naming "numbered" is not "table-numbered"`},
		{name: "a foreign key named twice", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
			d.IndexNaming.MaxBytes = 64
			d.ConstraintNaming = descriptor.ConstraintNamingTableColumnsSuffix
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key naming "table-numbered" and ` +
				`constraint naming "table-columns-suffix" both name an unnamed foreign key`},
		{name: "a foreign key index named by no rule", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key naming "table-numbered" names ` +
				`the index a key builds, and the index naming states no rule`},
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

// TestNewRegistry_ForeignKeyNaming_HappyPath accepts a foreign key naming
// beside the index naming that names the key's index, and keeps it as stated.
func TestNewRegistry_ForeignKeyNaming_HappyPath(t *testing.T) {
	c := qt.New(t)
	described := head("alpha")
	described.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
	described.IndexNaming = descriptor.IndexNaming{MaxBytes: 64}

	registry, err := descriptor.NewRegistry(described)

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.ForeignKeyNaming, qt.Equals, descriptor.ForeignKeyNamingTableNumbered)
	c.Assert(got, qt.DeepEquals, described)
}
