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
		{name: "a negative index base cut", dialect: with(func(d *descriptor.Dialect) {
			d.IndexNaming = descriptor.IndexNaming{MaxBytes: 64, BaseBytes: -1}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": index naming base bytes -1 is negative`},
		{name: "an index base cut that leaves no room for a suffix", dialect: with(func(d *descriptor.Dialect) {
			d.IndexNaming = descriptor.IndexNaming{MaxBytes: 64, BaseBytes: 64}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": index naming base bytes 64 leaves no room for a suffix within max bytes 64`},
		{name: "an index base cut where no index naming is stated", dialect: with(func(d *descriptor.Dialect) {
			d.IndexNaming = descriptor.IndexNaming{BaseBytes: 61}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": index naming base bytes 61 leaves no room for a suffix within max bytes 0`},
		{name: "an undeclared constraint naming", dialect: with(func(d *descriptor.Dialect) { d.ConstraintNaming = "numbered" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": constraint naming "numbered" is not "table-columns-suffix"`},
		{name: "an undeclared DO-block language", dialect: with(func(d *descriptor.Dialect) { d.Statements.DoBlocks = "plv8" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": DO-block language "plv8" is not one of \["plpgsql"\]`},
		{name: "a table option without a property", dialect: with(func(d *descriptor.Dialect) {
			d.TableOptions = []descriptor.TableOption{{Option: "STRICT"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": table option \{Option:STRICT Property:\} names no option or no property`},
		{name: "a table option spelled with an empty word", dialect: with(func(d *descriptor.Dialect) {
			d.TableOptions = []descriptor.TableOption{{Option: "WITHOUT_ROWID", Words: []string{"WITHOUT", ""}, Property: "without_rowid"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": table option "WITHOUT_ROWID" is spelled with "", which is not one word`},
		{name: "a table option spelled with two words in one", dialect: with(func(d *descriptor.Dialect) {
			d.TableOptions = []descriptor.TableOption{{Option: "WITHOUT_ROWID", Words: []string{"WITHOUT ROWID"}, Property: "without_rowid"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": table option "WITHOUT_ROWID" is spelled with "WITHOUT ROWID", which is not one word`},
		{name: "an undeclared check naming", dialect: with(func(d *descriptor.Dialect) { d.CheckNaming = "numbered" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": check naming "numbered" is not one of .*`},
		{name: "a numbered check naming with no identifier limit in characters", dialect: with(func(d *descriptor.Dialect) {
			d.CheckNaming = descriptor.CheckNamingTableNumbered
			d.IdentifierLimit = descriptor.IdentifierLimit{Max: 63, Unit: "bytes"}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": check naming "table-numbered" refuses a derived name ` +
				`longer than the identifier limit in characters, and the identifier limit states no length in characters`},
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
		{name: "a foreign key naming with no identifier limit", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
			d.IndexNaming.MaxBytes = 64
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key naming "table-numbered" refuses a derived name ` +
				`longer than the identifier limit in characters, and the identifier limit states no length in characters`},
		{name: "a foreign key naming with an identifier limit in bytes", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
			d.IndexNaming.MaxBytes = 64
			d.IdentifierLimit = descriptor.IdentifierLimit{Max: 64, Unit: "bytes"}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key naming "table-numbered" refuses a derived name ` +
				`longer than the identifier limit in characters, and the identifier limit states no length in characters`},
		{name: "a CREATE TABLE limit past the identifier limit", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
			d.IndexNaming.MaxBytes = 64
			d.IdentifierLimit = descriptor.IdentifierLimit{Max: 64, Unit: "characters"}
			d.ForeignKeyNameRules.CreateTableMaxChars = 65
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key name rules: CREATE TABLE max chars 65 ` +
				`is not between 0 and the identifier limit 64`},
		{name: "a negative CREATE TABLE limit", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
			d.IndexNaming.MaxBytes = 64
			d.IdentifierLimit = descriptor.IdentifierLimit{Max: 64, Unit: "characters"}
			d.ForeignKeyNameRules.CreateTableMaxChars = -1
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key name rules: CREATE TABLE max chars -1 ` +
				`is not between 0 and the identifier limit 64`},
		{name: "foreign key name rules with no foreign key naming", dialect: with(func(d *descriptor.Dialect) {
			d.ForeignKeyNameRules.BareNumbers = true
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": foreign key name rules are stated, and no foreign key naming is`},
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
// beside the index naming that names the key's index and an identifier limit
// in characters, with the rules that refine it, and keeps them as stated.
func TestNewRegistry_ForeignKeyNaming_HappyPath(t *testing.T) {
	c := qt.New(t)
	described := head("alpha")
	described.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
	described.IndexNaming = descriptor.IndexNaming{MaxBytes: 64, BaseBytes: 63}
	described.IdentifierLimit = descriptor.IdentifierLimit{Max: 64, Unit: "characters"}
	described.ForeignKeyNameRules = descriptor.ForeignKeyNameRules{CreateTableMaxChars: 64, BareNumbers: true}
	described.CheckNaming = descriptor.CheckNamingTableNumbered

	registry, err := descriptor.NewRegistry(described)

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.ForeignKeyNaming, qt.Equals, descriptor.ForeignKeyNamingTableNumbered)
	c.Assert(got, qt.DeepEquals, described)
}
