package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// scoped is a descriptor carrying every field scope.go declares, each at a
// value the checks accept.
func scoped() descriptor.Dialect {
	described := head("alpha")
	described.DefaultSchema = "main"
	described.Schemas = descriptor.Schemas{Selection: descriptor.SchemaSelectionSingle, Implicit: true}
	described.Roles = descriptor.Roles{OutOfScope: descriptor.RoleScopeUserMade, Widenable: true}
	described.Lock = descriptor.Lock{Protocol: descriptor.LockGetLock, UnboundedWaitSeconds: 60}
	described.URLValuedParameters = []string{"monitoring", "metrics"}
	return described
}

func TestNewRegistry_ScopeHappyPath(t *testing.T) {
	c := qt.New(t)

	registry, err := descriptor.NewRegistry(scoped())

	c.Assert(err, qt.IsNil)
	described, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(described, qt.DeepEquals, scoped())
}

func TestNewRegistry_ScopeFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		edit    descriptor.Dialect
		wantErr string
	}{
		{
			name:    "an undeclared schema selection",
			edit:    withScope(func(d *descriptor.Dialect) { d.Schemas.Selection = "catalog" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": schema selection "catalog" is not one of .*`,
		},
		{
			name:    "an implicit schema with no default schema",
			edit:    withScope(func(d *descriptor.Dialect) { d.Schemas.Selection = ""; d.DefaultSchema = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": an implicit or single schema needs a default schema`,
		},
		{
			name:    "a single schema with no default schema",
			edit:    withScope(func(d *descriptor.Dialect) { d.Schemas.Implicit = false; d.DefaultSchema = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": an implicit or single schema needs a default schema`,
		},
		{
			name:    "an undeclared role scope",
			edit:    withScope(func(d *descriptor.Dialect) { d.Roles.OutOfScope = "all" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": role scope "all" is not one of .*`,
		},
		{
			name:    "an undeclared lock protocol",
			edit:    withScope(func(d *descriptor.Dialect) { d.Lock = descriptor.Lock{Protocol: "flock"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": lock protocol "flock" is not one of .*`,
		},
		{
			name:    "a negative unbounded wait",
			edit:    withScope(func(d *descriptor.Dialect) { d.Lock.UnboundedWaitSeconds = -1 }),
			wantErr: `invalid dialect descriptor: dialect "alpha": an unbounded wait of -1 seconds needs the "get-lock" protocol and a positive value`,
		},
		{
			name:    "an unbounded wait on another protocol",
			edit:    withScope(func(d *descriptor.Dialect) { d.Lock.Protocol = descriptor.LockAppLock }),
			wantErr: `invalid dialect descriptor: dialect "alpha": an unbounded wait of 60 seconds needs the "get-lock" protocol and a positive value`,
		},
		{
			name:    "an empty URL-valued parameter",
			edit:    withScope(func(d *descriptor.Dialect) { d.URLValuedParameters = []string{" "} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": URL-valued parameter " " is empty or repeated`,
		},
		{
			name:    "a URL-valued parameter repeated in another case",
			edit:    withScope(func(d *descriptor.Dialect) { d.URLValuedParameters = []string{"monitoring", "Monitoring"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": URL-valued parameter "Monitoring" is empty or repeated`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.edit)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// withScope returns scoped with one change applied, so each failure row
// carries the one value it is about.
func withScope(change func(*descriptor.Dialect)) descriptor.Dialect {
	described := scoped()
	change(&described)
	return described
}
