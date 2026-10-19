package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// scopedCleanup is a descriptor whose cleanup is schema-scoped, with the
// savepoint such a cleanup retries a refused drop under.
func scopedCleanup() descriptor.Dialect {
	described := head("alpha")
	described.Cleanup = descriptor.Cleanup{SchemaScoped: true, Restrict: true}
	described.Savepoint = descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"}
	return described
}

func TestNewRegistry_Cleanup_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		described descriptor.Dialect
	}{
		{name: "a schema-scoped cleanup with a savepoint", described: scopedCleanup()},
		{name: "a savepoint with no release", described: withCleanup(func(d *descriptor.Dialect) {
			d.Savepoint = descriptor.Savepoint{Create: "SAVE TRANSACTION", Rollback: "ROLLBACK TRANSACTION"}
		})},
		{name: "no savepoint on an engine whose cleanup is not scoped", described: withCleanup(func(d *descriptor.Dialect) {
			d.Cleanup.SchemaScoped = false
			d.Savepoint = descriptor.Savepoint{}
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.described)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Equal(test.described), qt.IsTrue)
		})
	}
}

func TestNewRegistry_Cleanup_FailurePath(t *testing.T) {
	const refusal = `invalid dialect descriptor: dialect "alpha": a schema-scoped cleanup retries a refused drop under a savepoint, ` +
		`so the dialect states how a savepoint is created and rolled back`
	tests := []struct {
		name string
		edit descriptor.Dialect
	}{
		{name: "no savepoint", edit: withCleanup(func(d *descriptor.Dialect) { d.Savepoint = descriptor.Savepoint{} })},
		{name: "a savepoint that cannot be rolled back", edit: withCleanup(func(d *descriptor.Dialect) { d.Savepoint.Rollback = "" })},
		{name: "a savepoint that cannot be created", edit: withCleanup(func(d *descriptor.Dialect) { d.Savepoint.Create = "" })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.edit)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, refusal)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// withCleanup returns scopedCleanup with one change applied, so each row
// carries the one value it is about.
func withCleanup(change func(*descriptor.Dialect)) descriptor.Dialect {
	described := scopedCleanup()
	change(&described)
	return described
}
