package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// TestNewRegistry_SystemSchemas_HappyPath accepts a schema in more than one
// list, since each list states one thing a reader does with it, a reserved
// schema that starts with a reserved prefix, and an implicit schema whose
// spellings in other cases are reserved, and keeps the lists as stated.
func TestNewRegistry_SystemSchemas_HappyPath(t *testing.T) {
	c := qt.New(t)
	described := head("alpha")
	described.SystemSchemas = descriptor.SystemSchemas{
		Catalog:              []string{"alpha_catalog", "alpha_sys"},
		Reserved:             []string{"alpha_catalog"},
		Implicit:             []string{"public", "alpha_sys"},
		ReservedPrefixes:     []string{"alpha_cat"},
		ReservedCaseVariants: []string{"public", "alpha_sys"},
	}

	registry, err := descriptor.NewRegistry(described)

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.SystemSchemas, qt.DeepEquals, described.SystemSchemas)
}

func TestNewRegistry_SystemSchemas_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		schemas descriptor.SystemSchemas
		wantErr string
	}{
		{
			name:    "an empty catalog schema",
			schemas: descriptor.SystemSchemas{Catalog: []string{"alpha_sys", ""}},
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty catalog system schema`,
		},
		{
			name:    "an empty reserved schema",
			schemas: descriptor.SystemSchemas{Reserved: []string{""}},
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty reserved system schema`,
		},
		{
			name:    "an empty implicit schema",
			schemas: descriptor.SystemSchemas{Implicit: []string{""}},
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty implicit system schema`,
		},
		{
			name:    "a schema named twice in one list",
			schemas: descriptor.SystemSchemas{Catalog: []string{"alpha_sys", "other", "alpha_sys"}},
			wantErr: `invalid dialect descriptor: dialect "alpha": catalog system schema "alpha_sys" is named twice`,
		},
		{
			name:    "an empty reserved prefix",
			schemas: descriptor.SystemSchemas{ReservedPrefixes: []string{""}},
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty reserved prefix system schema`,
		},
		{
			name:    "an empty reserved case variant",
			schemas: descriptor.SystemSchemas{ReservedCaseVariants: []string{"public", ""}},
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty reserved case variant system schema`,
		},
		{
			name:    "a reserved prefix named twice",
			schemas: descriptor.SystemSchemas{ReservedPrefixes: []string{"alpha_", "alpha_"}},
			wantErr: `invalid dialect descriptor: dialect "alpha": reserved prefix system schema "alpha_" is named twice`,
		},
		{
			name:    "an implicit schema with a reserved prefix",
			schemas: descriptor.SystemSchemas{ReservedPrefixes: []string{"alpha_"}, Implicit: []string{"public", "alpha_sys"}},
			wantErr: `invalid dialect descriptor: dialect "alpha": implicit system schema "alpha_sys" starts with reserved prefix "alpha_"; ` +
				`a reserved schema is never created`,
		},
		{
			name:    "a reserved schema that is also implicit",
			schemas: descriptor.SystemSchemas{Reserved: []string{"alpha_sys"}, Implicit: []string{"public", "alpha_sys"}},
			wantErr: `invalid dialect descriptor: dialect "alpha": system schema "alpha_sys" is both reserved and implicit; ` +
				`a reserved schema is never created`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described := head("alpha")
			described.SystemSchemas = test.schemas

			registry, err := descriptor.NewRegistry(described)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}
