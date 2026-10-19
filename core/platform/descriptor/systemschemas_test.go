package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// TestNewRegistry_SystemSchemas_HappyPath accepts a schema in more than one
// list, since each list states one thing a reader does with it, and keeps the
// lists as stated.
func TestNewRegistry_SystemSchemas_HappyPath(t *testing.T) {
	c := qt.New(t)
	described := head("alpha")
	described.SystemSchemas = descriptor.SystemSchemas{
		Catalog:  []string{"alpha_catalog", "alpha_sys"},
		Reserved: []string{"alpha_catalog"},
		Implicit: []string{"public", "alpha_sys"},
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
