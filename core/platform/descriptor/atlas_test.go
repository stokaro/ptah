package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// withAtlas returns head(name) standing in the Atlas contract as atlas.
func withAtlas(name string, atlas descriptor.Atlas) descriptor.Dialect {
	described := head(name)
	described.Atlas = atlas
	return described
}

// everyAtlasName is an Atlas section that states a value in every list.
func everyAtlasName() descriptor.Atlas {
	return descriptor.Atlas{
		DockerImages:  []string{"alpha", "alpha-geo"},
		DockerDrivers: []string{"alpha"},
		Extension:     true,
		Schema:        "main",
		HCL: descriptor.AtlasHCL{
			TableProperties:  []string{"charset", "auto_increment"},
			ColumnProperties: []string{"on_update"},
			IndexProperties:  []string{"parser"},
			RefusedBlocks:    []string{"sequence"},
			ColumnTypes:      []string{"int4", "text"},
		},
	}
}

func TestNewRegistry_Atlas_HappyPath(t *testing.T) {
	c := qt.New(t)
	shared := descriptor.Atlas{HCL: descriptor.AtlasHCL{TableProperties: []string{"charset"}}}

	registry, err := descriptor.NewRegistry(withAtlas("alpha", everyAtlasName()), withAtlas("beta", shared))

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.Atlas, qt.DeepEquals, everyAtlasName())
	got, found = registry.Lookup("beta")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.Atlas, qt.DeepEquals, shared)
}

func TestNewRegistry_Atlas_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialects []descriptor.Dialect
		wantIs   error
		wantErr  string
	}{
		{
			name:     "a docker image in upper case",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{DockerImages: []string{"Alpha"}})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha": docker image "Alpha" must be .*`,
		},
		{
			name:     "a docker driver stated twice",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{DockerDrivers: []string{"alpha", "alpha"}})},
			wantIs:   descriptor.ErrInvalidDescriptor,
			wantErr:  `invalid dialect descriptor: dialect "alpha" states docker driver "alpha" twice`,
		},
		{
			name: "an empty table property",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{
				HCL: descriptor.AtlasHCL{TableProperties: []string{""}},
			})},
			wantIs:  descriptor.ErrInvalidDescriptor,
			wantErr: `invalid dialect descriptor: dialect "alpha": HCL table property "" must be .*`,
		},
		{
			name: "a column property with a space",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{
				HCL: descriptor.AtlasHCL{ColumnProperties: []string{"on update"}},
			})},
			wantIs:  descriptor.ErrInvalidDescriptor,
			wantErr: `invalid dialect descriptor: dialect "alpha": HCL column property "on update" must be .*`,
		},
		{
			name: "an index property stated twice",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{
				HCL: descriptor.AtlasHCL{IndexProperties: []string{"parser", "parser"}},
			})},
			wantIs:  descriptor.ErrInvalidDescriptor,
			wantErr: `invalid dialect descriptor: dialect "alpha" states HCL index property "parser" twice`,
		},
		{
			name: "a refused block beginning with a digit",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{
				HCL: descriptor.AtlasHCL{RefusedBlocks: []string{"1sequence"}},
			})},
			wantIs:  descriptor.ErrInvalidDescriptor,
			wantErr: `invalid dialect descriptor: dialect "alpha": refused HCL block "1sequence" must be .*`,
		},
		{
			name: "a column type in upper case",
			dialects: []descriptor.Dialect{withAtlas("alpha", descriptor.Atlas{
				HCL: descriptor.AtlasHCL{ColumnTypes: []string{"INTEGER"}},
			})},
			wantIs:  descriptor.ErrInvalidDescriptor,
			wantErr: `invalid dialect descriptor: dialect "alpha": HCL column type "INTEGER" must be .*`,
		},
		{
			name: "a docker image two dialects claim",
			dialects: []descriptor.Dialect{
				withAtlas("alpha", descriptor.Atlas{DockerImages: []string{"shared"}}),
				withAtlas("beta", descriptor.Atlas{DockerImages: []string{"beta", "shared"}}),
			},
			wantIs:  descriptor.ErrConflict,
			wantErr: `conflicting dialect descriptor: dialect "beta" claims docker image "shared", which dialect "alpha" already claims`,
		},
		{
			name: "a docker driver two dialects claim",
			dialects: []descriptor.Dialect{
				withAtlas("alpha", descriptor.Atlas{DockerDrivers: []string{"shared"}}),
				withAtlas("beta", descriptor.Atlas{DockerDrivers: []string{"shared"}}),
			},
			wantIs:  descriptor.ErrConflict,
			wantErr: `conflicting dialect descriptor: dialect "beta" claims docker driver "shared", which dialect "alpha" already claims`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialects...)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// TestRegistry_With_RefusesAClaimedDockerImage pins that a registration
// cannot take a docker image from a dialect the registry already holds, so a
// dev URL keeps starting the engine it started.
func TestRegistry_With_RefusesAClaimedDockerImage(t *testing.T) {
	c := qt.New(t)
	base, err := descriptor.NewRegistry(withAtlas("alpha", descriptor.Atlas{DockerImages: []string{"alpha"}}))
	c.Assert(err, qt.IsNil)

	extended, err := base.With(withAtlas("beta", descriptor.Atlas{DockerImages: []string{"alpha"}}))

	c.Assert(err, qt.ErrorIs, descriptor.ErrConflict)
	c.Assert(extended, qt.IsNil)
}
