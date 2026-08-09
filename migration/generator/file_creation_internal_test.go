package generator

// White-box testing required: this file verifies the version-selection helper
// against incomplete migration pairs before exclusive file creation. The
// selected candidate is not exposed by the public generation API independently
// from filesystem publication.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

func TestNextAvailablePtahVersionSkipsVersionWhenEitherDirectionExists(t *testing.T) {
	c := qt.New(t)
	name := "constraint_drift"

	tests := []struct {
		name  string
		names []string
		want  int64
	}{
		{name: "empty directory", names: nil, want: 42},
		{
			name:  "only the up half exists",
			names: []string{migrator.GenerateMigrationFileName(42, name, "up")},
			want:  43,
		},
		{
			name:  "only the down half exists",
			names: []string{migrator.GenerateMigrationFileName(42, name, "down")},
			want:  43,
		},
		{
			name: "both halves exist",
			names: []string{
				migrator.GenerateMigrationFileName(42, name, "up"),
				migrator.GenerateMigrationFileName(42, name, "down"),
			},
			want: 43,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			version, err := nextAvailablePtahVersion(test.names, 42, name)
			c.Assert(err, qt.IsNil)
			c.Assert(version, qt.Equals, test.want)
		})
	}
}
