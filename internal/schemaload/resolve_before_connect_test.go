package schemaload_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemasource"
	"ptah.run/internal/schemaload"
)

// TestResolveBeforeConnect_LeavesWhatNeedsADialect pins the boundary of what is
// resolved before a target exists.
//
// An artifact carries the model already, so nothing about resolving it depends
// on the database. Everything else is read against the target's dialect, and a
// source resolved early would be read against no dialect at all.
func TestResolveBeforeConnect_LeavesWhatNeedsADialect(t *testing.T) {
	tests := []struct {
		name string
		opts schemaload.Options
	}{
		{name: "no source at all", opts: schemaload.Options{}},
		{name: "a Go root", opts: schemaload.Options{RootDirs: []string{"./entities"}}},
		{name: "a schema file", opts: schemaload.Options{SchemaFiles: []string{"schema.sql"}}},
		{
			name: "a command",
			opts: schemaload.Options{Commands: []schemasource.Command{{Args: []string{"cat", "schema.sql"}}}},
		},
		{
			// Two sources merge into one composite schema, and merging is read
			// against the dialect like any other source.
			name: "an artifact beside a file",
			opts: schemaload.Options{SchemaFiles: []string{"oci://example.test/schema:v1", "extra.sql"}},
		},
		{
			name: "an artifact beside a Go root",
			opts: schemaload.Options{
				RootDirs:    []string{"./entities"},
				SchemaFiles: []string{"oci://example.test/schema:v1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result, isArtifact, err := schemaload.ResolveBeforeConnect(context.Background(), tt.opts)

			c.Assert(err, qt.IsNil)
			c.Assert(isArtifact, qt.IsFalse)
			c.Assert(result, qt.IsNil)
		})
	}
}
