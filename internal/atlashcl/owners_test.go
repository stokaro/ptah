package atlashcl_test

import (
	"context"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
)

// parseWithOwners parses HCL text with the owners the built-in runtime
// registers, whose blocks, such as a synonym, the document may carry.
// atlashcl.Parse selects no owner.
func parseWithOwners(ctx context.Context, data []byte, filename string) (*schemamodel.Database, error) {
	return atlashcl.ParseWithOptions(ctx, data, filename, atlashcl.Options{HCL: builtintest.Runtime().HCL()})
}
