package atlashcl

import (
	"github.com/hashicorp/hcl/v2/hclsyntax"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// YDB attributes extend HCL for a dialect Atlas does not implement. Their
// validation is shared with Go and YAML declarations.
func (p *parser) indexYDBSettings(block *hclsyntax.Block, index schemamodel.Index) (schemamodel.Index, error) {
	vector, err := p.indexVector(block, index)
	if err != nil {
		return schemamodel.Index{}, err
	}
	if vector != nil {
		if index.Facets, err = index.Facets.With(vector); err != nil {
			return schemamodel.Index{}, p.blockError(block, "index %q: %v", block.Labels[0], err)
		}
	}
	values := make(map[string]string)
	for _, name := range ydbpartition.Attributes() {
		if attr := block.Body.Attributes[name]; attr != nil {
			values[name] = p.exprString(attr)
		}
	}
	partitioning, err := ydbindex.ParseDeclaration(values)
	if err != nil {
		return schemamodel.Index{}, p.blockError(block, "index %q: %v", block.Labels[0], err)
	}
	index.Partitioning = partitioning
	return index, nil
}
