package yamlschema

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"

	"ptah.run/internal/yamlvalue"
)

// validateOwnerSyntax retains the YAML decoder's structural checks without
// imposing Go integer or float widths on an owner's numeric values. Only the
// temporary graph used for validation has finite numeric scalars retagged.
func validateOwnerSyntax(node *yaml.Node) error {
	cloned, err := syntaxValidationCopy(node, make(map[*yaml.Node]*yaml.Node))
	if err != nil {
		return err
	}
	var checked any
	return cloned.Decode(&checked)
}

func syntaxValidationCopy(node *yaml.Node, copies map[*yaml.Node]*yaml.Node) (*yaml.Node, error) {
	if node == nil {
		return nil, nil
	}
	if cloned, exists := copies[node]; exists {
		return cloned, nil
	}
	cloned := *node
	copies[node] = &cloned
	if node.Kind == yaml.ScalarNode && (node.Tag == "!!int" || node.Tag == "!!float") && !nonFiniteScalar(node) {
		if _, err := yamlvalue.ExactNumber(node.Tag, node.Value); err != nil {
			return nil, fmt.Errorf("line %d: %w", node.Line, err)
		}
		cloned.Tag = "!!str"
	}
	cloned.Content = make([]*yaml.Node, len(node.Content))
	for index, child := range node.Content {
		var err error
		cloned.Content[index], err = syntaxValidationCopy(child, copies)
		if err != nil {
			return nil, err
		}
	}
	var err error
	cloned.Alias, err = syntaxValidationCopy(node.Alias, copies)
	return &cloned, err
}

func nonFiniteScalar(node *yaml.Node) bool {
	if node.Tag != "!!float" {
		return false
	}
	switch strings.ToLower(node.Value) {
	case ".inf", "+.inf", "-.inf", ".nan":
		return true
	default:
		return false
	}
}
