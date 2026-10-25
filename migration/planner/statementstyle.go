package planner

import "ptah.run/core/ast"

// withoutDescriptiveComments returns the plan without the comments that only
// describe the statements beside them. A comment that reports something, such
// as a skipped change or a step left to a person, stays where it is.
func withoutDescriptiveComments(nodes []ast.Node) []ast.Node {
	kept := make([]ast.Node, 0, len(nodes))
	for _, node := range nodes {
		if comment, ok := node.(*ast.CommentNode); ok && comment.Descriptive {
			continue
		}
		kept = append(kept, node)
	}
	return kept
}
