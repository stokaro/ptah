package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

func TestCreateTriggerNode_FunctionArgumentsCopy(t *testing.T) {
	c := qt.New(t)
	input := []string{"updated_at"}
	node := ast.NewCreateTrigger("touch", "notes").SetFunctionArguments(input)
	input[0] = "changed"
	c.Assert(node.FunctionArguments(), qt.DeepEquals, []string{"updated_at"})

	readback := node.FunctionArguments()
	readback[0] = "changed"
	c.Assert(node.FunctionArguments(), qt.DeepEquals, []string{"updated_at"})

	copiedNode := *node
	copiedNode.SetFunctionArguments([]string{"created_at"})
	c.Assert(node.FunctionArguments(), qt.DeepEquals, []string{"updated_at"})
	c.Assert(copiedNode.FunctionArguments(), qt.DeepEquals, []string{"created_at"})
	c.Assert(*node == copiedNode, qt.IsFalse)
}
