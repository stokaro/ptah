package ast_test

// censusVisitor is test support for the node census in node_census_test.go: an
// ast.Visitor that records which method Accept reached and which node it was
// handed, and reads no field of that node. Reading a field would make a node
// whose zero value carries a nil child panic before anything was recorded, and
// the census has to describe every node kind, not the subset with a usable
// zero value.
//
// It does not walk a statement list: Accept hands it the list, and recording
// that is the measurement the census wants.

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
)

// censusCall is one Visitor method call: the method's name and the node that
// method received.
type censusCall struct {
	method string
	node   any
}

// censusVisitor records every call made to it and answers each one with err.
type censusVisitor struct {
	calls []censusCall
	err   error
}

// record appends one call and returns the answer this visitor is configured to
// give, so a caller can measure error propagation as well as dispatch.
func (v *censusVisitor) record(method string, node any) error {
	v.calls = append(v.calls, censusCall{method: method, node: node})
	return v.err
}

// methodsCalled is the ordered list of method names in calls.
func methodsCalled(calls []censusCall) []string {
	methods := make([]string, 0, len(calls))
	for _, call := range calls {
		methods = append(methods, call.method)
	}

	return methods
}

// VisitNode records the call.
//
// The method name it records is derived from the node's concrete type, so a
// node kind added to the AST is recorded without an edit here. That is the
// whole difference the collapse makes to this file: the census measures which
// node reached the visitor, and that question no longer needs one method per
// answer.
func (v *censusVisitor) VisitNode(node ast.Node) error {
	return v.record(censusMethodName(node), node)
}

// censusMethodName is the name the census reports for a node kind.
func censusMethodName(node ast.Node) string {
	name := fmt.Sprintf("%T", node)
	if index := strings.LastIndex(name, "."); index >= 0 {
		name = name[index+1:]
	}
	return "Visit" + strings.TrimSuffix(name, "Node")
}
