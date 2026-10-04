package ydb

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
	"ptah.run/migration/schemadiff/difftypes"
)

// dropViews drops each view the plan removes and, on a target without
// [capability.CreateOrReplaceView], each view whose query changed, dependents
// first. They run before anything else in the plan.
//
// YDB records no dependency on a view or on the table a view reads: measured
// on 25.1.4.7 and 26.2.1.14, DROP TABLE and DROP VIEW both succeed under a view
// that reads what they drop, and the view fails on every read afterwards. So
// no order is forced on these statements, and the one chosen keeps the plan
// readable to the rule that reports such a drop: no table goes while a view the
// plan replaces still reads it (lint rule YD106).
func (p *Planner) dropViews(diff *difftypes.SchemaDiff) []ast.Node {
	objects := make([]deporder.ViewLike, 0, len(diff.ViewsRemoved)+len(diff.ViewsModified))
	for _, view := range diff.ViewsRemoved {
		objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	if !p.caps.Has(capability.CreateOrReplaceView) {
		for _, viewDiff := range diff.ViewsModified {
			objects = append(objects, deporder.ViewLike{Name: viewDiff.ViewName, Body: viewDiff.PreviousBody})
		}
	}
	ordered := deporder.ViewLikesForCreateForDialect(objects, platform.YDB)
	slices.Reverse(ordered)
	nodes := make([]ast.Node, 0, len(ordered))
	for _, object := range ordered {
		nodes = append(nodes, ast.NewDropView(object.Name))
	}
	return nodes
}

// createViews creates each view the plan adds and each one whose query
// changed, a view after the views it reads. They run last, after the tables
// they read are created and changed: YDB checks a view's query against the
// schema when the view is created (measured: a view over a table that does
// not exist yet is refused with `Cannot find table`), and never again.
//
// A changed view is created again after dropViews dropped it. On a target
// with [capability.CreateOrReplaceView] it is replaced in place instead, and
// the renderer writes the statement the target has for that.
func (p *Planner) createViews(diff *difftypes.SchemaDiff) []ast.Node {
	views := make(map[string]*ast.CreateViewNode, len(diff.ViewsAdded)+len(diff.ViewsModified))
	objects := make([]deporder.ViewLike, 0, len(views))
	add := func(name string, node *ast.CreateViewNode, dependsOn []string) {
		views[name] = node
		objects = append(objects, deporder.ViewLike{Name: name, Body: node.Body, DependsOn: dependsOn})
	}
	for _, view := range diff.ViewsAdded {
		add(view.Name, modelast.FromView(view), view.DependsOn)
	}
	for _, viewDiff := range diff.ViewsModified {
		// The view this change leaves behind travels with it, so a rollback
		// creates the view the database had rather than the declaration.
		view := viewDiff.Desired
		if view.Name == "" {
			continue
		}
		node := modelast.FromView(view)
		if p.caps.Has(capability.CreateOrReplaceView) {
			node.SetReplace()
		}
		add(view.Name, node, view.DependsOn)
	}
	nodes := make([]ast.Node, 0, len(objects))
	for _, object := range deporder.ViewLikesForCreateForDialect(objects, platform.YDB) {
		nodes = append(nodes, views[object.Name])
	}
	return nodes
}

// viewChanges reports a diff that adds, drops or changes a view.
func viewChanges(diff *difftypes.SchemaDiff) bool {
	return len(diff.ViewsAdded)+len(diff.ViewsRemoved)+len(diff.ViewsModified) > 0
}
