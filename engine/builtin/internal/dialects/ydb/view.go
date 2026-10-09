package ydb

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/ydbcomment"
	"ptah.run/internal/ydbview"
)

// renderCreateView writes one CREATE VIEW with the security clause YDB
// requires on every view; see [ydbview.SecurityClause].
//
// A view in a directory is created at its path, and YDB creates the
// directories with it, as it does for a table. The query reaches the server
// as written. The server keeps its own form of it, which the schema comparison
// reads both sides into, and it resolves every name in the query from the
// database root rather than from the view's directory (measured on 25.1.4.7
// and 26.2.1.14: a view at `app/v` reading `sub/items` looks for
// `/local/sub/items`), so a query names a table by its whole path.
//
// The semicolon goes on a line of its own, so a query that ends with a line
// comment does not swallow it.
func (r *Renderer) renderCreateView(node *ast.CreateViewNode) error {
	subject := "view " + node.Name
	if !r.caps.Has(capability.Views) {
		return refuseKey(capability.Views, subject)
	}
	if err := r.refuseViewDeclarations(subject, node); err != nil {
		return err
	}
	var comment string
	if node.Comment != "" {
		statement := ydbcomment.Statement{Object: ydbcomment.View, Path: ydbscheme.ObjectPath(node.Name), Comment: node.Comment}
		text, err := r.commentStatement(statement, subject)
		if err != nil {
			return err
		}
		comment = text
	}
	body := strings.TrimSpace(node.Body)
	for strings.HasSuffix(body, ";") {
		body = strings.TrimSpace(strings.TrimSuffix(body, ";"))
	}
	r.w.WriteLinef("CREATE VIEW %s %s AS", tablePath(node.Name), ydbview.SecurityClause)
	r.w.WriteLine(body)
	r.w.WriteLine(";")
	if comment != "" {
		r.w.WriteLine(comment)
	}
	return nil
}

// refuseViewDeclarations refuses what a view declaration asks for and YDB
// cannot hold.
func (r *Renderer) refuseViewDeclarations(subject string, node *ast.CreateViewNode) error {
	switch {
	case ydbview.QueryText(node.Body) == "":
		return refuseFact(subject, "a YDB view needs a query")
	case node.Replace:
		// YQL has neither CREATE OR REPLACE VIEW nor ALTER VIEW, so the
		// planner replaces a changed view with DROP VIEW and CREATE VIEW.
		return r.keyed(capability.CreateOrReplaceView, "CREATE OR REPLACE VIEW", "CREATE OR REPLACE VIEW "+node.Name)
	case node.WithCheck:
		// Measured on 25.1.4.7 and 26.2.1.14: a view cannot be written
		// through (INSERT INTO a view answers `No such column`), and WITH
		// CHECK OPTION after the query is accepted as a table hint on its
		// last table, which checks nothing.
		return refuseFact("WITH CHECK OPTION on "+subject,
			"a YDB view cannot be written through, so there is nothing for the option to check")
	case len(node.Attributes) > 0:
		return refuseFact("WITH "+strings.Join(node.Attributes, ", ")+" on "+subject,
			"a YDB view takes the security_invoker option alone, which Ptah writes on every view")
	}
	return nil
}

// renderDropView writes one DROP VIEW. YDB's has no CASCADE (`extraneous
// input 'CASCADE'`), and it never needs one: YDB records no dependency on a
// view, so dropping a view another view reads succeeds and leaves the reader
// failing until the view is back.
func (r *Renderer) renderDropView(node *ast.DropViewNode) error {
	subject := "DROP VIEW " + node.Name
	if !r.caps.Has(capability.Views) {
		return refuseKey(capability.Views, subject)
	}
	if node.Cascade {
		return refuseFact(subject+" CASCADE", "YDB's DROP VIEW has no CASCADE")
	}
	guard := ""
	if node.IfExists {
		if !r.caps.Has(capability.ObjectExistenceGuards) {
			return refuseKey(capability.ObjectExistenceGuards, "DROP VIEW IF EXISTS "+node.Name)
		}
		guard = " IF EXISTS"
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("DROP VIEW%s %s;", guard, tablePath(node.Name))
	return nil
}
