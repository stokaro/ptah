// Package viewrefusal holds the refusal every renderer but SQL Server's makes
// for a view that declares attributes of its own WITH clause.
//
// [ptah.run/core/schemamodel.View.Attributes] carries SQL Server's
// SCHEMABINDING and VIEW_METADATA, which only the SQL Server renderer writes
// and only the SQL Server reader reads back. Another renderer that wrote the
// view without them would create a view that is not the one declared, and
// every later comparison would find the attribute missing and replace the
// view again. So the PostgreSQL family, MySQL, MariaDB, SQLite, ClickHouse
// and Oracle refuse the view by name. The YDB renderer refuses it with a
// message of its own, because YDB has a WITH clause that takes one other
// option.
//
// It also holds the refusal every renderer but PostgreSQL's makes for a view
// or a materialized view that carries PostgreSQL options:
// [ptah.run/core/ast.CreateViewNode.Options] and
// [ptah.run/core/ast.CreateMaterializedViewNode.Options], and the refusal of
// WITH LOCAL CHECK OPTION by the renderers whose target has no level.
package viewrefusal

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
)

// Attributes reports a view's attributes a renderer has no clause for,
// wrapping [ptaherr.ErrUnsupportedFeature], or nil when the view declares
// none.
func Attributes(dialect, view string, attributes []string) error {
	if len(attributes) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: %s: view %s declares WITH %s, which only SQL Server has; a view created without it is not the view declared",
		ptaherr.ErrUnsupportedFeature, dialect, view, strings.Join(attributes, ", "))
}

// Options reports the PostgreSQL options of a view or a materialized view a
// renderer has no clause for, wrapping [ptaherr.ErrUnsupportedFeature], or nil
// when it carries none. kind is "view" or "materialized view".
//
// The options come from the declaration, and a declaration may reach a target
// other than PostgreSQL. Writing the object without them would drop a
// security_invoker or a storage parameter in silence, and the next comparison
// would find it missing again.
func Options(dialect, kind, name string, options []string) error {
	if len(options) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: %s: %s %s carries the PostgreSQL options %s, which this target has no clause for",
		ptaherr.ErrUnsupportedFeature, dialect, kind, name, strings.Join(options, ", "))
}

// LocalCheck reports a view that asks for WITH LOCAL CHECK OPTION on a target
// whose check option has no level, wrapping [ptaherr.ErrUnsupportedFeature],
// or nil when the view does not ask for it.
//
// SQL Server and Oracle take WITH CHECK OPTION alone. Writing that for a LOCAL
// view would create a view that checks rows against the views it reads too,
// and refuses rows the declared view takes.
func LocalCheck(dialect string, view *ast.CreateViewNode) error {
	if !view.WithCheck || !view.LocalCheck {
		return nil
	}
	return fmt.Errorf(
		"%w: %s: view %s asks for WITH LOCAL CHECK OPTION, and this target's check option has no LOCAL level",
		ptaherr.ErrUnsupportedFeature, dialect, view.Name)
}
