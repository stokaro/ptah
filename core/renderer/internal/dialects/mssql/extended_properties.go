package mssql

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
)

// extendedPropertyProcedures maps an operation onto the procedure that
// performs it.
//
// The three take the same address arguments and differ only here and in
// whether @value is passed, which is why one node carries all three.
var extendedPropertyProcedures = map[ast.ExtendedPropertyOperation]string{
	ast.ExtendedPropertyAdd:    "sp_addextendedproperty",
	ast.ExtendedPropertyUpdate: "sp_updateextendedproperty",
	ast.ExtendedPropertyDrop:   "sp_dropextendedproperty",
}

// VisitExtendedProperty renders one of SQL Server's three extended-property
// procedures.
//
// Every argument is a STRING literal, including the names of the objects the
// property hangs off, and that is the procedure's contract rather than a
// choice: sp_addextendedproperty takes @level1name as an sysname, so bracket
// quoting an identifier here would write a property onto an object literally
// called `[docs]`. Measured on SQL Server 2022 -- the procedure accepts the
// brackets and stores them, and the property then belongs to nothing.
//
// The address is written level by level and stops where the declaration stops.
// A DATABASE-scoped property passes no level at all; a schema-scoped one
// passes level 0; a table adds level 1; a column adds level 2. Passing a level
// with an empty name is not the same as omitting it, so the levels are
// appended rather than always written -- and a database property with an empty
// @level0name would be a property on a schema called "", which the procedure
// accepts and which belongs to nothing.
//
// A drop passes no @value. sp_dropextendedproperty does not take one, and
// passing it answers `Procedure or function sp_dropextendedproperty has too
// many arguments specified`.
func (r *Renderer) VisitExtendedProperty(node *ast.ExtendedPropertyNode) error {
	procedure, known := extendedPropertyProcedures[node.Operation]
	if !known {
		return fmt.Errorf("extended property %q: unknown operation %q", node.Name, node.Operation)
	}
	if strings.TrimSpace(node.Table) != "" && strings.TrimSpace(node.Schema) == "" {
		return fmt.Errorf(
			"extended property %q names table %q and no schema; SQL Server addresses a table "+
				"through the schema that holds it", node.Name, node.Table)
	}
	if strings.TrimSpace(node.Column) != "" && strings.TrimSpace(node.Table) == "" {
		return fmt.Errorf(
			"extended property %q names column %q and no table; SQL Server addresses a column "+
				"through the table that holds it", node.Name, node.Column)
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	arguments := []string{"@name = N" + escapeStringLiteral(node.Name)}
	if node.Operation != ast.ExtendedPropertyDrop {
		arguments = append(arguments, "@value = N"+escapeStringLiteral(node.Value))
	}
	if strings.TrimSpace(node.Schema) != "" {
		arguments = append(arguments,
			"@level0type = N'SCHEMA'", "@level0name = N"+escapeStringLiteral(node.Schema))
	}
	if strings.TrimSpace(node.Table) != "" {
		arguments = append(arguments,
			"@level1type = N'TABLE'", "@level1name = N"+escapeStringLiteral(node.Table))
	}
	if strings.TrimSpace(node.Column) != "" {
		arguments = append(arguments,
			"@level2type = N'COLUMN'", "@level2name = N"+escapeStringLiteral(node.Column))
	}

	r.w.WriteLinef("EXEC %s %s;", procedure, strings.Join(arguments, ", "))
	return nil
}
