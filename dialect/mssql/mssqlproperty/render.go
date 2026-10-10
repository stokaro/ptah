package mssqlproperty

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// Handlers returns the render descriptor of an [Operation].
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&Operation{}, ast.StatementExtension, validate, render)}
}

func validate(ctx renderer.ExtensionContext, value *Operation) error {
	if platform.NormalizeDialect(ctx.Target) != platform.SQLServer {
		return renderer.UnsupportedExtension(ctx.Target, OperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

// procedures maps an action onto the procedure that performs it. The three
// take the same address arguments and differ only here and in whether @value
// is passed.
var procedures = map[Action]string{Add: "sp_addextendedproperty", Update: "sp_updateextendedproperty", Drop: "sp_dropextendedproperty"}

// render writes one of SQL Server's three extended-property procedures.
//
// Every argument is a string literal, the names of the objects the property
// hangs off included, and that is the procedure's contract rather than a
// choice: @level1name is a sysname, so bracket quoting an identifier would
// write a property onto an object literally called `[docs]` (measured on SQL
// Server 2022: the procedure accepts the brackets and stores them).
//
// The address is written level by level and stops where the property stops:
// a database property passes no level, a schema property level 0, a table
// level 1 and a column level 2. Passing a level with an empty name is not the
// same as omitting it. A drop passes no @value, which
// sp_dropextendedproperty refuses (`has too many arguments specified`).
func render(_ renderer.ExtensionContext, value *Operation) ([]string, error) {
	property := value.Property
	arguments := []string{"@name = N" + literal(property.Name)}
	if value.Action != Drop {
		arguments = append(arguments, "@value = N"+literal(property.Value))
	}
	for _, level := range []struct{ number, kind, name string }{
		{"0", "SCHEMA", property.Schema}, {"1", "TABLE", property.Table}, {"2", "COLUMN", property.Column},
	} {
		if strings.TrimSpace(level.name) == "" {
			break
		}
		arguments = append(arguments, fmt.Sprintf("@level%stype = N'%s'", level.number, level.kind), fmt.Sprintf("@level%sname = N%s", level.number, literal(level.name)))
	}
	statement := fmt.Sprintf("EXEC %s %s;", procedures[value.Action], strings.Join(arguments, ", "))
	if property.Comment != "" {
		return []string{"-- " + property.Comment, statement}, nil
	}
	return []string{statement}, nil
}

func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
