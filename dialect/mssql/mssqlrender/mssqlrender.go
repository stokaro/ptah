// Package mssqlrender lowers the SQL Server security policy operation to
// T-SQL. It reads only the operation it is handed: no database, no
// comparison. Which statements a change needs is decided once, by
// [mssqldiff.SecurityPolicy.Edit], which the planner reads too.
package mssqlrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/mssql/mssqlast"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Handlers returns the owner's render handlers: the security policy operation
// as a statement of its own, on SQL Server only.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&mssqlast.SecurityPolicy{}, ast.StatementExtension, validate, render),
	}
}

// Registry returns [Handlers] as a registry.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validate(ctx renderer.ExtensionContext, value *mssqlast.SecurityPolicy) error {
	if platform.NormalizeDialect(ctx.Target) != platform.SQLServer {
		return renderer.UnsupportedExtension(ctx.Target, mssqlast.SecurityPolicyKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

// render writes the statements of one change in the order its edit gives. A
// created policy names its state and schema binding even where they are the
// defaults, so the statement says what the policy is without the reader
// knowing them.
func render(_ renderer.ExtensionContext, value *mssqlast.SecurityPolicy) ([]string, error) {
	name := mssqlschema.ObjectName{Schema: value.Schema, Name: value.Name}.String()
	edit := value.Change.Edit()
	var statements []string
	if edit.Drop {
		statements = append(statements, "DROP SECURITY POLICY "+name+";")
	}
	if edit.Create {
		statements = append(statements, create(name, value.Change.After))
	}
	if edit.Disable {
		statements = append(statements, "ALTER SECURITY POLICY "+name+" WITH (STATE = OFF);")
	}
	clauses := make([]string, 0, len(edit.Drops)+len(edit.Alters)+len(edit.Adds))
	for _, predicate := range edit.Drops {
		clauses = append(clauses, clause("DROP", predicate))
	}
	if edit.SeparateDrops && len(clauses) > 0 {
		statements = append(statements, alter(name, clauses))
		clauses = clauses[:0]
	}
	for _, predicate := range edit.Alters {
		clauses = append(clauses, clause("ALTER", predicate))
	}
	for _, predicate := range edit.Adds {
		clauses = append(clauses, clause("ADD", predicate))
	}
	if len(clauses) > 0 {
		statements = append(statements, alter(name, clauses))
	}
	if edit.Enable {
		statements = append(statements, "ALTER SECURITY POLICY "+name+" WITH (STATE = ON);")
	}
	return statements, nil
}

// create writes CREATE SECURITY POLICY with every predicate in canonical order
// and every value named.
func create(name string, policy *mssqlschema.DesiredSecurityPolicy) string {
	enabled, schemaBinding, notForReplication := policy.Resolved()
	var b strings.Builder
	b.WriteString("CREATE SECURITY POLICY " + name)
	for i, predicate := range mssqlschema.SortedPredicates(policy.Predicates) {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n    " + clause("ADD", predicate))
	}
	fmt.Fprintf(&b, "\n    WITH (STATE = %s, SCHEMABINDING = %s)", onOff[enabled], onOff[schemaBinding])
	if notForReplication {
		b.WriteString("\n    NOT FOR REPLICATION")
	}
	b.WriteString(";")
	return b.String()
}

func alter(name string, clauses []string) string {
	return "ALTER SECURITY POLICY " + name + "\n    " + strings.Join(clauses, ",\n    ") + ";"
}

// clause writes one predicate clause. A DROP names only the slot; ADD and
// ALTER name the invocation too, its arguments as declared.
func clause(verb string, predicate mssqlschema.Predicate) string {
	text := verb + " " + string(predicate.Type) + " PREDICATE "
	if verb != "DROP" {
		text += predicate.Function.String() + "(" + strings.Join(predicate.Arguments, ", ") + ") "
	}
	text += "ON " + predicate.Table.String()
	if predicate.Operation != "" {
		text += " " + string(predicate.Operation)
	}
	return text
}

// onOff spells a switch as a WITH option writes it.
var onOff = map[bool]string{false: "OFF", true: "ON"}
