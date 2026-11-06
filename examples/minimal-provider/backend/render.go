package backend

import (
	"context"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
)

// columnTypes are the column types the tutorial engine stores.
var columnTypes = []string{"INTEGER", "TEXT", "REAL", "BLOB"}

// Render writes one SQL statement for each change Ptah planned, in the order
// the changes arrive. It connects to nothing. A change it cannot write
// refuses the whole batch, so Ptah never receives part of a plan.
func (*Provider) Render(_ context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		statement, err := renderChange(change)
		if err != nil {
			return nil, err
		}
		result.Units = append(result.Units, &protocol.Unit{
			ChangeId: change.GetId(),
			Body:     &protocol.Unit_Sql{Sql: statement},
		})
	}
	return result, nil
}

// renderChange writes the statement of one change.
func renderChange(change *protocol.Change) (string, error) {
	object := change.GetObject()
	if object.GetName() == "" || object.GetSchema() != "" {
		return "", refuse("a tutorial table has a name and no schema qualifier")
	}
	table := quote(object.GetName())
	switch operation := change.GetOperation().(type) {
	case *protocol.Change_CreateTable:
		return createTable(table, operation.CreateTable)
	case *protocol.Change_DropTable:
		return "DROP TABLE " + table + ";", nil
	case *protocol.Change_AddColumn:
		if operation.AddColumn.GetPrimaryKey() {
			return "", refuse("the tutorial engine cannot add a primary key column to an existing table")
		}
		column, err := columnDefinition(operation.AddColumn)
		if err != nil {
			return "", err
		}
		return "ALTER TABLE " + table + " ADD COLUMN " + column + ";", nil
	case *protocol.Change_DropColumn:
		return "ALTER TABLE " + table + " DROP COLUMN " + quote(operation.DropColumn) + ";", nil
	default:
		return "", refuse("the tutorial engine creates and drops tables and adds and drops columns, nothing else")
	}
}

// createTable writes the CREATE TABLE statement of a table. Inspect compares
// the definition SQLite stored with this text to prove it read the whole
// table.
func createTable(name string, table *protocol.Table) (string, error) {
	if len(table.GetColumns()) == 0 || len(table.GetFacets()) != 0 {
		return "", refuse("a tutorial table has columns and no engine settings")
	}
	columns := make([]string, 0, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		definition, err := columnDefinition(column)
		if err != nil {
			return "", err
		}
		columns = append(columns, definition)
	}
	return "CREATE TABLE " + name + " (" + strings.Join(columns, ", ") + ");", nil
}

// columnDefinition writes one column: its name, type and constraints.
func columnDefinition(column *protocol.Column) (string, error) {
	kind := strings.ToUpper(column.GetType())
	switch {
	case column.GetName() == "":
		return "", refuse("a column has no name")
	case !slices.Contains(columnTypes, kind):
		return "", refuse("the tutorial engine stores the column types INTEGER, TEXT, REAL and BLOB only")
	case column.DefaultExpression != nil || len(column.GetFacets()) != 0:
		return "", refuse("a tutorial column has no default and no engine settings")
	case column.GetPrimaryKey() && column.GetNullable():
		return "", refuse("a primary key column of the tutorial engine is declared NOT NULL")
	}
	definition := quote(column.GetName()) + " " + kind
	if !column.GetNullable() {
		definition += " NOT NULL"
	}
	if column.GetPrimaryKey() {
		definition += " PRIMARY KEY"
	}
	return definition, nil
}

// quote writes a name as a quoted SQL identifier.
func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// refuse fails a call with a reason Ptah shows to the operator. A reason
// names what failed and why. It never quotes a statement, a value, an address
// or a credential.
func refuse(reason string) error {
	return runtime.ErrorWithReason(codes.InvalidArgument, reason)
}
