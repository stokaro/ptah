// Package sqlserver serves the sqlserver routine grammar to the SQL reader.
package sqlserver

import (
	"fmt"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineext"
)

// Services returns the grammars and conversion this owner supplies.
func Services() routineext.Services {
	return routineext.Services{Grammars: []string{descriptor.RoutineTransactSQL}, Converter: converter{}}
}

type converter struct{}

// Convert converts a routine the transact-sql routine grammar read.
//
// EXECUTE AS CALLER is the security mode INVOKER, and every other EXECUTE AS
// is DEFINER, as the reader reports them. A named principal, SCHEMABINDING,
// RECOMPILE and RETURNS NULL ON NULL INPUT are the SQL Server owner's to read,
// and reach the model as the facet its SQL syntax gives the routine. EXECUTE
// AS SELF is refused: the catalog records the principal that ran the CREATE in
// its place, so it would compare back only against whichever account applied
// it. Every other option the header writes, such as ENCRYPTION, is refused by
// name too: the model has no place for it, so it could be neither written nor
// compared back, and keeping the routine without it would create one the
// author did not write.
func (converter) Convert(request routineext.Request) (schemamodel.Function, error) {
	node := request.Node
	security := ""
	for _, option := range node.Options {
		switch {
		case option.Text == "EXECUTE AS CALLER":
			security = "INVOKER"
		case option.Text == "EXECUTE AS OWNER":
			security = "DEFINER"
		case option.Text == "EXECUTE AS SELF":
			return schemamodel.Function{}, fmt.Errorf(
				"the sqlserver %s %s states EXECUTE AS SELF, which the catalog records as the principal that created it, "+
					"so it compares back only against the account that applies it; write EXECUTE AS 'name' for the principal it should run as",
				node.Kind, node.Name)
		case option.Read && strings.HasPrefix(option.Text, "EXECUTE AS "):
			security = "DEFINER"
		case option.Read:
			// An option the owner's facet holds, such as SCHEMABINDING; the
			// facet below carries it.
		case option.Text == "CALLED ON NULL INPUT":
			// The default, and what the catalog reports for a routine that
			// writes nothing.
		default:
			return schemamodel.Function{}, fmt.Errorf(
				"the sqlserver %s %s states %s, which Ptah can neither write nor compare back; "+
					"remove it, or keep the routine out of the schema",
				node.Kind, node.Name, option.Text)
		}
	}
	function := schemamodel.Function{
		Facets:     node.Facets,
		Name:       request.Name,
		Kind:       string(node.Kind),
		Parameters: node.Parameters,
		Returns:    node.Returns,
		// The body is T-SQL, which the catalog reports as sql.
		Language: "sql",
		Security: security,
		Body:     strings.TrimSpace(node.Body.SQL),
	}
	function.Canonicalize()
	return function, nil
}
