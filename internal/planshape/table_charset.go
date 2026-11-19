package planshape

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
)

// withoutUnreadCharsets takes the character set off each change of a table's
// default collation where a catalog read names the character set of neither
// side. information_schema.COLLATIONS lists a collation of a family the target
// lists without one ([descriptor.CollationNames.ListedWithoutCharset]) under
// no character set, and the community binary reads the character set of such
// a table as the schema's. With both sides of the change in such a family it
// sees no change of character set, and writes `COLLATE utf8mb4_uca1400_ai_ci`
// alone where the planner writes `CHARSET utf8mb4, COLLATE
// utf8mb4_uca1400_ai_ci`. A collation names its character set, so the server
// makes the same change from either statement.
//
// The diff says which collation each side holds, so the pass reads both a
// forward plan and its rollback.
func withoutUnreadCharsets(nodes []ast.Node, diff *difftypes.SchemaDiff, dialect string) {
	described, ok := platform.Lookup(dialect)
	families := described.Columns.CollationNames.ListedWithoutCharset
	if !ok || len(families) == 0 || diff == nil {
		return
	}
	unread := make(map[string]bool)
	for _, table := range diff.TablesModified {
		change := table.CollationChange
		if change != nil && listedWithoutCharset(change.Current, families) && listedWithoutCharset(change.Desired, families) {
			unread[table.TableName] = true
		}
	}
	for _, node := range nodes {
		alter, isAlter := node.(*ast.AlterTableNode)
		if !isAlter || !unread[alter.Name] {
			continue
		}
		for _, operation := range alter.Operations {
			if option, isOption := operation.(*ast.SetTableCollationOperation); isOption {
				option.Charset = ""
			}
		}
	}
}

// listedWithoutCharset reports whether collation, `utf8mb4_uca1400_ai_ci` or
// `uca1400_ai_ci`, belongs to one of families.
func listedWithoutCharset(collation string, families []string) bool {
	collation = strings.ToLower(collation)
	_, unqualified, _ := strings.Cut(collation, "_")
	for _, family := range families {
		if strings.HasPrefix(collation, family) || strings.HasPrefix(unqualified, family) {
			return true
		}
	}
	return false
}
