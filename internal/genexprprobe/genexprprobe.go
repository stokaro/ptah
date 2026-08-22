// Package genexprprobe turns a desired schema into the probes that ask a dev
// database how it spells each declared generated expression.
//
// It exists as its own package because the two halves it joins may not import
// each other: [go.5x5.cz/ptah/dbschema] must stay free of the renderer, and the
// renderer knows nothing about connections. What is left over is this — a pure
// function from a declaration to a list of statements.
package genexprprobe

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// For returns one probe per declared table that carries a generated column, and
// nothing for a target that stores the expression it was given.
//
// The whole table is rendered rather than the generated column alone, because
// the expression references its siblings: `"size" * 2` needs a `size` column to
// reference, and a probe table carrying only the generated column is refused.
//
// The probe table is renamed rather than reusing the declared name. A dev
// database may legitimately already hold the schema being compared -- that is
// what a dev database is for -- and creating a second table under the same name
// would fail on the first schema that did.
func For(dialect string, caps capability.Capabilities, declared *goschema.Database) ([]dbschema.GeneratedExpressionProbe, error) {
	if declared == nil || !rewritesStoredExpressions(dialect) {
		return nil, nil
	}

	var probes []dbschema.GeneratedExpressionProbe
	for _, table := range declared.Tables {
		fields := fieldsForTable(declared, table)
		generated := generatedColumnNames(fields)
		if len(generated) == 0 {
			continue
		}
		probeTable := dbschema.GeneratedExpressionProbeTable(len(probes))
		node := fromschema.FromTable(table, fields, declared.Enums, dialect)
		if node == nil {
			continue
		}
		// The rendered statement has to name the probe table, and the node is a
		// fresh value from FromTable, so renaming it here changes nothing the
		// caller holds.
		node.Name = probeTable
		statement, err := renderCreateTable(dialect, caps, node)
		if err != nil {
			return nil, err
		}
		probes = append(probes, dbschema.GeneratedExpressionProbe{
			Schema:     table.Schema,
			Table:      table.Name,
			ProbeTable: probeTable,
			Create:     statement,
			Generated:  generated,
		})
	}
	return probes, nil
}

// rewritesStoredExpressions reports that the target stores a rewrite of a
// generated column's expression rather than the text it was given, which is the
// only case a probe is worth its round trip. It is the same fact the comparison
// consults, asked from the side that builds the probes (stokaro/ptah#1915).
func rewritesStoredExpressions(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.Oracle
}

func fieldsForTable(declared *goschema.Database, table goschema.Table) []goschema.Field {
	var fields []goschema.Field
	for _, field := range declared.Fields {
		if field.StructName == table.StructName {
			fields = append(fields, field)
		}
	}
	return fields
}

func generatedColumnNames(fields []goschema.Field) []string {
	var names []string
	for _, field := range fields {
		if strings.TrimSpace(field.GeneratedExpression) != "" {
			names = append(names, field.Name)
		}
	}
	return names
}

// renderCreateTable renders one node and strips the statement terminator, which
// the Oracle driver refuses on a single statement.
func renderCreateTable(dialect string, caps capability.Capabilities, node *ast.CreateTableNode) (string, error) {
	rendered, err := renderer.RenderSQLWithCapabilities(dialect, caps, node)
	if err != nil {
		return "", fmt.Errorf("render generated-expression probe for %s: %w", node.Name, err)
	}
	statement := strings.TrimSpace(rendered)
	statement = strings.TrimSuffix(statement, ";")
	return strings.TrimSpace(statement), nil
}
