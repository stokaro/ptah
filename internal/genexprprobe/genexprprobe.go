// Package genexprprobe turns a desired schema into the probes that ask a dev
// database how it spells each declared generated expression.
//
// It exists as its own package because the two halves it joins may not import
// each other: [ptah.run/internal/dbexprprobe] must stay free of the
// renderer, and the renderer knows nothing about connections. What is left
// over is this — a pure function from a declaration to a list of statements.
package genexprprobe

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/modelast"
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
//
// All probe tables are rendered in one selected batch. Missing output, reported
// omissions, refusal, service failure, or cancellation returns no probes.
func For(ctx context.Context, service renderer.Service, dialect string, caps capability.Capabilities, declared *schemamodel.Database) ([]dbexprprobe.GeneratedExpressionProbe, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return nil, err
	}
	if declared == nil || !rewritesStoredExpressions(dialect) {
		return nil, nil
	}

	var probes []dbexprprobe.GeneratedExpressionProbe
	var nodes []ast.Node
	for _, table := range declared.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields := fieldsForTable(declared, table)
		desired := generatedColumnNames(fields)
		if len(desired) == 0 {
			continue
		}
		probeTable := dbexprprobe.GeneratedExpressionProbeTable(len(probes))
		node := modelast.FromTable(table, fields, declared.Enums, dialect)
		if node == nil {
			continue
		}
		// The rendered statement has to name the probe table, and the node is a
		// fresh value from FromTable, so renaming it here changes nothing the
		// caller holds.
		node.Name = probeTable
		nodes = append(nodes, node)
		probes = append(probes, dbexprprobe.GeneratedExpressionProbe{
			Schema:     table.Schema,
			Table:      table.Name,
			ProbeTable: probeTable,
			Generated:  desired,
		})
	}
	if len(probes) == 0 {
		return nil, nil
	}
	result, err := renderer.Render(ctx, service, renderer.Request{Target: dialect, Capabilities: caps, Nodes: nodes})
	if err != nil {
		return nil, fmt.Errorf("render generated-expression probes: %w", err)
	}
	if len(result.Omissions) != 0 {
		return nil, fmt.Errorf("%w: generated-expression probe rendering omitted declarations", ptaherr.ErrUnsupportedFeature)
	}
	for index, fragment := range result.Fragments {
		// Oracle's driver refuses a terminator on a single statement.
		statement := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(fragment), ";"))
		if statement == "" {
			return nil, fmt.Errorf("%w: generated-expression probe %s produced no SQL", renderer.ErrInvalidResult, probes[index].ProbeTable)
		}
		probes[index].Create = statement
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

func fieldsForTable(declared *schemamodel.Database, table schemamodel.Table) []schemamodel.Field {
	var fields []schemamodel.Field
	for _, field := range declared.Fields {
		if field.StructName == table.StructName {
			fields = append(fields, field)
		}
	}
	return fields
}

func generatedColumnNames(fields []schemamodel.Field) []string {
	var names []string
	for _, field := range fields {
		if strings.TrimSpace(field.GeneratedExpression) != "" {
			names = append(names, field.Name)
		}
	}
	return names
}
