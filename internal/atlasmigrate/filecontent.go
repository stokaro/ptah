package atlasmigrate

import (
	"errors"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/migration/planner"
)

// AtlasTxModeNoneDirective is the Atlas file directive that keeps a migration
// file from running inside a transaction. Atlas places file directives as
// leading comment lines separated from the statements by a blank line; Ptah's
// migrator and Atlas's migrate apply both honor this form.
const AtlasTxModeNoneDirective = "-- atlas:txmode none"

// MigrationFileContent is one planned Atlas-format migration file produced by
// a single migrate diff run.
type MigrationFileContent struct {
	// NameSuffix is appended to the operator-chosen migration name when the
	// plan splits into several files ("" for a single-file plan).
	NameSuffix string
	// SQL is the complete file content, including any Atlas file directives.
	SQL string
	// NoTransaction reports whether the file carries the
	// `-- atlas:txmode none` directive.
	NoTransaction bool
}

// BuildMigrationFileContents renders a planned migration AST into Atlas
// migration file contents.
//
// Statements that PostgreSQL rejects inside a transaction block (CREATE INDEX
// CONCURRENTLY, ALTER TYPE ... ADD VALUE) must not silently strip transaction
// safety from ordinary DDL, so the plan is split the same way Ptah's native
// generator splits it:
//
//   - a plan with no such statements becomes one plain file;
//   - a plan made only of such statements becomes one file tagged with the
//     Atlas `-- atlas:txmode none` directive;
//   - a mixed plan whose non-transactional statements are all concurrent
//     index builds becomes two files: the transactional statements first,
//     then a `-- atlas:txmode none` file with the concurrent index builds;
//   - any other mix is refused explicitly.
//
// Atlas itself tags generated concurrent-index migrations with
// `atlas:txmode none`; the two-file split preserves that metadata while
// keeping Ptah's stricter transactional-safety contract.
func BuildMigrationFileContents(
	dialect string,
	caps capability.Capabilities,
	format string,
	nodes []ast.Node,
) ([]MigrationFileContent, error) {
	transactional, noTransaction := splitNoTransactionPlanNodes(dialect, nodes)
	if len(noTransaction) == 0 {
		content, err := renderMigrationFileContent(dialect, caps, format, nodes)
		if err != nil {
			return nil, err
		}
		return []MigrationFileContent{content}, nil
	}
	transactionalSQL, err := renderMigrationStatements(dialect, caps, transactional)
	if err != nil {
		return nil, err
	}
	if !hasActualSQLStatements(transactionalSQL) {
		// Only comments accompany the non-transactional statements: keep the
		// whole plan (comments included, in order) in one no-transaction file.
		content, err := renderMigrationFileContent(dialect, caps, format, nodes)
		if err != nil {
			return nil, err
		}
		return []MigrationFileContent{withTxModeNoneDirective(content)}, nil
	}
	if !nodesAreConcurrentIndexes(noTransaction) {
		return nil, errors.New("generated migration mixes transactional statements with non-transactional statements that cannot be split automatically")
	}
	transactionalContent, err := renderMigrationFileContent(dialect, caps, format, transactional)
	if err != nil {
		return nil, err
	}
	transactionalContent.NameSuffix = "_transactional"
	concurrentContent, err := renderMigrationFileContent(dialect, caps, format, noTransaction)
	if err != nil {
		return nil, err
	}
	concurrentContent = withTxModeNoneDirective(concurrentContent)
	concurrentContent.NameSuffix = "_concurrent_indexes"
	return []MigrationFileContent{transactionalContent, concurrentContent}, nil
}

func renderMigrationFileContent(
	dialect string,
	caps capability.Capabilities,
	format string,
	nodes []ast.Node,
) (MigrationFileContent, error) {
	statements, err := renderMigrationStatements(dialect, caps, nodes)
	if err != nil {
		return MigrationFileContent{}, err
	}
	sql, err := renderMigrationDiffSQL(statements, format)
	if err != nil {
		return MigrationFileContent{}, err
	}
	return MigrationFileContent{SQL: sql}, nil
}

// withTxModeNoneDirective tags one planned file with the Atlas
// `-- atlas:txmode none` header (directives lead the file, separated from the
// statements by a blank line).
func withTxModeNoneDirective(content MigrationFileContent) MigrationFileContent {
	content.SQL = AtlasTxModeNoneDirective + "\n\n" + content.SQL
	content.NoTransaction = true
	return content
}

func renderMigrationStatements(dialect string, caps capability.Capabilities, nodes []ast.Node) ([]string, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	output, err := renderer.RenderSQLWithCapabilities(dialect, caps, nodes...)
	if err != nil {
		return nil, err
	}
	return sqlutil.SplitSQLStatements(output), nil
}

// splitNoTransactionPlanNodes partitions the planned nodes into statements
// that may run inside the migrator's per-file transaction and statements that
// must not, preserving relative order within each group. Comments stay with
// the transactional group, matching the native generator's split.
func splitNoTransactionPlanNodes(dialect string, nodes []ast.Node) (transactional, noTransaction []ast.Node) {
	transactional = make([]ast.Node, 0, len(nodes))
	for _, node := range nodes {
		if planner.NodeRequiresNoTransaction(dialect, node) {
			noTransaction = append(noTransaction, node)
			continue
		}
		transactional = append(transactional, node)
	}
	return transactional, noTransaction
}

func nodesAreConcurrentIndexes(nodes []ast.Node) bool {
	for _, node := range nodes {
		index, ok := node.(*ast.IndexNode)
		if !ok || !index.Concurrently {
			return false
		}
	}
	return true
}

func hasActualSQLStatements(statements []string) bool {
	for _, statement := range statements {
		if strings.TrimSpace(sqlutil.StripComments(statement)) != "" {
			return true
		}
	}
	return false
}
