package atlasmigrate

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/txrequire"
	"go.5x5.cz/ptah/migration/planner"
)

// AtlasTxModeNoneDirective is the Atlas file directive that keeps a migration
// file from running inside a transaction. Atlas places file directives as
// leading comment lines separated from the statements by a blank line; Ptah's
// migrator and Atlas's migrate apply both honor this form.
const AtlasTxModeNoneDirective = "-- atlas:txmode none"

// MigrationFileContent is one planned migration produced by a single migrate
// diff run.
//
// It is named for a FILE because the native Atlas layout writes one, but a
// planned migration is not a file on every layout this verb can be pointed at:
// golang-migrate and flyway split it into a forward file and a rollback file,
// goose and dbmate keep both halves under directives in one file, and liquibase
// wraps them in a changeset. [composeMigrationArtifacts] is what turns this
// into the files of a given layout; everything below is what those five
// compositions need, rendered once here rather than five times there.
type MigrationFileContent struct {
	// NameSuffix is appended to the operator-chosen migration name when the
	// plan splits into several files ("" for a single-file plan).
	NameSuffix string
	// SQL is the complete forward file content, including any Atlas file
	// directives.
	SQL string
	// DownSQL is the complete rollback file content, rendered exactly as SQL
	// is. It is empty when the run planned no reverse — which is every run on
	// the native Atlas layout, whose migration files carry no rollback half at
	// all, and any run whose caller supplied no
	// [DiffOptions.PlanBidirectional].
	DownSQL string
	// Statements are the forward statements SQL renders, in apply order, each
	// carrying whatever leading comment the planner emitted for it.
	//
	// It exists for liquibase, the one layout that cannot take the rendered
	// body: its rollback is attached to a changeset rather than appended as a
	// block, so that layout composes the file from the statements instead of
	// from SQL (see [composeLiquibaseArtifact]).
	Statements []string
	// ReverseStatements are the statements that undo Statements, in the order
	// they must run. Empty exactly when DownSQL is.
	ReverseStatements []string
	// ReverseNoTransaction reports that the rollback contains statements the
	// database refuses inside a transaction block.
	ReverseNoTransaction bool
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
	if unsplittable := unsplittableNoTransactionNodes(noTransaction); len(unsplittable) > 0 {
		return nil, txrequire.UnsplittableMixError(unsplittable)
	}

	// The order of these files is the order the statements have to run in.
	// `ALTER TYPE ... ADD VALUE` leads, because PostgreSQL answers 55P04 to a
	// statement that uses the value before the ADD VALUE has committed. The
	// concurrent indexes follow, because an index is built after the table it
	// indexes. A plan with no enum additions produces exactly the two files it
	// produced before (stokaro/ptah#1714).
	enumNodes, concurrentNodes := partitionNoTransactionNodes(noTransaction)
	contents := make([]MigrationFileContent, 0, 3)
	for _, group := range []struct {
		nodes         []ast.Node
		suffix        string
		noTransaction bool
	}{
		{enumNodes, "_enum_values", true},
		{transactional, "_transactional", false},
		{concurrentNodes, "_concurrent_indexes", true},
	} {
		if len(group.nodes) == 0 {
			continue
		}
		content, err := renderMigrationFileContent(dialect, caps, format, group.nodes)
		if err != nil {
			return nil, err
		}
		if group.noTransaction {
			content = withTxModeNoneDirective(content)
		}
		content.NameSuffix = group.suffix
		contents = append(contents, content)
	}
	return contents, nil
}

// partitionNoTransactionNodes splits the statements that must leave the
// transactional file into the two kinds this package can order, preserving
// input order within each.
func partitionNoTransactionNodes(nodes []ast.Node) (enumValues, concurrentIndexes []ast.Node) {
	for _, node := range nodes {
		if txrequire.Kind(node) == txrequire.KindEnumValue {
			enumValues = append(enumValues, node)
			continue
		}
		concurrentIndexes = append(concurrentIndexes, node)
	}
	return enumValues, concurrentIndexes
}

// unsplittableNoTransactionNodes returns the statements this package has no
// ordered place for.
func unsplittableNoTransactionNodes(nodes []ast.Node) []ast.Node {
	var unsplittable []ast.Node
	for _, node := range nodes {
		if txrequire.Kind(node) == txrequire.KindUnsplittable {
			unsplittable = append(unsplittable, node)
		}
	}
	return unsplittable
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
	return MigrationFileContent{SQL: sql, Statements: statements}, nil
}

// attachReversePlan records the reverse of the whole plan on the LAST file of
// it, and renders that reverse the same way the forward half was rendered.
//
// The last file, and not each file, because a rollback runs newest-first: the
// files of one diff run are staged at consecutive versions, so the last one is
// the first to be undone and undoing it has to undo the whole run. Splitting
// the reverse across the files of a plan would require knowing which forward
// statement each reverse statement answers, which the planner does not report —
// it plans the reverse of the run, in reverse dependency order, as one set.
//
// A plan that produced no reverse leaves every file's DownSQL empty, which is
// what the native Atlas layout wants in every case: its migration files carry
// no rollback half.
func attachReversePlan(
	contents []MigrationFileContent,
	reverse []string,
	format string,
	requiresNoTransaction bool,
) ([]MigrationFileContent, error) {
	if len(contents) == 0 || len(reverse) == 0 {
		return contents, nil
	}
	sql, err := renderMigrationDiffSQL(reverse, format)
	if err != nil {
		return nil, err
	}
	last := len(contents) - 1
	contents[last].DownSQL = sql
	contents[last].ReverseStatements = reverse
	contents[last].ReverseNoTransaction = requiresNoTransaction
	return contents, nil
}

// validateForeignTransactionMode refuses a foreign artifact unless its source
// format can represent the planned execution requirement. Goose is the one
// proven foreign format here: `-- +goose NO TRANSACTION` governs its whole file,
// which contains both the up and down sections. The other formats remain
// fail-closed until their directional metadata is measured and implemented.
// formatExpressesNoTransaction reports the layouts that have a marker of their
// own for a migration that must not run inside a transaction.
//
// Goose publishes a whole-file `-- +goose NO TRANSACTION`; dbmate takes
// `transaction:false` on each direction's own directive line. The layouts left
// out are left out for a measured reason and not for want of trying:
// golang-migrate documents no per-file mechanism at all -- its own guidance for
// CREATE INDEX CONCURRENTLY is to put the statement in a separate migration --
// so writing one would be inventing syntax the tool that owns the format would
// not read (stokaro/ptah#1630).
func formatExpressesNoTransaction(format atlasmigrateimport.Format) bool {
	switch format {
	case atlasmigrateimport.FormatGoose, atlasmigrateimport.FormatDBMate:
		return true
	default:
		return false
	}
}

func validateForeignTransactionMode(
	format atlasmigrateimport.Format,
	content MigrationFileContent,
) error {
	if ReadsNativeAtlasDir(format) || formatExpressesNoTransaction(format) {
		return nil
	}
	if content.NoTransaction {
		return fmt.Errorf(
			"migration directory format %q cannot safely express a forward migration that requires no-transaction execution; no migration files or atlas.sum were written",
			format,
		)
	}
	if !content.ReverseNoTransaction {
		return nil
	}
	return fmt.Errorf(
		"migration directory format %q cannot safely express a rollback that requires no-transaction execution; no migration files or atlas.sum were written",
		format,
	)
}

func validateMigrationFileContentsTransactionModes(
	format atlasmigrateimport.Format,
	contents []MigrationFileContent,
) error {
	for _, content := range contents {
		if err := validateForeignTransactionMode(format, content); err != nil {
			return err
		}
	}
	return nil
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

func hasActualSQLStatements(statements []string) bool {
	for _, statement := range statements {
		if strings.TrimSpace(sqlutil.StripComments(statement)) != "" {
			return true
		}
	}
	return false
}
