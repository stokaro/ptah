package atlasmigrate

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/internal/atlaschange"
	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/internal/sqlscript"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/planner"
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
	// carrying whatever leading comment the planner emitted for it and the
	// change comment written above it.
	//
	// It exists for liquibase, the one layout that cannot take the rendered
	// body: its rollback is attached to a changeset rather than appended as a
	// block, so that layout composes the file from the statements instead of
	// from SQL (see [composeLiquibaseArtifact]).
	Statements []Statement
	// ReverseStatements are the statements that undo Statements, in the order
	// they must run. Empty exactly when DownSQL is. Each change comment names
	// the forward change the statement undoes, after `reverse: `.
	ReverseStatements []Statement
	// ReverseNoTransaction reports that the rollback contains statements the
	// database refuses inside a transaction block.
	ReverseNoTransaction bool
	// NoTransaction reports whether the file carries the
	// `-- atlas:txmode none` directive.
	NoTransaction bool
}

// Statement is one statement of a migration file.
type Statement struct {
	// SQL is the statement as the renderer wrote it, with any comment the
	// planner put before it.
	SQL string
	// Change names the change the statement makes, as the community binary
	// writes it above the statement: `create "users" table`. It is empty for a
	// statement no change names; see [ptah.run/internal/atlaschange].
	Change string
}

// FileFormat is how the statements of a migration file are written.
type FileFormat struct {
	// Template is the `migrate diff --format` template.
	Template string
	// Foreign marks a layout another tool reads, such as golang-migrate. The
	// community binary writes such a file through its own template, which
	// writes each change comment as the change names it, where the `sql`
	// helper of the native layout writes its first letter in upper case.
	Foreign bool
}

// BuildMigrationFileContents renders a planned migration AST into Atlas
// migration file contents. Each statement is written under the comment that
// names its change, as the community binary writes it.
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
	ctx context.Context,
	service renderer.Service,
	dialect string,
	caps capability.Capabilities,
	format FileFormat,
	nodes []ast.Node,
) ([]MigrationFileContent, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return nil, err
	}
	transactional, noTransaction := splitNoTransactionPlanNodes(dialect, nodes)
	if len(noTransaction) == 0 {
		content, err := renderMigrationFileContent(ctx, service, dialect, caps, format, nodes)
		if err != nil {
			return nil, err
		}
		return []MigrationFileContent{content}, nil
	}
	transactionalSQL, err := renderMigrationStatements(ctx, service, dialect, caps, transactional, atlaschange.Of)
	if err != nil {
		return nil, err
	}
	if !hasActualSQLStatements(transactionalSQL) {
		// Only comments accompany the non-transactional statements: keep the
		// whole plan (comments included, in order) in one no-transaction file.
		content, err := renderMigrationFileContent(ctx, service, dialect, caps, format, nodes)
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
	// indexes. A validation of a constraint already in the database comes last,
	// in a file of its own, so its scan commits apart from everything else. A
	// plan with none of these produces exactly the files it produced before
	// (stokaro/ptah#1714).
	ordered := partitionNoTransactionNodes(noTransaction)
	contents := make([]MigrationFileContent, 0, 4)
	for _, group := range []struct {
		nodes         []ast.Node
		suffix        string
		noTransaction bool
	}{
		{ordered.enumValues, "_enum_values", true},
		{transactional, "_transactional", false},
		{ordered.concurrentIndexes, "_concurrent_indexes", true},
		{ordered.validations, validationSuffix, true},
	} {
		if len(group.nodes) == 0 {
			continue
		}
		content, err := renderMigrationFileContent(ctx, service, dialect, caps, format, group.nodes)
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

// validationSuffix names the file that validates constraints already in the
// database. A golang-migrate run numbers its files after it.
const validationSuffix = "_constraint_validations"

// noTransactionGroups are the statements that leave the transactional file,
// grouped by where their file belongs.
type noTransactionGroups struct {
	enumValues        []ast.Node
	concurrentIndexes []ast.Node
	validations       []ast.Node
}

// partitionNoTransactionNodes splits the statements that must leave the
// transactional file into the kinds this package can order, preserving input
// order within each. A statement of any other kind has been refused already.
func partitionNoTransactionNodes(nodes []ast.Node) noTransactionGroups {
	var groups noTransactionGroups
	for _, node := range nodes {
		switch txrequire.Kind(node) {
		case txrequire.KindEnumValue:
			groups.enumValues = append(groups.enumValues, node)
		case txrequire.KindConstraintValidation:
			groups.validations = append(groups.validations, node)
		default:
			groups.concurrentIndexes = append(groups.concurrentIndexes, node)
		}
	}
	return groups
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
	ctx context.Context,
	service renderer.Service,
	dialect string,
	caps capability.Capabilities,
	format FileFormat,
	nodes []ast.Node,
) (MigrationFileContent, error) {
	statements, err := renderMigrationStatements(ctx, service, dialect, caps, nodes, atlaschange.Of)
	if err != nil {
		return MigrationFileContent{}, err
	}
	sql, err := renderMigrationDiffSQL(dialect, statements, format)
	if err != nil {
		return MigrationFileContent{}, err
	}
	return MigrationFileContent{SQL: sql, Statements: statements}, nil
}

// attachReversePlan records the reverse of the whole plan on the LAST file of
// it that has a rollback to give (see [reverseOwner]), and renders that reverse the same way the forward half was rendered.
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
	dialect string,
	reverse []Statement,
	format FileFormat,
	requiresNoTransaction bool,
) ([]MigrationFileContent, error) {
	if len(contents) == 0 || len(reverse) == 0 {
		return contents, nil
	}
	sql, err := renderMigrationDiffSQL(dialect, reverse, format)
	if err != nil {
		return nil, err
	}
	last := reverseOwner(contents)
	contents[last].DownSQL = sql
	contents[last].ReverseStatements = reverse
	contents[last].ReverseNoTransaction = requiresNoTransaction
	return contents, nil
}

// reverseOwner picks the file that carries the rollback of the run.
//
// It is the last file, except for the validations that end a run: they have no
// rollback of their own, and a rollback of one step that ran the whole reverse
// would undo statements from files the migrator still records as applied.
func reverseOwner(contents []MigrationFileContent) int {
	for i := range slices.Backward(contents) {
		if !strings.HasPrefix(contents[i].NameSuffix, validationSuffix) {
			return i
		}
	}
	return len(contents) - 1
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
// Liquibase takes `runInTransaction:false` as an attribute on the changeset
// line, and this layout writes one changeset per migration, so the attribute
// covers both directions.
//
// The layouts left out are left out for a measured reason and not for want of
// trying. golang-migrate documents no per-file mechanism at all -- its own
// guidance for CREATE INDEX CONCURRENTLY is to put the statement in a separate
// migration -- so writing one would be inventing syntax the tool that owns the
// format would not read. Flyway does have one, a `<script>.conf` sidecar
// carrying `executeInTransaction=false`, but it is a second FILE rather than a
// line, which this artifact set does not yet model (stokaro/ptah#1630).
func formatExpressesNoTransaction(format atlasmigrateimport.Format) bool {
	switch format {
	case atlasmigrateimport.FormatGoose, atlasmigrateimport.FormatDBMate,
		atlasmigrateimport.FormatLiquibase, atlasmigrateimport.FormatFlyway:
		return true
	default:
		return false
	}
}

// validateGolangMigrateTransactionMode accepts the one shape golang-migrate can
// run outside a transaction, and refuses the rest.
//
// golang-migrate has no directive. What it has is a file rule, and it is the
// one its own postgres driver guidance names: put the statement in a migration
// of its own. Measured against golang-migrate v4.19.0 on PostgreSQL 17, a
// migration holding ONE statement runs unwrapped and applies a CREATE INDEX
// CONCURRENTLY, while the same statement beside a second one in the same file
// answers `CREATE INDEX CONCURRENTLY cannot run inside a transaction block` --
// a multi-statement send is an implicit transaction block, which is
// PostgreSQL's rule rather than the tool's (stokaro/ptah#1630).
//
// The two directions are separate files here, so each is judged on its own
// count. A migration that needs several statements outside a transaction still
// fails closed: writing it would produce a file the source tool cannot run.
func validateGolangMigrateTransactionMode(content MigrationFileContent) error {
	if content.NoTransaction && len(content.Statements) > 1 {
		return fmt.Errorf(
			"migration directory format %q runs a migration outside a transaction only when it holds one "+
				"statement, and this forward migration holds %d; no migration files or atlas.sum were written",
			atlasmigrateimport.FormatGolangMigrate, len(content.Statements),
		)
	}
	if content.ReverseNoTransaction && len(content.ReverseStatements) > 1 {
		return fmt.Errorf(
			"migration directory format %q runs a migration outside a transaction only when it holds one "+
				"statement, and this rollback holds %d; no migration files or atlas.sum were written",
			atlasmigrateimport.FormatGolangMigrate, len(content.ReverseStatements),
		)
	}
	return nil
}

// splitNoTransactionStatements writes each statement of a file that must run
// outside a transaction as a migration of its own.
//
// golang-migrate has no directive for it. A migration runs unwrapped only when
// it holds one statement (see [validateGolangMigrateTransactionMode]), so a
// group of concurrent index builds or constraint validations becomes one
// migration per statement, at consecutive versions, in the order the group
// listed them. The file that ends the run keeps the rollback.
func splitNoTransactionStatements(contents []MigrationFileContent, dialect string, format FileFormat) ([]MigrationFileContent, error) {
	split := make([]MigrationFileContent, 0, len(contents))
	for _, content := range contents {
		if !content.NoTransaction || len(content.Statements) < 2 {
			split = append(split, content)
			continue
		}
		for i, statement := range content.Statements {
			sql, err := renderMigrationDiffSQL(dialect, []Statement{statement}, format)
			if err != nil {
				return nil, err
			}
			one := withTxModeNoneDirective(MigrationFileContent{SQL: sql, Statements: []Statement{statement}})
			one.NameSuffix = fmt.Sprintf("%s_%d", content.NameSuffix, i+1)
			split = append(split, one)
		}
	}
	return split, nil
}

func validateForeignTransactionMode(
	format atlasmigrateimport.Format,
	content MigrationFileContent,
) error {
	if ReadsNativeAtlasDir(format) || formatExpressesNoTransaction(format) {
		return nil
	}
	if format == atlasmigrateimport.FormatGolangMigrate {
		return validateGolangMigrateTransactionMode(content)
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

// renderMigrationStatements renders nodes and returns their statements, each
// under the change comment name gives the node that rendered it.
//
// The comment heads the first statement a node renders, and the node's other
// statements follow under it, as the community binary writes the statements
// of one change. A comment-only piece, such as a planner's note, is joined to
// the statement after it and takes that statement's node.
//
// [planner.RenderedPlan.PlannedStatements] runs without a dialect here, which
// leaves the boundaries to the generic splitter. The dialect's own grammar
// would move a boundary on the engines whose grammar differs, and with it the
// one-statement files a golang-migrate run writes, which is a decision of its
// own.
func renderMigrationStatements(
	ctx context.Context,
	service renderer.Service,
	dialect string,
	caps capability.Capabilities,
	nodes []ast.Node,
	name func(ast.Node) (string, bool),
) ([]Statement, error) {
	target := platform.NormalizeDialect(dialect)
	if target == "" {
		target = dialect
	}
	request := renderer.Request{Target: target, Capabilities: caps, Nodes: nodes}
	output, err := renderer.Render(ctx, service, request)
	if err != nil {
		return nil, err
	}
	planned, err := planner.RenderedPlan{Request: request, Result: output}.PlannedStatements()
	if err != nil {
		return nil, err
	}
	statements := make([]Statement, 0, len(planned))
	named := -1
	for _, piece := range planned {
		statement := Statement{SQL: piece.SQL}
		if piece.Node >= 0 && piece.Node != named {
			named = piece.Node
			statement.Change, _ = name(nodes[piece.Node])
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

// reverseChange names, for a statement of a rollback, the forward change it
// undoes, written the way the community binary writes a rollback file.
func reverseChange(node ast.Node) (string, bool) {
	undone, ok := atlaschange.Undone(node)
	if !ok {
		return "", false
	}
	return "reverse: " + undone, true
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

func hasActualSQLStatements(statements []Statement) bool {
	return slices.ContainsFunc(statements, func(statement Statement) bool {
		return !sqlscript.CommentOnly(statement.SQL)
	})
}
