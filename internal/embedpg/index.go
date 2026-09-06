package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/internal/embedgen"
)

// indexDatabase is the SQL surface index creation needs. Both *sql.DB and the
// dedicated *sql.Conn used by Store.EnsureRunIndex implement it; the latter is
// what keeps one PostgreSQL session-level lifecycle lock across CREATE INDEX
// CONCURRENTLY, which cannot run inside a transaction.
type indexDatabase interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// IndexOutcome is what EnsureIndex did, for a caller that has to say so.
type IndexOutcome string

const (
	// IndexNotDeclared means the specification names no index method. It is an
	// answer rather than a failure: a generation may be queried by sequential
	// scan, and an author who wrote no method asked for that.
	IndexNotDeclared IndexOutcome = "not declared"
	// IndexAlreadyValid means it was there and usable, which is the finished
	// state and not an error to run into twice.
	IndexAlreadyValid IndexOutcome = "already valid"
	// IndexRebuilt means an invalid index was dropped and built again. A
	// concurrent build that fails leaves one behind, and PostgreSQL will not
	// use it -- so it is neither present nor absent, and saying "already
	// exists" about it would report a generation as ready for queries that all
	// become sequential scans.
	IndexRebuilt IndexOutcome = "rebuilt"
	// IndexBuilt means it was created.
	IndexBuilt IndexOutcome = "built"
)

// EnsureIndex builds the generation's vector index and leaves it valid.
//
// It exists because nothing built one. `ptah inference plan` has always listed
// `[index] build the vector index and wait for it to be valid` as a step,
// `Spec.TargetObjects` has always derived the index, and the only consumers
// read it -- to verify one, to drop one. A specification naming an index method
// described a generation that could never become ready: verification reported
// the index absent, and the rollback policy required one
// (stokaro/ptah#2415).
//
// After the backfill rather than in prepare. An IVFFlat index trains its lists
// on the data present when it is built, so one built over an empty column is
// valid and useless.
func EnsureIndex(ctx context.Context, db indexDatabase, spec embedgen.Spec) (IndexOutcome, error) {
	objects, err := spec.TargetObjects()
	if err != nil {
		return "", err
	}
	if !objects.HasIndex {
		return IndexNotDeclared, nil
	}
	name := objects.Index.Name

	// Present AND valid, because the two are different states and only one of
	// them is finished. A concurrent build that failed leaves an index behind
	// that PostgreSQL will not use, so asking only whether it EXISTS would
	// report a generation ready while every query over it is a sequential scan
	// over the whole corpus.
	present, valid, err := indexState(ctx, db, name)
	if err != nil {
		return "", err
	}
	if present && valid {
		return IndexAlreadyValid, nil
	}
	outcome := IndexBuilt
	if present {
		// #nosec G201 -- a derived index name, through quoteIdentifier.
		if _, err := db.ExecContext(ctx,
			"DROP INDEX "+quoteIdentifier(name)); err != nil {
			return "", fmt.Errorf("drop the invalid index %s: %w", name, err)
		}
		outcome = IndexRebuilt
	}

	statement, err := createIndexStatement(spec, objects.Index.Name, objects.Index.Operator)
	if err != nil {
		return "", err
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		// A concurrent build that fails leaves an invalid index behind AND
		// returns the error, so the leftover is not silent -- it is picked up
		// by the check above on the next run, which is the only moment it is
		// observable. There is deliberately no read-back here: this statement
		// cannot both succeed and leave an unusable index, so a check for that
		// would be an answer nothing can produce, and no fixture could measure
		// it.
		return "", fmt.Errorf("build the index %s: %w", name, err)
	}
	return outcome, nil
}

// indexState reports whether an index exists and whether it is usable.
func indexState(
	ctx context.Context, db indexDatabase, name string,
) (present, valid bool, err error) {
	const query = `SELECT i.indisvalid
		FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
		WHERE ic.relname = $1`
	switch scanErr := db.QueryRowContext(ctx, query, name).Scan(&valid); {
	case errors.Is(scanErr, sql.ErrNoRows):
		return false, false, nil
	case scanErr != nil:
		return false, false, fmt.Errorf("read the index %s: %w", name, scanErr)
	}
	return true, valid, nil
}

// createIndexStatement renders the build.
//
// CONCURRENTLY, because the table is one an application is reading and writing
// while this runs, and a plain build takes a lock that stops both for as long
// as it takes to index a corpus. It is why this cannot be inside a transaction
// and why the outcome has to be read back afterwards.
func createIndexStatement(spec embedgen.Spec, name, operatorClass string) (string, error) {
	options, err := indexWithClause(spec.Target.IndexOptions)
	if err != nil {
		return "", err
	}
	// #nosec G201 -- every interpolation is either a derived name through
	// quoteIdentifier, a method and operator class from the specification's own
	// validated enumeration, or an option pair refused unless it matches
	// indexOptionName and indexOptionValue.
	return fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s USING %s (%s %s)%s",
		quoteIdentifier(name), qualify(spec.Target.Schema, spec.Target.Table),
		spec.Target.IndexMethod,
		quoteIdentifier(spec.Target.Column), operatorClass, options), nil
}

// indexOptionName and indexOptionValue are what an option may be.
//
// pgvector's build options are all numbers -- lists, m, ef_construction -- and
// an option reaches SQL as text rather than as a parameter, because PostgreSQL
// takes no parameter in a WITH clause. So the shapes are refused rather than
// escaped: anything that is not a bare identifier and a number is not an option
// this knows how to place, and guessing would put author-controlled text in a
// statement.
var (
	indexOptionName  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	indexOptionValue = regexp.MustCompile(`^[0-9]+$`)
)

// indexWithClause renders the build options, in name order.
//
// Sorted because a map has no order and a statement that differs run to run is
// one nobody can compare against what a database reports.
func indexWithClause(options map[string]string) (string, error) {
	if len(options) == 0 {
		return "", nil
	}
	names := make([]string, 0, len(options))
	for name := range options {
		names = append(names, name)
	}
	slices.Sort(names)

	pairs := make([]string, 0, len(names))
	for _, name := range names {
		value := strings.TrimSpace(options[name])
		if !indexOptionName.MatchString(name) {
			return "", fmt.Errorf(
				"index option %q is not a name this can place in a WITH clause: "+
					"an option is a lower-case identifier", name)
		}
		if !indexOptionValue.MatchString(value) {
			return "", fmt.Errorf(
				"index option %s = %q is not a value this can place in a WITH clause: "+
					"pgvector's build options are whole numbers", name, value)
		}
		pairs = append(pairs, name+" = "+value)
	}
	return " WITH (" + strings.Join(pairs, ", ") + ")", nil
}
