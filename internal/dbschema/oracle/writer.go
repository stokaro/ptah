package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlrunner"
)

// Writer applies schema changes to an Oracle server.
//
// The transaction methods are deliberate no-ops, and unlike ClickHouse's the
// reason is not that transactions are missing: Oracle has them, and they do not
// cover DDL. Measured on 23.26.2.0.0 and 21.3.0.0.0 alike, a CREATE TABLE
// issued inside an explicit transaction is still in user_tables after the
// ROLLBACK, because Oracle commits the transaction in progress before every
// schema statement. Returning a transaction whose Commit and Rollback do
// nothing states that plainly; opening a real one would promise an atomicity
// the engine does not provide, which is what capability.TransactionalDDL reads
// false for. With no transaction open every statement commits on its own, which
// is why internal/ddltx classifies Oracle as NoTransaction.
type Writer struct {
	db     sqlrunner.Runner
	schema string
	dryRun bool
}

type transactionWriter struct {
	writer *Writer
}

// NewOracleWriterForRunner constructs a writer scoped to one schema.
func NewOracleWriterForRunner(db sqlrunner.Runner, schema string) *Writer {
	return &Writer{db: db, schema: strings.ToUpper(strings.TrimSpace(schema))}
}

func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

func (w *Writer) IsDryRun() bool { return w.dryRun }

func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("oracle: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction returns a transaction whose Commit and Rollback are no-ops.
// See the Writer doc for the measurement behind that.
func (w *Writer) BeginTransaction(_ context.Context) (catalog.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction (a no-op for Oracle DDL)")
	}
	return &transactionWriter{writer: w}, nil
}

func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	return w.writer.ExecuteSQL(ctx, sqlExpr, args...)
}

func (w *transactionWriter) IsDryRun() bool { return w.writer.IsDryRun() }

// Commit is a no-op: every schema statement already committed itself.
func (w *transactionWriter) Commit() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would commit transaction (a no-op for Oracle DDL)")
	}
	return nil
}

// Rollback is a no-op for the same reason. It does not report an error,
// because there is nothing wrong with the request -- the engine simply has
// nothing left to undo.
func (w *transactionWriter) Rollback() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would roll back transaction (a no-op for Oracle DDL)")
	}
	return nil
}

// cleanupObjectQuery lists the objects DropAllTables removes, newest first
// within each kind.
//
// It reads USER_OBJECTS rather than ALL_OBJECTS, and that is the confinement:
// the view shows only what the connected account owns, so a cleanup cannot
// reach another schema even if the account has been granted access to one.
//
// Three exclusions are load-bearing. An index or a trigger Oracle created for
// a constraint disappears with its table, and naming it separately would answer
// ORA-02429 or drop something that is already gone. A table in the recycle bin
// -- name starting BIN$ -- is already dropped; it is listed by USER_OBJECTS
// until the bin is purged, and asking to drop it again is an error about a
// table nobody has. And a materialized view keeps its rows in a table of its
// own name, which USER_OBJECTS lists as a TABLE with GENERATED N: measured on
// 23.26.3.0.0, DROP MATERIALIZED VIEW takes that table with it, and the DROP
// TABLE after it answered ORA-00942 and stopped the cleanup.
//
// Types and domains come after the tables whose columns they type, and
// domains last: an object type whose attribute names a domain depends on the
// domain, and DROP DOMAIN answers ORA-11502 while anything still does,
// measured on 23.26.3.0.0. Oracle 21 has no domains, and lists none.
const cleanupObjectQuery = `
SELECT o.object_type, o.object_name
FROM user_objects o
WHERE o.object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SEQUENCE', 'SYNONYM', 'TRIGGER', 'TYPE', 'DOMAIN')
  AND o.object_name NOT LIKE 'BIN$%'
  AND o.generated = 'N'
  AND NOT (o.object_type = 'TABLE' AND EXISTS (
        SELECT 1 FROM user_mviews m WHERE m.mview_name = o.object_name))
ORDER BY CASE o.object_type
           WHEN 'TRIGGER' THEN 1
           WHEN 'MATERIALIZED VIEW' THEN 2
           WHEN 'VIEW' THEN 3
           WHEN 'TABLE' THEN 4
           WHEN 'SEQUENCE' THEN 5
           WHEN 'SYNONYM' THEN 6
           WHEN 'TYPE' THEN 7
           ELSE 8
         END,
         o.object_name`

// domainDependencyQuery lists which of the account's domains each domain
// references.
//
// A flexible domain references the domains it chooses from, and measured on
// 23.26.3.0.0, dropping one of those first answers ORA-11502. USER_DEPENDENCIES
// records the reference with TYPE and REFERENCED_TYPE both DOMAIN.
const domainDependencyQuery = `
SELECT d.name, d.referenced_name
FROM user_dependencies d
WHERE d.type = 'DOMAIN'
  AND d.referenced_type = 'DOMAIN'
  AND d.referenced_owner = USER
ORDER BY d.name, d.referenced_name`

// cleanupObject is one object DropAllTables drops: its kind as USER_OBJECTS
// spells it, and its name.
type cleanupObject struct{ kind, name string }

// cleanupObjects lists the objects DropAllTables drops, in the order it drops
// them: cleanupObjectQuery's order, with each domain after every domain that
// references it.
func (w *Writer) cleanupObjects(ctx context.Context) ([]cleanupObject, error) {
	objects, err := w.listCleanupObjects(ctx)
	if err != nil {
		return nil, err
	}
	first := slices.IndexFunc(objects, func(object cleanupObject) bool { return object.kind == "DOMAIN" })
	if first < 0 {
		return objects, nil
	}
	references, err := w.domainReferences(ctx)
	if err != nil {
		return nil, err
	}
	return append(objects[:first], dependentsFirst(objects[first:], references)...), nil
}

// listCleanupObjects runs cleanupObjectQuery and reads every row before it
// returns, since go-ora answers a second query with EOF while a result set
// has rows left to read.
func (w *Writer) listCleanupObjects(ctx context.Context) ([]cleanupObject, error) {
	rows, err := w.db.QueryContext(ctx, cleanupObjectQuery)
	if err != nil {
		return nil, fmt.Errorf("oracle: list objects to drop: %w", err)
	}
	var objects []cleanupObject
	for rows.Next() {
		var found cleanupObject
		if err := rows.Scan(&found.kind, &found.name); err != nil {
			rows.Close()
			return nil, fmt.Errorf("oracle: list objects to drop: %w", err)
		}
		objects = append(objects, found)
	}
	closeErr := rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("oracle: list objects to drop: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("oracle: list objects to drop: %w", closeErr)
	}
	return objects, nil
}

// domainReferences maps each domain to the domains it references.
func (w *Writer) domainReferences(ctx context.Context) (map[string][]string, error) {
	rows, err := w.db.QueryContext(ctx, domainDependencyQuery)
	if err != nil {
		return nil, fmt.Errorf("oracle: list domain dependencies: %w", err)
	}
	defer rows.Close()
	references := make(map[string][]string)
	for rows.Next() {
		var name, referenced string
		if err := rows.Scan(&name, &referenced); err != nil {
			return nil, fmt.Errorf("oracle: list domain dependencies: %w", err)
		}
		references[name] = append(references[name], referenced)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("oracle: list domain dependencies: %w", err)
	}
	return references, nil
}

// dependentsFirst orders domains so each comes after every domain that
// references it, keeping the given order otherwise. A cycle, which Oracle does
// not allow, would leave the rest in the given order rather than lose them.
func dependentsFirst(domains []cleanupObject, references map[string][]string) []cleanupObject {
	referencedBy := make(map[string]int, len(domains))
	listed := make(map[string]bool, len(domains))
	for _, domain := range domains {
		listed[domain.name] = true
	}
	for name, referenced := range references {
		if !listed[name] {
			continue
		}
		for _, target := range referenced {
			referencedBy[target]++
		}
	}
	ordered := make([]cleanupObject, 0, len(domains))
	remaining := slices.Clone(domains)
	for len(remaining) > 0 {
		next := slices.IndexFunc(remaining, func(domain cleanupObject) bool { return referencedBy[domain.name] == 0 })
		if next < 0 {
			return append(ordered, remaining...)
		}
		domain := remaining[next]
		remaining = slices.Delete(remaining, next, next+1)
		ordered = append(ordered, domain)
		for _, target := range references[domain.name] {
			referencedBy[target]--
		}
	}
	return ordered
}

// ResetObjects lists what DropAllTables drops, through the query it runs:
// the tables, views, materialized views, sequences, synonyms, triggers, types
// and domains the connected account owns, in the order it drops them, each
// with the statement it drops the object with. The schema of each is the
// account. A routine or a package is not listed, since
// the reset leaves it in place. The scope is PostgreSQL's and is not read.
func (w *Writer) ResetObjects(ctx context.Context, _ dbreset.Scope) ([]dbreset.Object, error) {
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}
	var account string
	if err := w.db.QueryRowContext(ctx, "SELECT USER FROM dual").Scan(&account); err != nil {
		return nil, fmt.Errorf("oracle: read the connected account: %w", err)
	}
	found, err := w.cleanupObjects(ctx)
	if err != nil {
		return nil, err
	}
	objects := make([]dbreset.Object, 0, len(found))
	for _, object := range found {
		objects = append(objects, dbreset.Object{Kind: strings.ToLower(object.kind), Schema: account, Name: object.name,
			Statement: dropStatement(object.kind, object.name)})
	}
	return objects, nil
}

// DropAllTables removes every object the connected account owns.
//
// Tables are dropped with CASCADE CONSTRAINTS so a cycle of foreign keys does
// not decide the order, and with PURGE so the objects do not land in the
// recycle bin -- where they would keep their storage and be listed by
// USER_OBJECTS on the next run. Domains are dropped last, each after the
// domains that reference it.
func (w *Writer) DropAllTables(ctx context.Context) error {
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	objects, err := w.cleanupObjects(ctx)
	if err != nil {
		return err
	}
	for _, found := range objects {
		if err := w.ExecuteSQL(ctx, dropStatement(found.kind, found.name)); err != nil {
			return err
		}
	}
	return nil
}

func dropStatement(kind, name string) string {
	quoted := sqlident.Quote(platform.Oracle, name)
	switch kind {
	case "TABLE":
		return "DROP TABLE " + quoted + " CASCADE CONSTRAINTS PURGE"
	case "TYPE":
		return "DROP TYPE " + quoted + " FORCE"
	default:
		return "DROP " + kind + " " + quoted
	}
}
