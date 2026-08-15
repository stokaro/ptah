package mssql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

func quoteIdent(name string) string {
	return sqlident.Quote(platform.SQLServer, name)
}

func quoteQualified(schema, name string) string {
	return sqlident.Qualified(platform.SQLServer, schema, name)
}

// Writer applies schema changes to SQL Server.
type Writer struct {
	db     sqlrunner.Runner
	schema string
	dryRun bool
}

type foreignKey struct {
	Schema           string
	Table            string
	Name             string
	ReferencedSchema string
	ReferencedTable  string
}

type realmObject struct {
	ID              int64
	ParentID        int64
	HistoryTableID  int64
	Schema          string
	Name            string
	Type            string
	TypeDescription string
	TemporalType    int64
	LedgerType      int64
}

type realmDependency struct {
	ReferencingID int64
	ReferencedID  int64
}

type realmType struct {
	Schema string
	Name   string
}

type realmXMLSchema struct {
	Schema string
	Name   string
}

type realmResidual struct {
	Category string
	Schema   string
	Name     string
	Detail   string
}

type realmCleanupPlan struct {
	foreignKeys  []foreignKey
	objects      []realmObject
	dependencies []realmDependency
	types        []realmType
	xmlSchemas   []realmXMLSchema
	schemas      []string
}

type transactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	dryRun bool
}

func NewSQLServerWriter(db *sql.DB, schema string) *Writer {
	return NewSQLServerWriterForRunner(db, schema)
}

// NewSQLServerWriterForRunner creates a writer bound to a pool or pinned
// database session.
func NewSQLServerWriterForRunner(runner sqlrunner.Runner, schema string) *Writer {
	if schema == "" {
		schema = "dbo"
	}
	return &Writer{db: runner, schema: schema}
}

func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("sqlserver: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

func (w *Writer) BeginTransaction(ctx context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction")
		return &transactionWriter{dryRun: true}, nil
	}
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &transactionWriter{tx: tx}, nil
}

func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return fmt.Errorf("transaction is closed")
	}
	if _, err := w.tx.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("sqlserver: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

func (w *transactionWriter) Commit() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would commit transaction")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return fmt.Errorf("transaction is closed")
	}
	err := w.tx.Commit()
	w.tx = nil
	return err
}

func (w *transactionWriter) Rollback() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

func (w *transactionWriter) IsDryRun() bool { return w.dryRun }

func (w *Writer) DropAllTables(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	tables, err := w.listTables(ctx)
	if err != nil {
		return err
	}
	foreignKeys, err := w.listForeignKeys(ctx)
	if err != nil {
		return err
	}
	if err := w.rejectExternalForeignKeys(foreignKeys); err != nil {
		return err
	}

	tx, err := w.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: begin drop transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				resultErr = errors.Join(resultErr, fmt.Errorf("sqlserver: roll back drop transaction: %w", rollbackErr))
			}
		}
	}()

	for _, fk := range foreignKeys {
		qualified := quoteQualified(fk.Schema, fk.Table)
		constraint := quoteIdent(fk.Name)
		if err := tx.ExecuteSQL(ctx, "ALTER TABLE "+qualified+" DROP CONSTRAINT "+constraint); err != nil {
			return fmt.Errorf("sqlserver: drop foreign key %s on %s: %w", constraint, qualified, err)
		}
	}
	for _, table := range tables {
		qualified := quoteQualified(table.Schema, table.Name)
		if err := tx.ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+qualified); err != nil {
			return fmt.Errorf("sqlserver: drop table %s: %w", qualified, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlserver: commit drop transaction: %w", err)
	}
	committed = true
	return nil
}

// DropDatabaseRealm removes supported user objects from every user schema in
// the current database as one transaction. The configured schema and dbo are
// preserved, while other user-defined schemas are removed after their contents.
func (w *Writer) DropDatabaseRealm(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sqlserver: database realm cleanup canceled before start: %w", err)
	}

	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("sqlserver: begin database realm cleanup transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf("sqlserver: roll back database realm cleanup transaction: %w", rollbackErr),
				)
			}
		}
	}()

	if err := requireRealmMetadataVisibility(ctx, tx); err != nil {
		return err
	}
	unsupported, err := findUnsupportedRealmArtifact(ctx, tx, w.schema)
	if err != nil {
		return err
	}
	if unsupported != nil {
		return fmt.Errorf(
			"sqlserver: refusing to clean database realm with unsupported database-scoped %s %s (%s)",
			unsupported.Category,
			quoteResidual(*unsupported),
			unsupported.Detail,
		)
	}
	plan, err := buildRealmCleanupPlan(ctx, tx, w.schema)
	if err != nil {
		return err
	}
	if err := executeRealmCleanupPlan(ctx, tx, plan); err != nil {
		return err
	}
	residual, err := findRealmResidual(ctx, tx, w.schema)
	if err != nil {
		return err
	}
	if residual != nil {
		return fmt.Errorf(
			"sqlserver: refusing to commit database realm cleanup: residual %s %s (%s)",
			residual.Category,
			quoteResidual(*residual),
			residual.Detail,
		)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlserver: commit database realm cleanup transaction: %w", err)
	}
	committed = true
	return nil
}

func requireRealmMetadataVisibility(ctx context.Context, tx *sql.Tx) error {
	var databaseName string
	var hasControl bool
	var hasViewDefinition bool
	err := tx.QueryRowContext(
		ctx,
		`SELECT
			DB_NAME(),
			HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'CONTROL'),
			HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION')`,
	).Scan(&databaseName, &hasControl, &hasViewDefinition)
	if err != nil {
		return fmt.Errorf("sqlserver: verify database realm metadata visibility: %w", err)
	}
	switch databaseName {
	case "master", "model", "msdb", "tempdb":
		return fmt.Errorf(
			"sqlserver: refusing to clean system database %s",
			quoteIdent(databaseName),
		)
	}
	if !hasControl {
		return fmt.Errorf(
			"sqlserver: refusing to clean database realm without CONTROL permission on the current database",
		)
	}
	if !hasViewDefinition {
		return fmt.Errorf(
			"sqlserver: refusing to clean database realm without VIEW DEFINITION permission on the current database",
		)
	}
	return nil
}

func buildRealmCleanupPlan(
	ctx context.Context,
	tx *sql.Tx,
	preservedSchema string,
) (realmCleanupPlan, error) {
	objects, err := listRealmObjects(ctx, tx)
	if err != nil {
		return realmCleanupPlan{}, err
	}
	if err := rejectUnsupportedRealmObjects(objects); err != nil {
		return realmCleanupPlan{}, err
	}
	foreignKeys, err := listRealmForeignKeys(ctx, tx)
	if err != nil {
		return realmCleanupPlan{}, err
	}
	dependencies, err := listRealmDependencies(ctx, tx)
	if err != nil {
		return realmCleanupPlan{}, err
	}
	dependencies = append(dependencies, temporalRealmDependencies(objects)...)
	userTypes, err := listRealmTypes(ctx, tx)
	if err != nil {
		return realmCleanupPlan{}, err
	}
	xmlSchemas, err := listRealmXMLSchemas(ctx, tx)
	if err != nil {
		return realmCleanupPlan{}, err
	}
	schemas, err := listDroppableRealmSchemas(ctx, tx, preservedSchema)
	if err != nil {
		return realmCleanupPlan{}, err
	}
	return realmCleanupPlan{
		foreignKeys:  foreignKeys,
		objects:      objects,
		dependencies: dependencies,
		types:        userTypes,
		xmlSchemas:   xmlSchemas,
		schemas:      schemas,
	}, nil
}

func executeRealmCleanupPlan(ctx context.Context, tx *sql.Tx, plan realmCleanupPlan) error {
	for _, fk := range plan.foreignKeys {
		qualifiedTable := quoteQualified(fk.Schema, fk.Table)
		constraint := quoteIdent(fk.Name)
		// #nosec G202 -- catalog identifiers are emitted only through SQL Server identifier quoting.
		dropSQL := "ALTER TABLE " + qualifiedTable + " DROP CONSTRAINT " + constraint
		if _, err := tx.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf(
				"sqlserver: drop foreign key %s on %s: %w",
				constraint,
				qualifiedTable,
				err,
			)
		}
	}

	if err := dropRealmSecurityPolicies(ctx, tx, plan.objects); err != nil {
		return err
	}
	if err := dropRealmObjects(ctx, tx, plan.objects, plan.dependencies); err != nil {
		return err
	}
	for _, userType := range plan.types {
		qualified := quoteQualified(userType.Schema, userType.Name)
		if _, err := tx.ExecContext(ctx, "DROP TYPE IF EXISTS "+qualified); err != nil {
			return fmt.Errorf("sqlserver: drop type %s: %w", qualified, err)
		}
	}
	for _, xmlSchema := range plan.xmlSchemas {
		qualified := quoteQualified(xmlSchema.Schema, xmlSchema.Name)
		if _, err := tx.ExecContext(ctx, "DROP XML SCHEMA COLLECTION "+qualified); err != nil {
			return fmt.Errorf("sqlserver: drop XML schema collection %s: %w", qualified, err)
		}
	}
	for _, schema := range plan.schemas {
		quoted := quoteIdent(schema)
		if _, err := tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted); err != nil {
			return fmt.Errorf("sqlserver: drop schema %s: %w", quoted, err)
		}
	}
	return nil
}

func dropRealmSecurityPolicies(ctx context.Context, tx *sql.Tx, objects []realmObject) error {
	for _, object := range objects {
		if object.Type != "SP" {
			continue
		}
		if err := dropRealmObject(ctx, tx, object); err != nil {
			return err
		}
	}
	return nil
}

func dropRealmObjects(
	ctx context.Context,
	tx *sql.Tx,
	objects []realmObject,
	dependencies []realmDependency,
) error {
	var droppable []realmObject
	for _, object := range objects {
		_, canDrop := realmDropPrefix(object)
		if canDrop && object.Type != "SP" {
			droppable = append(droppable, object)
		}
	}
	for _, object := range orderRealmObjects(droppable, dependencies) {
		if err := dropRealmObject(ctx, tx, object); err != nil {
			return err
		}
	}
	return nil
}

func dropRealmObject(ctx context.Context, tx *sql.Tx, object realmObject) error {
	prefix, canDrop := realmDropPrefix(object)
	if !canDrop {
		return fmt.Errorf(
			"sqlserver: internal cleanup error: unsupported drop for %s %s",
			object.TypeDescription,
			quoteQualified(object.Schema, object.Name),
		)
	}
	qualified := quoteQualified(object.Schema, object.Name)
	if object.TemporalType == 2 {
		// #nosec G202 -- catalog identifiers are emitted only through SQL Server identifier quoting.
		disableSQL := "ALTER TABLE " + qualified + " SET (SYSTEM_VERSIONING = OFF)"
		if _, err := tx.ExecContext(ctx, disableSQL); err != nil {
			return fmt.Errorf(
				"sqlserver: disable system versioning on temporal table %s: %w",
				qualified,
				err,
			)
		}
	}
	// #nosec G202 -- the fixed drop prefix is selected by type and catalog identifiers are safely quoted.
	dropSQL := prefix + qualified
	if _, err := tx.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf(
			"sqlserver: drop %s %s: %w",
			strings.ToLower(object.TypeDescription),
			qualified,
			err,
		)
	}
	return nil
}

func realmDropPrefix(object realmObject) (string, bool) {
	switch object.Type {
	case "AF":
		return "DROP AGGREGATE IF EXISTS ", true
	case "D":
		return "DROP DEFAULT IF EXISTS ", object.ParentID == 0
	case "ET":
		return "DROP EXTERNAL TABLE IF EXISTS ", true
	case "FN", "FS", "FT", "IF", "TF":
		return "DROP FUNCTION IF EXISTS ", true
	case "P", "PC":
		return "DROP PROCEDURE IF EXISTS ", true
	case "R":
		return "DROP RULE IF EXISTS ", true
	case "SN":
		return "DROP SYNONYM IF EXISTS ", true
	case "SO":
		return "DROP SEQUENCE IF EXISTS ", true
	case "SP":
		return "DROP SECURITY POLICY IF EXISTS ", true
	case "TA", "TR":
		return "DROP TRIGGER IF EXISTS ", true
	case "U":
		return "DROP TABLE IF EXISTS ", true
	case "V":
		return "DROP VIEW IF EXISTS ", true
	default:
		return "", false
	}
}

func rejectUnsupportedRealmObjects(objects []realmObject) error {
	var unsupported []string
	for _, object := range objects {
		if object.LedgerType != 0 {
			unsupported = append(
				unsupported,
				fmt.Sprintf(
					"%s (%s)",
					quoteQualified(object.Schema, object.Name),
					"LEDGER_TABLE",
				),
			)
			continue
		}
		_, canDrop := realmDropPrefix(object)
		if canDrop || realmObjectOwnedBySupportedObject(object) {
			continue
		}
		unsupported = append(
			unsupported,
			fmt.Sprintf(
				"%s (%s)",
				quoteQualified(object.Schema, object.Name),
				object.TypeDescription,
			),
		)
	}
	if len(unsupported) == 0 {
		return nil
	}
	return fmt.Errorf(
		"sqlserver: refusing to clean database realm with unsupported user objects: %s",
		strings.Join(unsupported, "; "),
	)
}

func temporalRealmDependencies(objects []realmObject) []realmDependency {
	var dependencies []realmDependency
	for _, object := range objects {
		if object.TemporalType != 2 || object.HistoryTableID == 0 {
			continue
		}
		dependencies = append(dependencies, realmDependency{
			ReferencingID: object.ID,
			ReferencedID:  object.HistoryTableID,
		})
	}
	return dependencies
}

func realmObjectOwnedBySupportedObject(object realmObject) bool {
	switch object.Type {
	case "C", "EC", "F", "IT", "PK", "ST", "TT", "UQ":
		return true
	case "D":
		return object.ParentID != 0
	default:
		return false
	}
}

func orderRealmObjects(objects []realmObject, dependencies []realmDependency) []realmObject {
	objectsByID := make(map[int64]realmObject, len(objects))
	inDegree := make(map[int64]int, len(objects))
	outgoing := make(map[int64][]int64, len(objects))
	for _, object := range objects {
		objectsByID[object.ID] = object
		inDegree[object.ID] = 0
	}

	edges := make(map[[2]int64]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		_, hasReferencing := objectsByID[dependency.ReferencingID]
		_, hasReferenced := objectsByID[dependency.ReferencedID]
		edge := [2]int64{dependency.ReferencingID, dependency.ReferencedID}
		if !hasReferencing || !hasReferenced || dependency.ReferencingID == dependency.ReferencedID {
			continue
		}
		if _, duplicate := edges[edge]; duplicate {
			continue
		}
		edges[edge] = struct{}{}
		outgoing[dependency.ReferencingID] = append(
			outgoing[dependency.ReferencingID],
			dependency.ReferencedID,
		)
		inDegree[dependency.ReferencedID]++
	}

	var ready []realmObject
	for _, object := range objects {
		if inDegree[object.ID] == 0 {
			ready = append(ready, object)
		}
	}
	sortRealmObjects(ready)

	ordered := make([]realmObject, 0, len(objects))
	for len(ready) > 0 {
		object := ready[0]
		ready = ready[1:]
		ordered = append(ordered, object)
		for _, referencedID := range outgoing[object.ID] {
			inDegree[referencedID]--
			if inDegree[referencedID] == 0 {
				ready = append(ready, objectsByID[referencedID])
				sortRealmObjects(ready)
			}
		}
	}

	if len(ordered) == len(objects) {
		return ordered
	}
	var cyclic []realmObject
	for _, object := range objects {
		if inDegree[object.ID] > 0 {
			cyclic = append(cyclic, object)
		}
	}
	sortRealmObjects(cyclic)
	return append(ordered, cyclic...)
}

func sortRealmObjects(objects []realmObject) {
	slices.SortFunc(objects, func(left, right realmObject) int {
		if result := cmp.Compare(left.Schema, right.Schema); result != 0 {
			return result
		}
		if result := cmp.Compare(left.Name, right.Name); result != 0 {
			return result
		}
		if result := cmp.Compare(left.Type, right.Type); result != 0 {
			return result
		}
		return cmp.Compare(left.ID, right.ID)
	})
}

func listRealmObjects(ctx context.Context, tx *sql.Tx) ([]realmObject, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			o.object_id,
			o.parent_object_id,
			s.name,
			o.name,
			o.type,
			o.type_desc,
			COALESCE(t.temporal_type, 0),
			COALESCE(t.history_table_id, 0),
			COALESCE(t.ledger_type, 0)
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		LEFT JOIN sys.tables AS t ON t.object_id = o.object_id
		WHERE o.is_ms_shipped = 0
		  AND s.name NOT IN (
			  N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
		ORDER BY s.name, o.name, o.type
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list database realm objects: %w", err)
	}
	defer rows.Close()

	var objects []realmObject
	for rows.Next() {
		var object realmObject
		if err := rows.Scan(
			&object.ID,
			&object.ParentID,
			&object.Schema,
			&object.Name,
			&object.Type,
			&object.TypeDescription,
			&object.TemporalType,
			&object.HistoryTableID,
			&object.LedgerType,
		); err != nil {
			return nil, fmt.Errorf("sqlserver: scan database realm object: %w", err)
		}
		object.Type = strings.TrimSpace(object.Type)
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate database realm objects: %w", err)
	}
	return objects, nil
}

func listRealmForeignKeys(ctx context.Context, tx *sql.Tx) ([]foreignKey, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT ps.name, pt.name, fk.name, rs.name, rt.name
		FROM sys.foreign_keys AS fk
		JOIN sys.tables AS pt ON pt.object_id = fk.parent_object_id
		JOIN sys.schemas AS ps ON ps.schema_id = pt.schema_id
		JOIN sys.tables AS rt ON rt.object_id = fk.referenced_object_id
		JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
		WHERE pt.is_ms_shipped = 0
		  AND ps.name NOT IN (
			  N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
		ORDER BY ps.name, pt.name, fk.name
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list database realm foreign keys: %w", err)
	}
	defer rows.Close()

	var foreignKeys []foreignKey
	for rows.Next() {
		var fk foreignKey
		if err := rows.Scan(&fk.Schema, &fk.Table, &fk.Name, &fk.ReferencedSchema, &fk.ReferencedTable); err != nil {
			return nil, fmt.Errorf("sqlserver: scan database realm foreign key: %w", err)
		}
		foreignKeys = append(foreignKeys, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate database realm foreign keys: %w", err)
	}
	return foreignKeys, nil
}

func listRealmDependencies(ctx context.Context, tx *sql.Tx) ([]realmDependency, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT
			CASE
				WHEN referencing_object.type IN (N'C', N'D')
					AND referencing_object.parent_object_id <> 0
					THEN referencing_object.parent_object_id
				ELSE dependency.referencing_id
			END,
			dependency.referenced_id
		FROM sys.sql_expression_dependencies AS dependency
		JOIN sys.objects AS referencing_object
			ON referencing_object.object_id = dependency.referencing_id
		WHERE dependency.referencing_class = 1
		  AND dependency.referenced_class = 1
		  AND dependency.referenced_id IS NOT NULL
		ORDER BY 1, 2
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list database realm dependencies: %w", err)
	}
	defer rows.Close()

	var dependencies []realmDependency
	for rows.Next() {
		var dependency realmDependency
		if err := rows.Scan(&dependency.ReferencingID, &dependency.ReferencedID); err != nil {
			return nil, fmt.Errorf("sqlserver: scan database realm dependency: %w", err)
		}
		dependencies = append(dependencies, dependency)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate database realm dependencies: %w", err)
	}
	return dependencies, nil
}

func listRealmTypes(ctx context.Context, tx *sql.Tx) ([]realmType, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT s.name, t.name
		FROM sys.types AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_user_defined = 1
		  AND s.name NOT IN (
			  N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
		ORDER BY t.is_table_type DESC, s.name, t.name
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list database realm types: %w", err)
	}
	defer rows.Close()

	var userTypes []realmType
	for rows.Next() {
		var userType realmType
		if err := rows.Scan(&userType.Schema, &userType.Name); err != nil {
			return nil, fmt.Errorf("sqlserver: scan database realm type: %w", err)
		}
		userTypes = append(userTypes, userType)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate database realm types: %w", err)
	}
	return userTypes, nil
}

func listRealmXMLSchemas(ctx context.Context, tx *sql.Tx) ([]realmXMLSchema, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT s.name, x.name
		FROM sys.xml_schema_collections AS x
		JOIN sys.schemas AS s ON s.schema_id = x.schema_id
		WHERE x.xml_collection_id > 1
		  AND s.name NOT IN (
			  N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
		ORDER BY s.name, x.name
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list database realm XML schema collections: %w", err)
	}
	defer rows.Close()

	var xmlSchemas []realmXMLSchema
	for rows.Next() {
		var xmlSchema realmXMLSchema
		if err := rows.Scan(&xmlSchema.Schema, &xmlSchema.Name); err != nil {
			return nil, fmt.Errorf("sqlserver: scan database realm XML schema collection: %w", err)
		}
		xmlSchemas = append(xmlSchemas, xmlSchema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate database realm XML schema collections: %w", err)
	}
	return xmlSchemas, nil
}

func listDroppableRealmSchemas(
	ctx context.Context,
	tx *sql.Tx,
	preservedSchema string,
) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT s.name
		FROM sys.schemas AS s
		WHERE s.name NOT IN (
			  N'dbo', N'sys', N'INFORMATION_SCHEMA', N'guest',
			  N'db_owner', N'db_accessadmin', N'db_securityadmin',
			  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
			  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
		  )
		  AND s.name <> @p1
		ORDER BY s.name
	`, preservedSchema)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list droppable database realm schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("sqlserver: scan droppable database realm schema: %w", err)
		}
		schemas = append(schemas, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate droppable database realm schemas: %w", err)
	}
	return schemas, nil
}

func findRealmResidual(
	ctx context.Context,
	tx *sql.Tx,
	preservedSchema string,
) (*realmResidual, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT TOP (1) residual.category, residual.schema_name, residual.object_name, residual.detail
		FROM (
			SELECT
				N'object' AS category,
				s.name AS schema_name,
				o.name AS object_name,
				o.type_desc AS detail
			FROM sys.objects AS o
			JOIN sys.schemas AS s ON s.schema_id = o.schema_id
			WHERE o.is_ms_shipped = 0
			  AND s.name NOT IN (
				  N'sys', N'INFORMATION_SCHEMA', N'guest',
				  N'db_owner', N'db_accessadmin', N'db_securityadmin',
				  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
				  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
			  )

			UNION ALL

			SELECT N'type', s.name, t.name, N'USER_DEFINED_TYPE'
			FROM sys.types AS t
			JOIN sys.schemas AS s ON s.schema_id = t.schema_id
			WHERE t.is_user_defined = 1
			  AND s.name NOT IN (
				  N'sys', N'INFORMATION_SCHEMA', N'guest',
				  N'db_owner', N'db_accessadmin', N'db_securityadmin',
				  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
				  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
			  )

			UNION ALL

			SELECT N'XML schema collection', s.name, x.name, N'XML_SCHEMA_COLLECTION'
			FROM sys.xml_schema_collections AS x
			JOIN sys.schemas AS s ON s.schema_id = x.schema_id
			WHERE x.xml_collection_id > 1
			  AND s.name NOT IN (
				  N'sys', N'INFORMATION_SCHEMA', N'guest',
				  N'db_owner', N'db_accessadmin', N'db_securityadmin',
				  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
				  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
			  )

			UNION ALL

			SELECT N'schema', N'', s.name, N'USER_SCHEMA'
			FROM sys.schemas AS s
			WHERE s.name NOT IN (
				  N'dbo', N'sys', N'INFORMATION_SCHEMA', N'guest',
				  N'db_owner', N'db_accessadmin', N'db_securityadmin',
				  N'db_ddladmin', N'db_backupoperator', N'db_datareader',
				  N'db_datawriter', N'db_denydatareader', N'db_denydatawriter'
			  )
			  AND s.name <> @p1
		) AS residual
		ORDER BY residual.category, residual.schema_name, residual.object_name
	`, preservedSchema)

	var residual realmResidual
	if err := row.Scan(&residual.Category, &residual.Schema, &residual.Name, &residual.Detail); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("sqlserver: verify database realm cleanup: %w", err)
		}
		return findUnsupportedRealmArtifact(ctx, tx, preservedSchema)
	}
	return &residual, nil
}

const unsupportedRealmArtifactQuery = `
		SELECT TOP (1)
			artifact.category,
			N'' AS schema_name,
			artifact.object_name,
			artifact.detail
		FROM (
			SELECT
				N'database DDL trigger' AS category,
				t.name AS object_name,
				t.type_desc AS detail
			FROM sys.triggers AS t
			WHERE t.parent_class = 0
			  AND t.is_ms_shipped = 0

			UNION ALL

			SELECT
				N'replication-enabled database',
				d.name,
				CONCAT(
					N'is_distributor=', d.is_distributor,
					N', is_published=', d.is_published,
					N', is_merge_published=', d.is_merge_published
				)
			FROM sys.databases AS d
			WHERE d.name = DB_NAME()
			  AND (
				  d.is_distributor = 1
				  OR d.is_published = 1
				  OR d.is_merge_published = 1
			  )

			UNION ALL

			SELECT
				CASE
					WHEN TRY_CONVERT(int, subscription.raw_value) = 1
						THEN N'subscriber database'
					ELSE N'unknown database subscription state'
				END,
				DB_NAME(),
				CONCAT(
					N'IsSubscribed=',
					COALESCE(TRY_CONVERT(nvarchar(128), subscription.raw_value), N'<unavailable>')
				)
			FROM (
				SELECT DATABASEPROPERTYEX(DB_NAME(), N'IsSubscribed') AS raw_value
			) AS subscription
			WHERE COALESCE(TRY_CONVERT(int, subscription.raw_value), -1) <> 0

			UNION ALL

			SELECT
				N'replicated table',
				CONCAT(s.name, N'.', t.name),
				CONCAT(
					N'is_replicated=', t.is_replicated,
					N', is_merge_published=', t.is_merge_published,
					N', is_sync_tran_subscribed=', t.is_sync_tran_subscribed
				)
			FROM sys.tables AS t
			JOIN sys.schemas AS s ON s.schema_id = t.schema_id
			WHERE t.is_ms_shipped = 0
			  AND (
				  t.is_replicated = 1
				  OR t.is_merge_published = 1
				  OR t.is_sync_tran_subscribed = 1
			  )

			UNION ALL

			SELECT N'database principal', p.name, p.type_desc
			FROM sys.database_principals AS p
			WHERE p.principal_id > 4
			  AND p.is_fixed_role = 0
			  AND p.principal_id <> DATABASE_PRINCIPAL_ID()

			UNION ALL

			SELECT
				N'database role membership',
				CONCAT(role_principal.name, N': ', member_principal.name),
				N'DATABASE_ROLE_MEMBERSHIP'
			FROM sys.database_role_members AS membership
			JOIN sys.database_principals AS role_principal
				ON role_principal.principal_id = membership.role_principal_id
			JOIN sys.database_principals AS member_principal
				ON member_principal.principal_id = membership.member_principal_id
			WHERE NOT (
				role_principal.name = N'db_owner'
				AND member_principal.principal_id = DATABASE_PRINCIPAL_ID()
			)

			UNION ALL

			SELECT
				N'schema authorization',
				CONCAT(s.name, N': ', principal.name),
				N'SCHEMA_AUTHORIZATION'
			FROM sys.schemas AS s
			JOIN sys.database_principals AS principal
				ON principal.principal_id = s.principal_id
			WHERE s.name IN (N'dbo', @p1)
			  AND s.principal_id <> DATABASE_PRINCIPAL_ID(N'dbo')

			UNION ALL

			SELECT N'assembly', a.name, N'USER_ASSEMBLY'
			FROM sys.assemblies AS a
			WHERE a.is_user_defined = 1

			UNION ALL

			SELECT N'filegroup', f.name, N'FILEGROUP'
			FROM sys.filegroups AS f
			WHERE f.data_space_id > 1

			UNION ALL

			SELECT N'database file', f.name, f.type_desc
			FROM sys.database_files AS f
			WHERE f.file_id > 2

			UNION ALL

			SELECT N'partition function', f.name, N'PARTITION_FUNCTION'
			FROM sys.partition_functions AS f
			WHERE f.is_system = 0

			UNION ALL

			SELECT N'full-text catalog', c.name, N'FULLTEXT_CATALOG'
			FROM sys.fulltext_catalogs AS c

			UNION ALL

			SELECT N'full-text stoplist', s.name, N'FULLTEXT_STOPLIST'
			FROM sys.fulltext_stoplists AS s

			UNION ALL

			SELECT N'search property list', p.name, N'SEARCH_PROPERTY_LIST'
			FROM sys.registered_search_property_lists AS p

			UNION ALL

			SELECT N'database-scoped credential', c.name, N'DATABASE_SCOPED_CREDENTIAL'
			FROM sys.database_scoped_credentials AS c

			UNION ALL

			SELECT N'external data source', s.name, N'EXTERNAL_DATA_SOURCE'
			FROM sys.external_data_sources AS s

			UNION ALL

			SELECT N'external file format', f.name, N'EXTERNAL_FILE_FORMAT'
			FROM sys.external_file_formats AS f

			UNION ALL

			SELECT N'external library', l.name, N'EXTERNAL_LIBRARY'
			FROM sys.external_libraries AS l

			UNION ALL

			SELECT N'external language', l.language, N'EXTERNAL_LANGUAGE'
			FROM sys.external_languages AS l
			WHERE l.principal_id <> DATABASE_PRINCIPAL_ID(N'sys')

			UNION ALL

			SELECT N'certificate', c.name, N'CERTIFICATE'
			FROM sys.certificates AS c

			UNION ALL

			SELECT N'asymmetric key', k.name, N'ASYMMETRIC_KEY'
			FROM sys.asymmetric_keys AS k

			UNION ALL

			SELECT N'symmetric key', k.name, N'SYMMETRIC_KEY'
			FROM sys.symmetric_keys AS k

			UNION ALL

			SELECT N'column master key', k.name, N'COLUMN_MASTER_KEY'
			FROM sys.column_master_keys AS k

			UNION ALL

			SELECT N'column encryption key', k.name, N'COLUMN_ENCRYPTION_KEY'
			FROM sys.column_encryption_keys AS k

			UNION ALL

			SELECT N'database audit specification', s.name, N'DATABASE_AUDIT_SPECIFICATION'
			FROM sys.database_audit_specifications AS s

			UNION ALL

			SELECT N'database event notification', n.name, N'EVENT_NOTIFICATION'
			FROM sys.event_notifications AS n
			WHERE n.parent_class = 0

			UNION ALL

			SELECT
				N'Service Broker message type',
				m.name COLLATE DATABASE_DEFAULT,
				N'SERVICE_MESSAGE_TYPE'
			FROM sys.service_message_types AS m
			WHERE m.message_type_id >= 65536

			UNION ALL

			SELECT
				N'Service Broker contract',
				c.name COLLATE DATABASE_DEFAULT,
				N'SERVICE_CONTRACT'
			FROM sys.service_contracts AS c
			WHERE c.service_contract_id >= 65536

			UNION ALL

			SELECT
				N'Service Broker service',
				s.name COLLATE DATABASE_DEFAULT,
				N'SERVICE'
			FROM sys.services AS s
			WHERE s.service_id >= 65536

			UNION ALL

			SELECT
				N'Service Broker route',
				r.name COLLATE DATABASE_DEFAULT,
				N'ROUTE'
			FROM sys.routes AS r
			WHERE r.name <> N'AutoCreatedLocal'

			UNION ALL

			SELECT
				N'remote service binding',
				b.name COLLATE DATABASE_DEFAULT,
				N'REMOTE_SERVICE_BINDING'
			FROM sys.remote_service_bindings AS b

			UNION ALL

			SELECT
				N'conversation priority',
				p.name COLLATE DATABASE_DEFAULT,
				N'CONVERSATION_PRIORITY'
			FROM sys.conversation_priorities AS p

			UNION ALL

			SELECT
				N'database-scoped configuration',
				c.name,
				CONCAT(N'VALUE=', CONVERT(nvarchar(128), c.value))
			FROM sys.database_scoped_configurations AS c
			WHERE c.is_value_default = 0

			UNION ALL

			SELECT
				N'database permission',
				CONCAT(p.state_desc, N' ', p.permission_name, N' TO ', principal.name),
				N'DATABASE_PERMISSION'
			FROM sys.database_permissions AS p
			JOIN sys.database_principals AS principal
				ON principal.principal_id = p.grantee_principal_id
			WHERE p.class = 0
			  AND NOT (
				  p.grantee_principal_id = 0
				  AND p.permission_name IN (
					  N'VIEW ANY COLUMN ENCRYPTION KEY DEFINITION',
					  N'VIEW ANY COLUMN MASTER KEY DEFINITION'
				  )
			  )
			  AND NOT (
				  p.grantee_principal_id IN (1, DATABASE_PRINCIPAL_ID())
				  AND p.permission_name = N'CONNECT'
			  )

			UNION ALL

			SELECT
				N'schema permission',
				CONCAT(
					s.name,
					N': ',
					p.state_desc,
					N' ',
					p.permission_name,
					N' TO ',
					principal.name
				),
				N'SCHEMA_PERMISSION'
			FROM sys.database_permissions AS p
			JOIN sys.schemas AS s ON s.schema_id = p.major_id
			JOIN sys.database_principals AS principal
				ON principal.principal_id = p.grantee_principal_id
			WHERE p.class = 3
			  AND s.name IN (N'dbo', @p1)

			UNION ALL

			SELECT N'database extended property', p.name, N'EXTENDED_PROPERTY'
			FROM sys.extended_properties AS p
			WHERE p.class = 0
			   OR (
				   p.class = 3
				   AND p.major_id IN (SCHEMA_ID(N'dbo'), SCHEMA_ID(@p1))
			   )
		) AS artifact
		ORDER BY artifact.category, artifact.object_name
	`

func findUnsupportedRealmArtifact(
	ctx context.Context,
	tx *sql.Tx,
	preservedSchema string,
) (*realmResidual, error) {
	row := tx.QueryRowContext(ctx, unsupportedRealmArtifactQuery, preservedSchema)
	var artifact realmResidual
	if err := row.Scan(&artifact.Category, &artifact.Schema, &artifact.Name, &artifact.Detail); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("sqlserver: inspect unsupported database-scoped artifacts: %w", err)
	}
	return &artifact, nil
}

func quoteResidual(residual realmResidual) string {
	if residual.Schema == "" {
		return quoteIdent(residual.Name)
	}
	return quoteQualified(residual.Schema, residual.Name)
}

func (w *Writer) rejectExternalForeignKeys(foreignKeys []foreignKey) error {
	var blockers []string
	for _, fk := range foreignKeys {
		if strings.EqualFold(fk.Schema, w.schema) {
			continue
		}
		blockers = append(blockers, fmt.Sprintf(
			"%s.%s.%s references %s.%s",
			fk.Schema,
			fk.Table,
			fk.Name,
			fk.ReferencedSchema,
			fk.ReferencedTable,
		))
	}
	if len(blockers) == 0 {
		return nil
	}
	return fmt.Errorf(
		"sqlserver: cannot drop schema %s tables because external foreign keys reference them: %s",
		quoteIdent(w.schema),
		strings.Join(blockers, "; "),
	)
}

func (w *Writer) listTables(ctx context.Context) ([]types.DBTable, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT s.name, t.name
		FROM sys.tables AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_ms_shipped = 0
		  AND s.name = @p1
		ORDER BY s.name, t.name
	`, w.schema)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list tables: %w", err)
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
		if err := rows.Scan(&table.Schema, &table.Name); err != nil {
			return nil, fmt.Errorf("sqlserver: scan table name: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate tables: %w", err)
	}
	return tables, nil
}

func (w *Writer) listForeignKeys(ctx context.Context) ([]foreignKey, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT ps.name, pt.name, fk.name, rs.name, rt.name
		FROM sys.foreign_keys AS fk
		JOIN sys.tables AS pt ON pt.object_id = fk.parent_object_id
		JOIN sys.schemas AS ps ON ps.schema_id = pt.schema_id
		JOIN sys.tables AS rt ON rt.object_id = fk.referenced_object_id
		JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
		WHERE pt.is_ms_shipped = 0
		  AND rt.is_ms_shipped = 0
		  AND (ps.name = @p1 OR rs.name = @p1)
		ORDER BY ps.name, pt.name, fk.name
	`, w.schema)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list foreign keys: %w", err)
	}
	defer rows.Close()

	var foreignKeys []foreignKey
	for rows.Next() {
		var fk foreignKey
		if err := rows.Scan(&fk.Schema, &fk.Table, &fk.Name, &fk.ReferencedSchema, &fk.ReferencedTable); err != nil {
			return nil, fmt.Errorf("sqlserver: scan foreign key: %w", err)
		}
		foreignKeys = append(foreignKeys, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate foreign keys: %w", err)
	}
	return foreignKeys, nil
}

func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

func (w *Writer) IsDryRun() bool { return w.dryRun }
