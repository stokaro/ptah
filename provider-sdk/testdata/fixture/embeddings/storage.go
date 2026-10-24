package embeddings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// The columns a generation writes beside its vector, as suffixes of the
// vector column's name: the names the PostgreSQL and YDB layouts use.
const (
	generationSuffix = "_generation"
	inputHashSuffix  = "_input_hash"
	versionSuffix    = "_source_version"
	stateSuffix      = "_state"
	ordinalSuffix    = "_chunk_ordinal"
)

// columnInfo is one column as PRAGMA table_info reports it.
type columnInfo struct {
	name, kind string
	notNull    bool
	primary    int
}

// targetColumns are the five columns a generation adds, with their types.
func targetColumns(column string) []columnInfo {
	return []columnInfo{{name: column, kind: ColumnType}, {name: column + generationSuffix, kind: "TEXT"},
		{name: column + inputHashSuffix, kind: "TEXT"}, {name: column + versionSuffix, kind: "TEXT"}, {name: column + stateSuffix, kind: "TEXT"}}
}

// describeTable reads a table's columns. exists is false when there is no
// such table.
func describeTable(ctx context.Context, connection *sql.Conn, table string) (columns []columnInfo, exists bool, err error) {
	if exists, err = tableExists(ctx, connection, table); err != nil || !exists {
		return nil, exists, err
	}
	rows, err := connection.QueryContext(ctx, "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?)", table)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var column columnInfo
		if err := rows.Scan(&column.name, &column.kind, &column.notNull, &column.primary); err != nil {
			return nil, false, err
		}
		columns = append(columns, column)
	}
	return columns, true, rows.Err()
}

// find returns a column by name, compared as SQLite compares names.
func find(columns []columnInfo, name string) (columnInfo, bool) {
	for _, column := range columns {
		if strings.EqualFold(column.name, name) {
			return column, true
		}
	}
	return columnInfo{}, false
}

// primaryKey lists a table's primary key columns in key order.
func primaryKey(columns []columnInfo) []string {
	var key []columnInfo
	for _, column := range columns {
		if column.primary > 0 {
			key = append(key, column)
		}
	}
	slices.SortFunc(key, func(a, b columnInfo) int { return a.primary - b.primary })
	names := make([]string, len(key))
	for i, column := range key {
		names[i] = column.name
	}
	return names
}

// keyColumns matches key fields with a table's primary key, which is what
// orders a keyset scan and addresses one row, and returns the key's columns.
func keyColumns(columns []columnInfo, fields []string, table string) ([]columnInfo, *protocol.EmbeddingRefusal) {
	key := primaryKey(columns)
	if !slices.EqualFunc(key, fields, strings.EqualFold) {
		return nil, refused(protocol.EmbeddingRefusal_UNSUPPORTED, fmt.Sprintf("the key fields are not the primary key of %s, which is %v", table, key))
	}
	result := make([]columnInfo, len(fields))
	for i, field := range fields {
		result[i], _ = find(columns, field)
	}
	return result, nil
}

// offlineRefusal is what the fixture cannot hold, decided without a
// database: another representation, an index method, a schema other than
// main, and an owned table that is the source itself.
func offlineRefusal(spec *protocol.EmbeddingSpec) *protocol.EmbeddingRefusal {
	target, source := spec.GetTarget(), spec.GetSource()
	switch {
	case !strings.EqualFold(target.GetRepresentation(), Representation):
		return refused(protocol.EmbeddingRefusal_UNSUPPORTED, fmt.Sprintf("the fixture stores representation %s, not %s", Representation, target.GetRepresentation()))
	case target.GetIndexMethod() != "":
		return refused(protocol.EmbeddingRefusal_UNSUPPORTED, "the fixture builds no vector index, so a specification declares no index method")
	}
	sourceTable, sourceErr := tableName(source.GetTable())
	targetTable, targetErr := tableName(target.GetTable())
	switch {
	case sourceErr != nil || targetErr != nil:
		return refused(protocol.EmbeddingRefusal_UNSUPPORTED, "the fixture has no schema other than main")
	case target.GetLayout() == protocol.EmbeddingTarget_OWN_TABLE && strings.EqualFold(sourceTable, targetTable):
		return refused(protocol.EmbeddingRefusal_INVALID, "a generation that owns its table cannot own the source table "+sourceTable)
	}
	return nil
}

// DescribeEmbeddingTarget answers what a generation needs on the fixture.
func (p *Provider) DescribeEmbeddingTarget(_ context.Context, request *protocol.DescribeEmbeddingTargetRequest) (*protocol.EmbeddingTargetObjects, error) {
	spec := request.GetSpec()
	if refusal := offlineRefusal(spec); refusal != nil {
		return &protocol.EmbeddingTargetObjects{Refusal: refusal}, nil
	}
	return &protocol.EmbeddingTargetObjects{ColumnType: ColumnType, OwnsTable: spec.GetTarget().GetLayout() == protocol.EmbeddingTarget_OWN_TABLE}, nil
}

// ResolveEmbeddingRelations answers each relation with the table's own
// spelling of its name, and a relation that names no table as written.
func (p *Provider) ResolveEmbeddingRelations(ctx context.Context, request *protocol.ResolveEmbeddingRelationsRequest) (*protocol.EmbeddingRelations, error) {
	result := &protocol.EmbeddingRelations{}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		for _, relation := range request.GetRelations() {
			resolved := &protocol.Reference{Kind: relation.GetKind(), Schema: relation.GetSchema(), Name: relation.GetName()}
			var stored string
			err := connection.QueryRowContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE", relation.GetName()).Scan(&stored)
			switch {
			case err == nil:
				resolved.Name = stored
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}
			result.Relations = append(result.Relations, resolved)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MeasureEmbedding answers what a plan reports.
func (p *Provider) MeasureEmbedding(ctx context.Context, request *protocol.MeasureEmbeddingRequest) (*protocol.EmbeddingMeasurement, error) {
	spec := request.GetSpec()
	measured := &protocol.EmbeddingMeasurement{Capabilities: map[string]bool{"vector_type": true, "vector_index": false}, IndexBuildable: spec.GetTarget().GetIndexMethod() == ""}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		if refusal := offlineRefusal(spec); refusal != nil {
			measured.Refusals = append(measured.Refusals, refusal.GetMessage())
			return nil
		}
		target, _ := tableName(spec.GetTarget().GetTable())
		targetColumns, targetExists, err := describeTable(ctx, connection, target)
		if err != nil {
			return err
		}
		// A table the generation owns does not exist before prepare creates it,
		// and that is not a plan that cannot run.
		measured.TargetTableExists = targetExists || spec.GetTarget().GetLayout() == protocol.EmbeddingTarget_OWN_TABLE
		_, measured.TargetColumnExists = find(targetColumns, spec.GetTarget().GetColumn())
		source, _ := tableName(spec.GetSource().GetTable())
		columns, exists, err := describeTable(ctx, connection, source)
		if err != nil || !exists {
			return err
		}
		measured.SourceExists = true
		if _, refusal := keyColumns(columns, spec.GetSource().GetKeyFields(), source); refusal != nil {
			measured.Refusals = append(measured.Refusals, refusal.GetMessage())
		}
		measured.Rows, err = countSource(ctx, connection, spec.GetSource())
		measured.RowsCounted = err == nil
		return err
	})
	if err != nil {
		return nil, err
	}
	return measured, nil
}

// filterClause is the source filter as a predicate, or one that selects every
// row.
func filterClause(source *protocol.EmbeddingSource) string {
	if filter := strings.TrimSpace(source.GetFilter()); filter != "" {
		return "(" + filter + ")"
	}
	return "1"
}

// countSource counts the source rows the filter selects.
func countSource(ctx context.Context, connection *sql.Conn, source *protocol.EmbeddingSource) (int64, error) {
	table, _ := tableName(source.GetTable())
	var count int64
	// #nosec G202 -- the table is quoted, and the filter is the specification's own SQL, which is what the field is for.
	err := connection.QueryRowContext(ctx, "SELECT count(*) FROM "+quote(table)+" WHERE "+filterClause(source)).Scan(&count)
	return count, err
}

// PrepareEmbeddingTarget creates a generation's storage or verifies it, in
// one transaction: SQLite's schema statements are transactional, so a
// preparation either completes or leaves nothing behind.
func (p *Provider) PrepareEmbeddingTarget(ctx context.Context, request *protocol.PrepareEmbeddingTargetRequest) (*protocol.EmbeddingWriteResult, error) {
	spec := request.GetSpec()
	refusal := offlineRefusal(spec)
	if refusal != nil {
		return writeResult(request.GetRequestId(), refusal, nil)
	}
	create, begin := request.GetMode() == protocol.PrepareEmbeddingTargetRequest_CREATE, reading
	if create {
		begin = writing
	}
	err := p.withConnection(ctx, request.GetSessionId(), begin, func(connection *sql.Conn, _ string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil {
			return err
		}
		if !exists {
			return errStoreMissing
		}
		source, _ := tableName(spec.GetSource().GetTable())
		sourceColumns, sourceExists, err := describeTable(ctx, connection, source)
		if err != nil {
			return err
		}
		if !sourceExists {
			refusal = refused(protocol.EmbeddingRefusal_ABSENT, "source table "+source+" does not exist")
			return errRefused
		}
		key, keyRefusal := keyColumns(sourceColumns, spec.GetSource().GetKeyFields(), source)
		if keyRefusal != nil {
			refusal = keyRefusal
			return errRefused
		}
		if spec.GetTarget().GetLayout() == protocol.EmbeddingTarget_OWN_TABLE {
			refusal, err = prepareOwnedTable(ctx, connection, spec, key, create)
		} else {
			refusal, err = prepareColumns(ctx, connection, spec, create)
		}
		if refusal != nil {
			return errRefused
		}
		return err
	})
	return writeResult(request.GetRequestId(), refusal, err)
}

// prepareColumns adds the generation's five columns to the target table, or
// checks that they are there. A column of the right name and another type is
// refused rather than written into, and so is a column that holds another
// generation's vectors.
func prepareColumns(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, create bool) (*protocol.EmbeddingRefusal, error) {
	table, _ := tableName(spec.GetTarget().GetTable())
	columns, exists, err := describeTable(ctx, connection, table)
	if err != nil {
		return nil, err
	}
	if !exists {
		return refused(protocol.EmbeddingRefusal_ABSENT, "target table "+table+" does not exist"), nil
	}
	if _, refusal := keyColumns(columns, spec.GetSource().GetKeyFields(), table); refusal != nil {
		return refusal, nil
	}
	column := spec.GetTarget().GetColumn()
	for _, wanted := range targetColumns(column) {
		present, found := find(columns, wanted.name)
		switch {
		case found && !strings.EqualFold(present.kind, wanted.kind):
			return refused(protocol.EmbeddingRefusal_INVALID, fmt.Sprintf("column %s on %s is %s, and a generation needs %s", wanted.name, table, present.kind, wanted.kind)), nil
		case !found && !create:
			return refused(protocol.EmbeddingRefusal_ABSENT, "prepared target column "+table+"."+wanted.name+" is absent"), nil
		case !found:
			if _, err := connection.ExecContext(ctx, "ALTER TABLE "+quote(table)+" ADD COLUMN "+quote(wanted.name)+" "+wanted.kind); err != nil {
				return nil, err
			}
		}
	}
	var occupant string
	// #nosec G202 -- the names are quoted.
	err = connection.QueryRowContext(ctx, "SELECT "+quote(column+generationSuffix)+" FROM "+quote(table)+" WHERE "+quote(column+generationSuffix)+
		" IS NOT NULL AND "+quote(column+generationSuffix)+" != ? LIMIT 1", spec.GetGeneration()).Scan(&occupant)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return refused(protocol.EmbeddingRefusal_NOT_OWNED, fmt.Sprintf("column %s on %s holds another generation", column, table)), nil
}

// ownedColumns are an owned table's columns in the order it declares them:
// the source key with the source's types, the chunk ordinal, and the five
// columns of a generation.
func ownedColumns(key []columnInfo, column string) []columnInfo {
	columns := make([]columnInfo, 0, len(key)+6)
	for i, part := range key {
		columns = append(columns, columnInfo{name: part.name, kind: part.kind, notNull: true, primary: i + 1})
	}
	columns = append(columns, columnInfo{name: column + ordinalSuffix, kind: "INTEGER", notNull: true, primary: len(key) + 1})
	return append(columns, targetColumns(column)...)
}

// prepareOwnedTable creates the table a generation owns and marks it as the
// generation's, or checks that it is there and marked. A table without the
// mark is adopted only when it is empty and has exactly the shape a
// generation creates.
func prepareOwnedTable(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, key []columnInfo, create bool) (*protocol.EmbeddingRefusal, error) {
	table, _ := tableName(spec.GetTarget().GetTable())
	mark := spec.GetNames().GetOwnershipMark()
	wanted := ownedColumns(key, spec.GetTarget().GetColumn())
	columns, exists, err := describeTable(ctx, connection, table)
	if err != nil {
		return nil, err
	}
	switch {
	case !exists && !create:
		return refused(protocol.EmbeddingRefusal_ABSENT, "prepared target table "+table+" is absent"), nil
	case !exists:
		if _, err := connection.ExecContext(ctx, createOwnedTable(table, wanted)); err != nil {
			return nil, err
		}
		return nil, writeMark(ctx, connection, table, mark)
	}
	found, err := ownershipMark(ctx, connection, table)
	switch {
	case err != nil:
		return nil, err
	case found == mark:
		return nil, nil
	case found != "" || !create || !sameShape(columns, wanted):
		return refused(protocol.EmbeddingRefusal_NOT_OWNED, "table "+table+" is not the generation's"), nil
	}
	var rows int
	if err := connection.QueryRowContext(ctx, "SELECT count(*) FROM "+quote(table)).Scan(&rows); err != nil {
		return nil, err
	}
	if rows != 0 {
		return refused(protocol.EmbeddingRefusal_NOT_OWNED, "table "+table+" has the shape a generation creates and holds rows, so it is not the generation's to adopt"), nil
	}
	return nil, writeMark(ctx, connection, table, mark)
}

// sameShape reports whether a table has exactly the columns a generation
// creates, in order.
func sameShape(columns, wanted []columnInfo) bool {
	return slices.EqualFunc(columns, wanted, func(a, b columnInfo) bool {
		return strings.EqualFold(a.name, b.name) && strings.EqualFold(a.kind, b.kind) && a.notNull == b.notNull && a.primary == b.primary
	})
}

// createOwnedTable renders an owned table's CREATE TABLE.
func createOwnedTable(table string, columns []columnInfo) string {
	definitions := make([]string, 0, len(columns))
	var key []string
	for _, column := range columns {
		definition := quote(column.name) + " " + column.kind
		if column.notNull {
			definition += " NOT NULL"
		}
		definitions = append(definitions, definition)
		if column.primary > 0 {
			key = append(key, quote(column.name))
		}
	}
	return "CREATE TABLE " + quote(table) + " (" + strings.Join(definitions, ", ") + ", PRIMARY KEY (" + strings.Join(key, ", ") + "))"
}

// ownershipMark reads the mark a table carries, empty when it carries none.
func ownershipMark(ctx context.Context, connection *sql.Conn, table string) (string, error) {
	var mark string
	err := connection.QueryRowContext(ctx, `SELECT "mark" FROM "`+ownerTable+`" WHERE "table_name" = ? COLLATE NOCASE`, table).Scan(&mark)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return mark, err
}

func writeMark(ctx context.Context, connection *sql.Conn, table, mark string) error {
	_, err := connection.ExecContext(ctx, `INSERT OR REPLACE INTO "`+ownerTable+`" ("table_name", "mark") VALUES (?, ?)`, table, mark)
	return err
}

// BuildEmbeddingIndex answers that no index is declared: the fixture refuses
// a specification that declares one before anything is created.
func (p *Provider) BuildEmbeddingIndex(_ context.Context, request *protocol.BuildEmbeddingIndexRequest) (*protocol.EmbeddingWriteResult, error) {
	if refusal := offlineRefusal(request.GetSpec()); refusal != nil {
		return writeResult(request.GetRequestId(), refusal, nil)
	}
	return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_APPLIED,
		IndexOutcome: protocol.EmbeddingWriteResult_NOT_DECLARED}, nil
}

// ReadEmbeddingStructure reads a generation's storage. A BLOB declares no
// width, so the dimension is the generation's own; verification measures
// each stored vector's width row by row.
func (p *Provider) ReadEmbeddingStructure(ctx context.Context, request *protocol.ReadEmbeddingStructureRequest) (*protocol.EmbeddingStructure, error) {
	location := request.GetLocation()
	structure := &protocol.EmbeddingStructure{VectorSupport: true, SearchSettings: "fixture.search=exhaustive", RowsCounted: request.GetCountRows()}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		table, err := tableName(location.GetTable())
		if err != nil {
			return err
		}
		columns, exists, err := describeTable(ctx, connection, table)
		if err != nil || !exists {
			return err
		}
		structure.TableExists = true
		column, found := find(columns, location.GetColumn())
		if found {
			structure.ColumnExists, structure.ColumnType, structure.Dimension = true, column.kind, location.GetDimension()
		}
		if _, tracked := find(columns, location.GetColumn()+generationSuffix); !request.GetCountRows() || !tracked {
			return nil
		}
		// #nosec G202 -- the names are quoted.
		return connection.QueryRowContext(ctx, "SELECT count(*) FROM "+quote(table)+" WHERE "+quote(location.GetColumn()+generationSuffix)+" = ?",
			location.GetGeneration()).Scan(&structure.Rows)
	})
	if err != nil {
		return nil, err
	}
	return structure, nil
}

// DropEmbeddingGeneration drops a retired generation's columns or table,
// after checking that they are the generation's, in one transaction.
func (p *Provider) DropEmbeddingGeneration(ctx context.Context, request *protocol.DropEmbeddingGenerationRequest) (*protocol.EmbeddingWriteResult, error) {
	location := request.GetLocation()
	var refusal *protocol.EmbeddingRefusal
	err := p.withConnection(ctx, request.GetSessionId(), writing, func(connection *sql.Conn, _ string) error {
		table, err := tableName(location.GetTable())
		if err != nil {
			return err
		}
		columns, exists, err := describeTable(ctx, connection, table)
		if err != nil || !exists {
			return err
		}
		switch request.GetStorage() {
		case protocol.DropEmbeddingGenerationRequest_TABLE:
			refusal, err = dropTable(ctx, connection, table, location.GetNames().GetOwnershipMark())
		case protocol.DropEmbeddingGenerationRequest_COLUMNS:
			refusal, err = dropColumns(ctx, connection, table, columns, location)
		}
		if refusal != nil {
			return errRefused
		}
		return err
	})
	return writeResult(request.GetRequestId(), refusal, err)
}

// dropTable drops a table only when its mark is the generation's.
func dropTable(ctx context.Context, connection *sql.Conn, table, mark string) (*protocol.EmbeddingRefusal, error) {
	found, err := ownershipMark(ctx, connection, table)
	if err != nil {
		return nil, err
	}
	if found != mark {
		return refused(protocol.EmbeddingRefusal_NOT_OWNED, fmt.Sprintf("table %s is not the generation's to drop: it carries the mark %q", table, found)), nil
	}
	if _, err := connection.ExecContext(ctx, "DROP TABLE "+quote(table)); err != nil {
		return nil, err
	}
	_, err = connection.ExecContext(ctx, `DELETE FROM "`+ownerTable+`" WHERE "table_name" = ? COLLATE NOCASE`, table)
	return nil, err
}

// dropColumns drops a generation's five columns, refusing when the column
// holds another generation's vectors.
func dropColumns(ctx context.Context, connection *sql.Conn, table string, columns []columnInfo, location *protocol.EmbeddingLocation) (*protocol.EmbeddingRefusal, error) {
	column := location.GetColumn()
	if _, tracked := find(columns, column+generationSuffix); tracked {
		var other int
		// #nosec G202 -- the names are quoted.
		if err := connection.QueryRowContext(ctx, "SELECT count(*) FROM "+quote(table)+" WHERE "+quote(column+generationSuffix)+" != ?",
			location.GetGeneration()).Scan(&other); err != nil {
			return nil, err
		}
		if other != 0 {
			return refused(protocol.EmbeddingRefusal_NOT_OWNED, fmt.Sprintf("column %s on %s holds another generation", column, table)), nil
		}
	}
	for _, wanted := range targetColumns(column) {
		if _, found := find(columns, wanted.name); found {
			if _, err := connection.ExecContext(ctx, "ALTER TABLE "+quote(table)+" DROP COLUMN "+quote(wanted.name)); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}
