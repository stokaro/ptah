package backend

import (
	"context"
	"database/sql"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/runtime"
)

// Inspect reads every table of the database with its columns. The snapshot is
// complete or the call fails: an object the engine cannot describe, such as a
// view or a table with a CHECK constraint, fails the read rather than
// disappearing from it, because Ptah would plan to create what a read leaves
// out.
func (p *Provider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	current, err := p.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	for _, schema := range request.GetSchemas() {
		if schema != defaultSchema {
			return nil, runtime.ErrorWithReason(codes.InvalidArgument, "a tutorial database has the schema main only")
		}
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	if err := refuseOtherObjects(ctx, current.db); err != nil {
		return nil, err
	}
	names, err := tableNames(ctx, current.db)
	if err != nil {
		return nil, err
	}
	snapshot := &protocol.Schema{
		Complete: true,
		Engine:   Engine,
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED},
	}
	for _, name := range names {
		table, err := readTable(ctx, current, name)
		if err != nil {
			return nil, err
		}
		snapshot.Tables = append(snapshot.Tables, table)
	}
	// A read narrowed to schemas says which namespace each selector names.
	for _, selector := range request.GetSchemas() {
		snapshot.ScopeBindings = append(snapshot.ScopeBindings, &protocol.InspectionScopeBinding{
			Selector: selector,
			Namespaces: []*protocol.ModelNamespace{{
				Database: &protocol.NamespaceComponent{Source: current.path, Normalized: current.path},
				Catalog:  &protocol.NamespaceComponent{},
				Schema:   &protocol.NamespaceComponent{},
			}},
		})
	}
	return snapshot, nil
}

// refuseOtherObjects fails the read when the database holds an object the
// engine does not describe. SQLite names its own objects sqlite_*.
func refuseOtherObjects(ctx context.Context, db *sql.DB) error {
	var count int
	query := "SELECT count(*) FROM sqlite_schema WHERE type IN ('view', 'trigger', 'index') AND name NOT LIKE 'sqlite_%'"
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return readFailed(ctx)
	}
	if count != 0 {
		return runtime.ErrorWithReason(codes.FailedPrecondition, "the database holds a view, a trigger or an index, which the tutorial engine does not describe")
	}
	return nil
}

// tableNames lists the tables of the database in name order.
func tableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, readFailed(ctx)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, readFailed(ctx)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, readFailed(ctx)
	}
	return names, nil
}

// readTable reads one table and proves the read is complete: the definition
// SQLite stored must be the statement Render writes for the columns read.
func readTable(ctx context.Context, current *session, name string) (*protocol.Table, error) {
	columns, err := tableColumns(ctx, current.db, name)
	if err != nil {
		return nil, err
	}
	identity := &protocol.Reference{Engine: Engine, Database: current.path, Kind: "table", Name: name, DisplayName: name}
	table := &protocol.Table{Identity: identity, Columns: columns}
	rendered, err := createTable(quote(name), table)
	if err != nil {
		return nil, runtime.ErrorWithReason(codes.FailedPrecondition, "a table holds a column the tutorial engine does not describe")
	}
	var stored string
	if err := current.db.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?", name).Scan(&stored); err != nil {
		return nil, readFailed(ctx)
	}
	if strings.TrimSuffix(strings.TrimSpace(stored), ";") != strings.TrimSuffix(rendered, ";") {
		return nil, runtime.ErrorWithReason(codes.FailedPrecondition, "a table definition holds more than the tutorial engine describes")
	}
	// The name the engine compares: ASCII letters folded, as identifierRules says.
	table.NormalizedIdentity = proto.CloneOf(identity)
	table.NormalizedIdentity.Name = strings.ToLower(name)
	return table, nil
}

// tableColumns reads the columns of one table in their declared order.
func tableColumns(ctx context.Context, db *sql.DB, table string) ([]*protocol.Column, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?)", table)
	if err != nil {
		return nil, readFailed(ctx)
	}
	defer rows.Close()
	var columns []*protocol.Column
	for rows.Next() {
		var name, kind string
		var notNull, primary int
		var defaultValue sql.NullString
		if err := rows.Scan(&name, &kind, &notNull, &defaultValue, &primary); err != nil {
			return nil, readFailed(ctx)
		}
		column := &protocol.Column{Name: name, Type: kind, Nullable: notNull == 0 && primary == 0, PrimaryKey: primary != 0}
		if defaultValue.Valid {
			column.DefaultExpression = &defaultValue.String
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, readFailed(ctx)
	}
	return columns, nil
}

// readFailed reports a failed catalog query. A canceled call keeps the
// context's error; any other failure says what failed and leaves the
// driver's text out, because it can quote a value.
func readFailed(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return runtime.ErrorWithReason(codes.Unavailable, "the database catalog could not be read")
}
