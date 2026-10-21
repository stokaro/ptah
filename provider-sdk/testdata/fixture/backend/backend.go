// Package backend implements a deliberately small SQLite-backed test engine.
// It depends on the public SDK and its own driver, never the Ptah host module.
package backend

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	// The fixture owns its database driver; the SDK and host transport do not.
	_ "modernc.org/sqlite"
)

// Provider owns isolated session pools for the fixture engine.
type Provider struct {
	mu       sync.Mutex
	sessions map[string]*session
}
type session struct {
	db    *sql.DB
	path  string
	fault string
}

// Description declares the fixture's intentionally narrow service contract.
func Description() *protocol.Description {
	return &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/fixture", Version: "0.0.1", SemanticIdentity: "fixture-schema-8",
		Engines: []*protocol.Engine{{Id: "fixture", DefaultSchema: "main", Features: []string{"tables", "columns", provider.CommonTableIdentityFeature},
			Identifiers: identifierRules(), ValueModels: valueModels(), YamlSourceKeys: yamlSourceKeys()}},
		Features: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature, provider.NormalizeValuesFeature, provider.DecodeSourceFeature}}
}

func identifierRules() *protocol.IdentifierRules {
	return &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
		ColumnNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE, IndexNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
		IndexNamespace: protocol.IdentifierRules_SCHEMA}
}

// Open creates an independent connection pool without changing the schema.
func (p *Provider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	if os.Getenv("PTAH_FIXTURE_HOST_SECRET") != "" {
		return nil, fmt.Errorf("host secret leaked into provider")
	}
	u, err := url.Parse(request.GetAddress())
	if err != nil {
		return nil, fmt.Errorf("invalid fixture address")
	}
	path := filepath.FromSlash(u.Path)
	if runtime.GOOS == "windows" && len(path) > 3 && path[0] == '\\' && path[2] == ':' {
		path = path[1:]
	}
	if u.Scheme != "fixture" || u.Host != "" || !filepath.IsAbs(path) {
		return nil, fmt.Errorf("fixture requires an absolute database file")
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open fixture database")
	}
	database.SetMaxOpenConns(1)
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("connect fixture database")
	}
	id := rand.Text()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sessions == nil {
		p.sessions = make(map[string]*session)
	}
	p.sessions[id] = &session{db: database, path: path, fault: u.Query().Get("fault")}
	return &protocol.Session{Id: id, Engine: "fixture", Database: path, ServerVersion: "fixture-1", DefaultSchema: "main", Identifiers: identifierRules()}, nil
}

func (p *Provider) lookup(id string) (*session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	found := p.sessions[id]
	if found == nil {
		return nil, fmt.Errorf("unknown or closed fixture session")
	}
	return found, nil
}

// Close releases one session. Repeated closes are harmless.
func (p *Provider) Close(_ context.Context, request *protocol.CloseRequest) error {
	p.mu.Lock()
	found := p.sessions[request.GetSessionId()]
	delete(p.sessions, request.GetSessionId())
	p.mu.Unlock()
	if found == nil {
		return nil
	}
	return found.db.Close()
}

// Inspect reads table and column metadata from the actual database file.
func (p *Provider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	s, err := p.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	if s.fault == "incomplete" {
		return &protocol.Schema{}, nil
	}
	if s.fault == "slow-inspect" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for _, schema := range request.GetSchemas() {
		if schema != "main" {
			return nil, fmt.Errorf("fixture supports only the main schema")
		}
	}
	var unsupported int
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type IN ('view', 'trigger', 'index') AND name NOT LIKE 'sqlite_%'").Scan(&unsupported); err != nil {
		return nil, err
	}
	if unsupported != 0 {
		return nil, fmt.Errorf("fixture cannot inspect views, triggers, or indexes")
	}
	names, err := tableNames(ctx, s.db)
	if err != nil {
		return nil, err
	}
	result := &protocol.Schema{Complete: true, Engine: "fixture", Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}}
	for _, name := range names {
		columns, err := tableColumns(ctx, s.db, name)
		if err != nil {
			return nil, err
		}
		table := &protocol.Table{Identity: &protocol.Reference{Engine: "fixture", Database: s.path, Kind: "table", Name: name, DisplayName: name}, Columns: columns}
		canonical, err := renderChange(&protocol.Change{Object: table.Identity, Operation: &protocol.Change_CreateTable{CreateTable: table}})
		if err != nil {
			return nil, err
		}
		var stored string
		if err := s.db.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?", name).Scan(&stored); err != nil {
			return nil, err
		}
		// This fixture accepts only definitions it can reconstruct exactly. CHECK,
		// foreign keys, generated columns, collations, and table options must not
		// disappear behind a complete table/column snapshot.
		if strings.TrimSuffix(strings.TrimSpace(stored), ";") != strings.TrimSuffix(canonical, ";") {
			return nil, fmt.Errorf("fixture cannot inspect this table definition completely")
		}
		result.Tables = append(result.Tables, table)
	}
	return result, nil
}

func tableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func tableColumns(ctx context.Context, db *sql.DB, name string) ([]*protocol.Column, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+quote(name)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []*protocol.Column
	for rows.Next() {
		var id, notNull, primary int
		var name, kind string
		var defaultValue sql.NullString
		if err := rows.Scan(&id, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
			return nil, err
		}
		column := &protocol.Column{Name: name, Type: kind, Nullable: notNull == 0 && primary == 0, PrimaryKey: primary != 0}
		if defaultValue.Valid {
			column.DefaultExpression = &defaultValue.String
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

// Render produces one SQL unit per typed common change, in request order.
func (*Provider) Render(_ context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		text, err := renderChange(change)
		if err != nil {
			return nil, err
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: text}})
	}
	return result, nil
}

func renderChange(change *protocol.Change) (string, error) {
	if change.GetObject().GetName() == "" || change.GetObject().GetSchema() != "" {
		return "", fmt.Errorf("fixture requires an unqualified table name")
	}
	name := quote(change.GetObject().GetName())
	switch operation := change.GetOperation().(type) {
	case *protocol.Change_CreateTable:
		if operation.CreateTable == nil || len(operation.CreateTable.GetColumns()) == 0 || len(operation.CreateTable.GetFacets()) != 0 {
			return "", fmt.Errorf("unsupported fixture table")
		}
		var columns []string
		for _, column := range operation.CreateTable.GetColumns() {
			text, err := renderColumn(column)
			if err != nil {
				return "", err
			}
			columns = append(columns, text)
		}
		return "CREATE TABLE " + name + " (" + strings.Join(columns, ", ") + ");", nil
	case *protocol.Change_DropTable:
		return "DROP TABLE " + name + ";", nil
	case *protocol.Change_AddColumn:
		if operation.AddColumn.GetPrimaryKey() {
			return "", fmt.Errorf("fixture cannot add a primary key column")
		}
		text, err := renderColumn(operation.AddColumn)
		if err != nil {
			return "", err
		}
		return "ALTER TABLE " + name + " ADD COLUMN " + text + ";", nil
	case *protocol.Change_DropColumn:
		if operation.DropColumn == "" {
			return "", fmt.Errorf("column name is required")
		}
		return "ALTER TABLE " + name + " DROP COLUMN " + quote(operation.DropColumn) + ";", nil
	default:
		return "", fmt.Errorf("fixture does not implement this change")
	}
}

func renderColumn(column *protocol.Column) (string, error) {
	if column.GetName() == "" || len(column.GetFacets()) != 0 || column.DefaultExpression != nil {
		return "", fmt.Errorf("unsupported fixture column")
	}
	if column.GetPrimaryKey() && column.GetNullable() {
		return "", fmt.Errorf("fixture requires an explicitly non-null primary key")
	}
	kind := strings.ToUpper(column.GetType())
	switch kind {
	case "INTEGER", "TEXT", "REAL", "BLOB":
	default:
		return "", fmt.Errorf("unsupported fixture column type")
	}
	text := quote(column.GetName()) + " " + kind
	if !column.GetNullable() {
		text += " NOT NULL"
	}
	if column.GetPrimaryKey() {
		text += " PRIMARY KEY"
	}
	return text, nil
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// Execute runs the exact submitted units. Fault modes exercise a lost reply
// after a real write; they do not simulate a successful database operation.
func (p *Provider) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	s, err := p.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	if s.fault == "reject" {
		return &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED, DiagnosticCode: "fixture.refused"}, nil
	}
	var applied uint32
	for _, unit := range request.GetUnits() {
		sqlText, ok := unit.GetBody().(*protocol.Unit_Sql)
		if !ok {
			return nil, fmt.Errorf("fixture accepts SQL units only")
		}
		if _, err := s.db.ExecContext(ctx, sqlText.Sql); err != nil {
			return &protocol.Execution{Outcome: protocol.Execution_UNKNOWN, AppliedCount: applied}, nil
		}
		if s.fault == "crash-after" {
			os.Exit(23) //revive:disable-line:deep-exit this fixture tests a provider crash after a committed write
		}
		if s.fault == "delay-after" {
			if err := os.WriteFile(s.path+".applied", []byte("applied"), 0o600); err != nil {
				return nil, err
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		applied++
		if s.fault == "partial" && int(applied) < len(request.GetUnits()) {
			return &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: applied}, nil
		}
	}
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: applied}, nil
}
