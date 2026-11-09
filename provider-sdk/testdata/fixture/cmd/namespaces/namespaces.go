package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/devdb"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// defaultSchema is the schema every fixture database has, and the one a
// table without a schema is in.
const defaultSchema = "main"

// statementPrefix opens each schema statement the program's executor reads:
// `FIXTURE CREATE SCHEMA "app";` and `FIXTURE DROP SCHEMA "app";`.
const statementPrefix = "FIXTURE "

type namespaces struct {
	*devdb.Provider
	// carried reports that the engine declares namespaces.
	carried bool
	// uninspected reads every schema as uninspected.
	uninspected bool
}

// schemasPath is the file beside a database that holds the schemas created
// in it, other than main.
func schemasPath(database string) string { return database + ".schemas.json" }

func loadSchemas(database string) ([]string, error) {
	data, err := os.ReadFile(schemasPath(database)) // #nosec G304 -- the path is the session's own database file
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	return names, json.Unmarshal(data, &names)
}

func saveSchemas(database string, names []string) error {
	slices.Sort(names)
	data, err := json.Marshal(names)
	if err != nil {
		return err
	}
	return os.WriteFile(schemasPath(database), data, 0o600)
}

// splitTable is the schema and the name of a SQLite table: "app.t" is table
// t of schema app, and a name without a dot is in main.
func splitTable(stored string) (schema, name string) {
	if schema, name, qualified := strings.Cut(stored, "."); qualified {
		return schema, name
	}
	return defaultSchema, stored
}

// storedTable is the SQLite table of a table of schema.
func storedTable(schema, name string) string {
	if schema == "" || schema == defaultSchema {
		return name
	}
	return schema + "." + name
}

// Inspect reads the whole database and reports what lies in the request's
// schemas: the tables, each under its schema, and to a host that lists
// namespaces, the schemas themselves.
func (p *namespaces) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	whole := proto.CloneOf(request)
	whole.Schemas = nil
	snapshot, err := p.Provider.Inspect(ctx, whole)
	if err != nil {
		return nil, err
	}
	var path string
	var held []string
	err = p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		path = access.Path
		held, err = loadSchemas(access.Path)
		return err
	})
	if err != nil {
		return nil, err
	}
	scope := request.GetSchemas()
	inScope := func(schema string) bool { return len(scope) == 0 || slices.Contains(scope, schema) }
	tables := snapshot.Tables[:0]
	for _, table := range snapshot.GetTables() {
		schema, name := splitTable(table.GetIdentity().GetName())
		if !inScope(schema) {
			continue
		}
		if schema != defaultSchema {
			for _, identity := range []*protocol.Reference{table.GetIdentity(), table.GetNormalizedIdentity()} {
				identity.Schema, identity.Name = schema, name
			}
			table.Identity.DisplayName = name
			table.NormalizedIdentity.Name = backend.NormalizeCommonIdentifier(name)
		}
		tables = append(tables, table)
	}
	snapshot.Tables = tables
	if !inScope(defaultSchema) {
		snapshot.ReservedObjects = nil
	}
	for _, selector := range scope {
		bound := &protocol.NamespaceComponent{}
		if selector != defaultSchema {
			bound = &protocol.NamespaceComponent{Source: selector, Normalized: selector}
		}
		snapshot.ScopeBindings = append(snapshot.ScopeBindings, &protocol.InspectionScopeBinding{Selector: selector, Namespaces: []*protocol.ModelNamespace{{
			Database: &protocol.NamespaceComponent{Source: path, Normalized: path}, Catalog: &protocol.NamespaceComponent{}, Schema: bound,
		}}})
	}
	listed := provider.HostFeatures(ctx)
	if !p.carried || !slices.Contains(listed, provider.CommonObjectsFeature) || !slices.Contains(listed, provider.CommonNamespacesFeature) {
		return snapshot, nil
	}
	observed := &protocol.ObservedCommonObjects{Coverage: []*protocol.CommonObjectCoverage{{Kind: protocol.CommonObjectSupport_SCHEMA,
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}
	if p.uninspected {
		observed.Coverage[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the fixture reads no schema"}
	} else {
		for _, schema := range slices.Concat([]string{defaultSchema}, held) {
			if inScope(schema) {
				observed.Namespaces = append(observed.Namespaces, &protocol.ObservedNamespace{Identity: &protocol.Reference{Engine: "fixture", Database: path,
					Kind: provider.NamespaceKind, Name: schema}})
			}
		}
	}
	snapshot.CommonObjects = observed
	return snapshot, nil
}

// PrepareCommonObjects returns the declaration as it is: the program carries
// schemas alone, and spells a schema's name one way.
func (p *namespaces) PrepareCommonObjects(_ context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: proto.CloneOf(request.GetDeclared())}, nil
}

// Render writes a schema change as one statement of the program, and leaves
// every table change to the base fixture with the table under its stored
// name.
func (p *namespaces) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		if common := change.GetCommonObject(); common != nil {
			written, err := schemaStatement(common)
			if err != nil {
				return nil, err
			}
			result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: written}})
			continue
		}
		stored := proto.CloneOf(change)
		stored.Object.Name, stored.Object.Schema = storedTable(change.GetObject().GetSchema(), change.GetObject().GetName()), ""
		rendered, err := p.Provider.Render(ctx, &protocol.RenderRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
			Changes: []*protocol.Change{stored}})
		if err != nil {
			return nil, err
		}
		result.Units = append(result.Units, rendered.GetUnits()...)
	}
	return result, nil
}

func schemaStatement(change *protocol.CommonObjectChange) (string, error) {
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateSchema:
		if operation.CreateSchema.GetComment() != "" {
			return "", fmt.Errorf("a fixture schema has no comment")
		}
		return statementPrefix + "CREATE SCHEMA " + quote(operation.CreateSchema.GetName()) + ";", nil
	case *protocol.CommonObjectChange_DropSchema:
		return statementPrefix + "DROP SCHEMA " + quote(operation.DropSchema.GetName()) + ";", nil
	default:
		return "", fmt.Errorf("the fixture has no %T", operation)
	}
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// readStatement reads a schema statement the program wrote: whether it
// creates, and the schema.
func readStatement(sql string) (create bool, schema string, own bool) {
	text, found := strings.CutPrefix(strings.TrimSpace(sql), statementPrefix)
	if !found {
		return false, "", false
	}
	text = strings.TrimSuffix(text, ";")
	if quoted, created := strings.CutPrefix(text, "CREATE SCHEMA "); created {
		return true, unquote(quoted), true
	}
	if quoted, dropped := strings.CutPrefix(text, "DROP SCHEMA "); dropped {
		return false, unquote(quoted), true
	}
	return false, "", false
}

func unquote(quoted string) string {
	return strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(quoted, `"`), `"`), `""`, `"`)
}

// createdTable is the SQLite table a CREATE TABLE of the base fixture names,
// and reports whether sql is one.
func createdTable(sql string) (string, bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(sql), `CREATE TABLE "`)
	if !found {
		return "", false
	}
	name, _, closed := strings.Cut(rest, `" (`)
	return strings.ReplaceAll(name, `""`, `"`), closed
}

// AnalyzeSQL reads a schema statement as one statement, and leaves every
// other source to the base fixture.
func (p *namespaces) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	result := &protocol.SQLAnalysis{Complete: true}
	for _, source := range request.GetSources() {
		create, _, own := readStatement(source)
		if !own {
			analyzed, err := p.Provider.AnalyzeSQL(ctx, &protocol.AnalyzeSQLRequest{Engine: request.GetEngine(), Sources: []string{source}})
			if err != nil {
				return nil, err
			}
			result.Files = append(result.Files, analyzed.GetFiles()...)
			continue
		}
		words, safety := []string{"FIXTURE", "CREATE", "SCHEMA"}, protocol.SQLStatement_BEHAVIORAL
		if !create {
			words, safety = []string{"FIXTURE", "DROP", "SCHEMA"}, protocol.SQLStatement_DESTRUCTIVE
		}
		start := strings.Index(source, statementPrefix)
		result.Files = append(result.Files, &protocol.SQLFile{Statements: []*protocol.SQLStatement{{
			Start: uint32(start), End: uint32(len(source)), Line: uint32(1 + strings.Count(source[:start], "\n")), // #nosec G115 -- a unit is far below 4 GiB
			Words: words, SourceWords: words, Canonical: strings.Join(words, " "), Safety: safety, Reason: "The fixture changes a schema"}}})
	}
	return result, nil
}

// Execute runs the units in order: a schema statement on the file of
// schemas, any other unit on the base fixture once its table's schema is
// held. A failure stops the batch after the units it applied.
func (p *namespaces) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	var applied uint32
	for _, unit := range request.GetUnits() {
		refusal := ""
		err := p.WithSession(ctx, request.GetSessionId(), request.GetTransactionId(), func(access backend.Access) error {
			held, err := loadSchemas(access.Path)
			if err != nil {
				return err
			}
			if create, schema, own := readStatement(unit.GetSql()); own {
				refusal, err = applySchema(ctx, access, held, create, schema)
				return err
			}
			if table, created := createdTable(unit.GetSql()); created {
				if schema, _ := splitTable(table); schema != defaultSchema && !slices.Contains(held, schema) {
					refusal = "Schema not found: " + schema
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if refusal != "" {
			return &protocol.Execution{Outcome: outcomeAfter(applied), AppliedCount: applied, DiagnosticCode: "fixture.schema",
				Diagnostic: &protocol.ProviderDiagnostic{Reason: refusal}}, nil
		}
		if _, _, own := readStatement(unit.GetSql()); !own {
			single := &protocol.ExecuteRequest{SessionId: request.GetSessionId(), RequestId: request.GetRequestId(), TransactionId: request.GetTransactionId(),
				Units: []*protocol.Unit{unit}, StatementTimeoutNanoseconds: request.GetStatementTimeoutNanoseconds()}
			execution, err := p.Provider.Execute(ctx, single)
			if err != nil || execution.GetOutcome() != protocol.Execution_APPLIED {
				return &protocol.Execution{Outcome: outcomeAfter(applied), AppliedCount: applied, DiagnosticCode: execution.GetDiagnosticCode(),
					Diagnostic: execution.GetDiagnostic()}, err
			}
		}
		applied++
	}
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: applied}, nil
}

// applySchema creates or removes a schema in the file, and returns the
// reason it refuses one: a schema created twice, a removal of a schema it
// does not hold, and one of a schema that still holds a table.
func applySchema(ctx context.Context, access backend.Access, held []string, create bool, schema string) (string, error) {
	switch {
	case schema == defaultSchema:
		return "Schema " + schema + " cannot be created or dropped", nil
	case create && slices.Contains(held, schema):
		return "Schema " + schema + " already exists", nil
	case create:
		return "", saveSchemas(access.Path, append(held, schema))
	case !slices.Contains(held, schema):
		return "Schema not found: " + schema, nil
	}
	var dependents int
	if err := access.Querier.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name LIKE ? ESCAPE '\\'",
		strings.ReplaceAll(strings.ReplaceAll(schema, `\`, `\\`), "_", `\_`)+".%").Scan(&dependents); err != nil {
		return "", err
	}
	if dependents != 0 {
		return "Cannot drop schema " + schema + " while it has dependencies", nil
	}
	return "", saveSchemas(access.Path, slices.DeleteFunc(held, func(name string) bool { return name == schema }))
}

func outcomeAfter(applied uint32) protocol.Execution_Outcome {
	if applied == 0 {
		return protocol.Execution_NOT_STARTED
	}
	return protocol.Execution_PARTIAL
}

// ListDevObjects lists the tables in the request's schemas, and with no
// schema named, every table and every schema the program created.
func (p *namespaces) ListDevObjects(ctx context.Context, request *protocol.ListDevObjectsRequest) (*protocol.DevObjects, error) {
	result := &protocol.DevObjects{Complete: true}
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		tables, schemas, err := devObjects(ctx, access, request.GetSchemas())
		for _, table := range tables {
			result.Objects = append(result.Objects, &protocol.DevObject{Object: &protocol.Reference{Kind: "table", Name: table}})
		}
		for _, schema := range schemas {
			result.Objects = append(result.Objects, &protocol.DevObject{Object: &protocol.Reference{Kind: provider.NamespaceKind, Name: schema}})
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CleanDevDatabase drops the tables of the request's schemas and keeps the
// schemas, and with no schema named drops every table and every schema the
// program created, as Spanner's provider cleans a database.
func (p *namespaces) CleanDevDatabase(ctx context.Context, request *protocol.CleanDevRequest) (*protocol.DevCleanup, error) {
	err := p.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		tables, schemas, err := devObjects(ctx, access, request.GetSchemas())
		if err != nil {
			return err
		}
		for _, table := range tables {
			if _, err := access.Querier.ExecContext(ctx, "DROP TABLE "+quote(table)); err != nil {
				return err
			}
		}
		if len(schemas) == 0 {
			return nil
		}
		return saveSchemas(access.Path, nil)
	})
	if err != nil {
		return nil, err
	}
	return &protocol.DevCleanup{RequestId: request.GetRequestId(), Outcome: protocol.DevCleanup_CLEANED}, nil
}

// devObjects are the SQLite tables of the named schemas, the fixture's own
// storage left out, and with no schema named every table and the schemas the
// file holds.
func devObjects(ctx context.Context, access backend.Access, scope []string) (tables, schemas []string, err error) {
	rows, err := access.Querier.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'ptah_fixture_%' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, nil, err
		}
		if schema, _ := splitTable(name); len(scope) == 0 || slices.Contains(scope, schema) {
			tables = append(tables, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(scope) == 0 {
		schemas, err = loadSchemas(access.Path)
	}
	return tables, schemas, err
}
