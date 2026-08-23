// Package agentapi is the versioned, read-only operation contract that both AI
// surfaces consume: the MCP server for external clients, and Ptah Assist.
//
// It exists so neither surface reimplements schema semantics. Every operation
// here names the package that already owns the work and adds none of its own,
// which is the invariant stokaro/ptah#1483 states and the thing this package
// makes checkable: an operation that cannot name an owner is new behavior and
// does not belong in an adapter.
//
// # What is here, and what is deliberately not
//
// This is the read-only MVP frozen by ADR 0002. Every operation reads and
// writes nothing -- not the database, not the migration directory, not a file.
//
// The three reading verbs Ptah has that need a scratch database -- schema
// inspect, schema diff and migrations lint -- are absent on purpose. Each takes
// a --dev-url the CLI resets destructively, so exposing them would put a
// destructive capability behind a read-only name on the surface most likely to
// be driven without a person reading the flag documentation. They return when a
// later phase can supply that database out of band, never from the caller.
//
// # Names
//
// Operations are named for what they answer rather than for the CLI verb that
// answers it today. A verb name carries flag history and CLI-shaped defaults an
// API should not inherit, and tying the two would make every CLI rename a
// breaking change here.
package agentapi

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/schemalineage"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/schemavalidate"
)

// Version is the contract version. A caller reads it to know what it is talking
// to; a change to the shape of any request or response below changes it, and so
// does adding an operation.
//
// 2026-08-23 added the artifact operations: describe_workspace, read_artifact,
// preview_patch and apply_patch. They live on a [Session] rather than beside the
// four read operations below, because an artifact operation is meaningless
// without knowing which directory it means and that must not come from the
// caller.
const Version = "2026-08-23"

// SchemaSource names where a declared schema is read from.
//
// It is one type rather than a parameter on each operation, because every
// operation that reads a declared schema reads it the same way, and a caller
// that learned one has learned all of them.
type SchemaSource struct {
	// RootDirs are directories scanned for Go annotations.
	RootDirs []string `json:"root_dirs,omitempty" jsonschema:"directories to scan for Ptah Go annotations"`
	// SchemaFiles are HCL, YAML or SQL schema files.
	SchemaFiles []string `json:"schema_files,omitempty" jsonschema:"schema files describing the desired schema"`
}

// empty reports whether a source names nothing, which is a caller error rather
// than an empty schema.
func (s SchemaSource) empty() bool {
	return len(s.RootDirs) == 0 && len(s.SchemaFiles) == 0
}

func (s SchemaSource) load(ctx context.Context, dialect string) (*goschema.Database, error) {
	if s.empty() {
		return nil, fmt.Errorf("no schema source: name at least one root_dirs entry or schema_files entry")
	}
	return schemaload.LoadContext(ctx, schemaload.Options{
		RootDirs:    s.RootDirs,
		SchemaFiles: s.SchemaFiles,
		Dialect:     dialect,
	})
}

// normalizedDialect resolves a requested dialect, refusing an unknown one by
// name.
//
// The refusal matters more here than on the command line: a caller that cannot
// see a flag's documentation will guess, and a guess silently treated as
// PostgreSQL produces an answer about a database the caller did not ask about.
func normalizedDialect(requested string) (string, error) {
	if requested == "" {
		return "", fmt.Errorf("dialect is required: a declaration valid for one target can be invalid for another")
	}
	dialect := platform.NormalizeDialect(requested)
	if dialect == "" {
		return "", fmt.Errorf("unknown dialect %q", requested)
	}
	return dialect, nil
}

// ValidateSchemaRequest asks whether a declared schema is structurally sound
// for one target.
type ValidateSchemaRequest struct {
	Source  SchemaSource `json:"source"`
	Dialect string       `json:"dialect" jsonschema:"the target dialect, such as postgres or mysql"`
}

// Problem is one structural fault.
type Problem struct {
	Dialect string `json:"dialect"`
	Kind    string `json:"kind"`
	Object  string `json:"object,omitempty"`
	Message string `json:"message"`
}

// ValidateSchemaResponse is what validation found.
type ValidateSchemaResponse struct {
	Dialect  string    `json:"dialect"`
	Problems []Problem `json:"problems"`
	// Valid is stated rather than left to be inferred from an empty list,
	// because a caller that mishandles the list should not read silence as
	// success.
	Valid bool `json:"valid"`
}

// ValidateSchema reports structural problems without a database.
//
// Owner: internal/schemavalidate.
func ValidateSchema(ctx context.Context, req ValidateSchemaRequest) (*ValidateSchemaResponse, error) {
	dialect, err := normalizedDialect(req.Dialect)
	if err != nil {
		return nil, err
	}
	database, err := req.Source.load(ctx, dialect)
	if err != nil {
		// A source that will not load is a validation result rather than a
		// transport failure: the caller asked whether the schema is sound, and
		// "it does not parse for this target" answers that.
		return &ValidateSchemaResponse{
			Dialect: dialect,
			Problems: []Problem{{
				Dialect: dialect,
				Kind:    "source",
				Message: err.Error(),
			}},
		}, nil
	}
	found := schemavalidate.Collect(database, dialect)
	response := &ValidateSchemaResponse{Dialect: dialect, Valid: len(found) == 0}
	for _, problem := range found {
		response.Problems = append(response.Problems, Problem{
			Dialect: problem.Dialect,
			Kind:    problem.Kind,
			Object:  problem.Object,
			Message: problem.Message,
		})
	}
	return response, nil
}

// RenderSchemaRequest asks what DDL a declared schema becomes.
type RenderSchemaRequest struct {
	Source  SchemaSource `json:"source"`
	Dialect string       `json:"dialect" jsonschema:"the target dialect the DDL is rendered for"`
}

// RenderSchemaResponse carries the statements in the order they must run.
type RenderSchemaResponse struct {
	Dialect    string   `json:"dialect"`
	Statements []string `json:"statements"`
}

// RenderSchema returns the DDL a declared schema renders to, in dependency
// order.
//
// Owner: core/renderer.
func RenderSchema(ctx context.Context, req RenderSchemaRequest) (*RenderSchemaResponse, error) {
	dialect, err := normalizedDialect(req.Dialect)
	if err != nil {
		return nil, err
	}
	database, err := req.Source.load(ctx, dialect)
	if err != nil {
		return nil, err
	}
	statements, err := renderer.GetOrderedCreateStatements(database, dialect)
	if err != nil {
		return nil, fmt.Errorf("render %s: %w", dialect, err)
	}
	return &RenderSchemaResponse{Dialect: dialect, Statements: statements}, nil
}

// SchemaLineageRequest asks where each view column's value comes from.
type SchemaLineageRequest struct {
	Source  SchemaSource `json:"source"`
	Dialect string       `json:"dialect" jsonschema:"the target dialect the schema is read for"`
}

// LineageEdge is one column-to-column dependency.
type LineageEdge struct {
	FromTable  string `json:"from_table"`
	FromColumn string `json:"from_column"`
	ToView     string `json:"to_view"`
	ToColumn   string `json:"to_column"`
}

// LineageUndecided names a view whose column sources could not be established.
//
// It carries the view rather than a column, because the owning derivation
// reports at view granularity: a body it cannot model yields no columns to name.
type LineageUndecided struct {
	View   string `json:"view"`
	Reason string `json:"reason"`
}

// SchemaLineageResponse carries both halves.
//
// Undecided is not omitted when empty in the caller's reading of it: a lineage
// answer that reported only what resolved would let a caller conclude a column
// depends on nothing when the truth is that nothing looked.
type SchemaLineageResponse struct {
	Edges     []LineageEdge      `json:"edges"`
	Undecided []LineageUndecided `json:"undecided"`
}

// SchemaLineage traces which base columns feed each view column.
//
// Owner: internal/schemalineage.
func SchemaLineage(ctx context.Context, req SchemaLineageRequest) (*SchemaLineageResponse, error) {
	dialect, err := normalizedDialect(req.Dialect)
	if err != nil {
		return nil, err
	}
	database, err := req.Source.load(ctx, dialect)
	if err != nil {
		return nil, err
	}
	derived := schemalineage.Derive(database)
	// Both lists start non-nil so the encoded answer carries [] rather than
	// null. A caller reading null as "no lineage" and [] as "no lineage" would
	// be right by accident; one reading null as an absent field would not.
	response := &SchemaLineageResponse{
		Edges:     make([]LineageEdge, 0, len(derived.Edges)),
		Undecided: make([]LineageUndecided, 0, len(derived.Undecided)),
	}
	for _, edge := range derived.Edges {
		response.Edges = append(response.Edges, LineageEdge{
			FromTable: edge.FromTable, FromColumn: edge.FromColumn,
			ToView: edge.ToView, ToColumn: edge.ToColumn,
		})
	}
	for _, undecided := range derived.Undecided {
		response.Undecided = append(response.Undecided, LineageUndecided{
			View: undecided.View, Reason: undecided.Reason,
		})
	}
	return response, nil
}

// ReadDatabaseRequest asks what a live database holds.
type ReadDatabaseRequest struct {
	DatabaseURL string   `json:"database_url" jsonschema:"connection URL of the database to read"`
	Schemas     []string `json:"schemas,omitempty" jsonschema:"schemas to read; empty reads the connection default"`
}

// DatabaseObject is one object the read returned.
type DatabaseObject struct {
	Kind    string `json:"kind"`
	Schema  string `json:"schema,omitempty"`
	Name    string `json:"name"`
	Columns int    `json:"columns,omitempty"`
}

// ReadDatabaseResponse summarizes the live schema.
type ReadDatabaseResponse struct {
	Dialect string           `json:"dialect"`
	Version string           `json:"version,omitempty"`
	Objects []DatabaseObject `json:"objects"`
}

// ReadDatabase reads a live database's schema.
//
// Owner: dbschema. It opens a connection and closes it; it runs no DDL.
func ReadDatabase(ctx context.Context, req ReadDatabaseRequest) (*ReadDatabaseResponse, error) {
	if req.DatabaseURL == "" {
		return nil, fmt.Errorf("database_url is required")
	}
	conn, err := dbschema.ConnectToDatabase(ctx, req.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemas(conn, req.Schemas)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	info := conn.Info()
	response := &ReadDatabaseResponse{
		Dialect: info.Dialect,
		Version: info.Version,
		Objects: make([]DatabaseObject, 0, len(live.Tables)+len(live.Views)),
	}
	for _, table := range live.Tables {
		response.Objects = append(response.Objects, DatabaseObject{
			Kind: "table", Schema: table.Schema, Name: table.Name,
			Columns: len(table.Columns),
		})
	}
	for _, view := range live.Views {
		response.Objects = append(response.Objects, DatabaseObject{
			Kind: "view", Schema: view.Schema, Name: view.Name,
		})
	}
	return response, nil
}
