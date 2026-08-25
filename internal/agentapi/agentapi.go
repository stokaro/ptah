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

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/agentdiag"
	"go.5x5.cz/ptah/internal/agenttarget"
	"go.5x5.cz/ptah/internal/docsembed"
	"go.5x5.cz/ptah/internal/schemalineage"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/schemavalidate"
)

// Version is the contract version. A caller reads it to know what it is talking
// to; a change to the shape of any request or response below changes it, and so
// does adding an operation.
//
// 2026-08-23 added the artifact operations: describe_session, read_artifact,
// preview_patch and apply_patch. They live on a [Session] rather than beside the
// four read operations below, because an artifact operation is meaningless
// without knowing which directory it means and that must not come from the
// caller.
//
// 2026-08-24 made the policy the boundary it was reported to be. Every operation
// now runs on a [Session] and asks the capability broker first; the four read
// operations had been callable beside it, which made every verdict published
// about them decorative -- database.inspect resolved to deny and read_database
// connected anyway.
//
// Three externally visible consequences. read_database no longer takes a
// connection URL: it names one of the databases the operator configured, so a
// caller cannot choose the resource its own authorization is decided about, and
// cannot hand Ptah a credential. describe_workspace became describe_session: it
// works without a workspace and reports what policy permits separately from
// what this process can reach. A declared schema is read only from directories
// the operator configured, and a source that would be fetched rather than
// opened is refused -- schema loading was otherwise a route around
// network.arbitrary, which no layer may grant.
// 2026-08-24 also gave every failure a code from
// [go.5x5.cz/ptah/internal/agentdiag], and made apply_patch answer with both an
// error and a response when verification undid the patch. A client that read
// only the message text still works; one that branches on the code no longer
// has to parse prose.
// 2026-08-24 also added search_docs, which answers a question about Ptah from
// the documentation the binary carries, and a `documentation` section on
// describe_session reporting how much of it is loaded. Both are additions: a
// client that does not call the tool and ignores the new section is unaffected,
// which is why the date does not move.
const Version = "2026-08-24"

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
		return nil, agentdiag.Errorf(agentdiag.CodeInvalidRequest,
			"no schema source: name at least one root_dirs entry or schema_files entry")
	}
	database, err := schemaload.LoadContext(ctx, schemaload.Options{
		RootDirs:    s.RootDirs,
		SchemaFiles: s.SchemaFiles,
		Dialect:     dialect,
	})
	// The loader's failures are coded here rather than at each call site,
	// because every operation reads a source the same way and a code assigned
	// per caller is a code that eventually differs per caller.
	return database, agentdiag.Wrap(agentdiag.CodeSchemaSourceUnreadable, err)
}

// normalizedDialect resolves a requested dialect, refusing an unknown one by
// name.
//
// The refusal matters more here than on the command line: a caller that cannot
// see a flag's documentation will guess, and a guess silently treated as
// PostgreSQL produces an answer about a database the caller did not ask about.
func normalizedDialect(requested string) (string, error) {
	if requested == "" {
		return "", agentdiag.Errorf(agentdiag.CodeInvalidRequest,
			"dialect is required: a declaration valid for one target can be invalid for another")
	}
	dialect := platform.NormalizeDialect(requested)
	if dialect == "" {
		return "", agentdiag.Errorf(agentdiag.CodeInvalidRequest, "unknown dialect %q", requested)
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
func validateSchema(ctx context.Context, req ValidateSchemaRequest) (*ValidateSchemaResponse, error) {
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
func renderSchema(ctx context.Context, req RenderSchemaRequest) (*RenderSchemaResponse, error) {
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
		return nil, agentdiag.Errorf(agentdiag.CodeRenderFailed, "render %s: %w", dialect, err)
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
func schemaLineage(ctx context.Context, req SchemaLineageRequest) (*SchemaLineageResponse, error) {
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

// SearchDocsRequest asks a question about Ptah itself.
//
// It reads Ptah's own documentation, carried in the binary. Nothing about the
// operator's project, database or filesystem is involved, and no network is
// reached (stokaro/ptah#2123).
type SearchDocsRequest struct {
	// Query is the question, in the words it would be asked in. It is matched
	// against the documentation's own words, so naming a flag or a command
	// finds more than describing one.
	Query string `json:"query" jsonschema:"a question about Ptah's behavior, flags, commands or concepts"`
	// Limit caps how many passages come back. Zero takes the default.
	Limit int `json:"limit,omitempty" jsonschema:"maximum passages to return; omit for the default"`
}

// DocPassage is one answer: what the documentation says, and where it says it.
//
// The text and its location together, because they answer to different
// readers. A model needs the passage; a person checking the model needs the
// file and the section, and a passage with no provenance is a claim the
// documentation cannot be held to.
type DocPassage struct {
	// Path is the document, relative to the repository root.
	Path string `json:"path"`
	// Heading is the trail of headings the passage sits under, joined with
	// " > ". It is empty for text above the first heading.
	Heading string `json:"heading"`
	// Text is the passage.
	Text string `json:"text"`
}

// SearchDocsResponse carries the passages that answer the question.
type SearchDocsResponse struct {
	// Passages are the answers, best first. An empty list means the
	// documentation does not answer the question -- which is a result, not a
	// failure, and is the answer this operation is built to be able to give.
	Passages []DocPassage `json:"passages"`
	// Documents is how many documents were searched, so a caller can tell an
	// unanswered question from an empty index.
	Documents int `json:"documents"`
	// Documentation names which documentation answered. An answer from
	// documentation that does not describe the binary being driven is worse
	// than no answer, because it is indistinguishable from a right one -- so
	// every answer says which build it came from, and describe_session reports
	// the same one.
	Documentation docsembed.Documentation `json:"documentation"`
}

// defaultDocsLimit is how many passages come back when a caller names no limit.
//
// Five rather than one: a question about a flag is often answered by the
// reference entry and the guide that explains when to reach for it, and a
// single passage would make the caller ask again to find out.
const defaultDocsLimit = 5

// searchDocs answers a question from Ptah's own documentation.
//
// Owner: internal/docsembed.
func searchDocs(_ context.Context, req SearchDocsRequest) (*SearchDocsResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = defaultDocsLimit
	}
	index := docsembed.Index()
	response := &SearchDocsResponse{
		Passages:      make([]DocPassage, 0, limit),
		Documents:     index.DocumentCount(),
		Documentation: docsembed.Version(),
	}
	for _, result := range index.Search(req.Query, limit) {
		response.Passages = append(response.Passages, DocPassage{
			Path:    result.Path,
			Heading: result.Heading,
			Text:    result.Text,
		})
	}
	return response, nil
}

// ReadDatabaseRequest asks what a live database holds.
type ReadDatabaseRequest struct {
	// Target names one of the databases the operator configured. Empty selects
	// the only one when a process has exactly one.
	//
	// A name, not a URL. A caller that could supply the connection string would
	// be choosing the resource its own authorization is decided about, and
	// would be handing Ptah a credential besides. What may be named here is
	// what the operator already named.
	Target string `json:"target,omitempty" jsonschema:"name of a configured database target; omit when the process has exactly one"`
	// Schemas narrows the read. It carries no authority: it selects among
	// what the authorized connection can already see.
	Schemas []string `json:"schemas,omitempty" jsonschema:"schemas to read; empty reads the connection default"`
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
func readDatabase(
	ctx context.Context,
	target *agenttarget.Target,
	req ReadDatabaseRequest,
) (*ReadDatabaseResponse, error) {
	conn, err := dbschema.ConnectToDatabase(ctx, target.URL())
	if err != nil {
		return nil, agentdiag.Errorf(agentdiag.CodeDatabaseUnreachable, "connect: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemas(conn, req.Schemas)
	if err != nil {
		return nil, agentdiag.Errorf(agentdiag.CodeDatabaseReadFailed, "read schema: %w", err)
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
