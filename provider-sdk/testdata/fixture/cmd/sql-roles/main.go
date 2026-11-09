// Package main serves the fixture engine in the DOCUMENT mode with the role
// passwords of contract 25.33: its SQL documents may create a user with
// `CREATE USER name PASSWORD 'text';`, and the reading returns the password
// to a host that lists source.sql-role-passwords. The engine carries roles on
// the common-object wire and takes a declared password as an execution input
// (schema.password-inputs), so a unit that creates the user names the input
// and never the value. The executor runs the base fixture's SQLite, which
// refuses such a unit, so a refused apply shows what an error prints. It is
// transport evidence, not an engine grammar.
package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, fmt.Appendf(nil, "%d", os.Getpid()), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &roles{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description(), Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SQLSchemaReader: owner, ExecutionInputs: owner,
		CommonObjects: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// description is the shared DOCUMENT description with roles on the
// common-object wire, password inputs and the role passwords of a reading.
func description() *protocol.Description {
	description := backend.SQLDocumentDescription()
	description.SemanticIdentity += "-roles"
	added := []string{provider.LocksFeature, provider.CommonObjectsFeature, provider.SensitiveInputsFeature, provider.PasswordInputsFeature,
		provider.SQLRolePasswordsFeature}
	description.Features = append(description.Features, added...)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, added...)
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_ROLE}}
	return description
}

type roles struct{ *backend.Provider }

// userStatement is the statement of this command's dialect that creates a
// user, with or without a password. The shared fixture dialect reads the rest
// of a document; it is built against the oldest SDK, so roles live here.
var userStatement = regexp.MustCompile(`(?i)\bCREATE\s+USER\s+(\w+)(?:\s+PASSWORD\s+'([^']*)')?\s*;`)

// declaredUser is one user statement and where it stands.
type declaredUser struct {
	document         *protocol.SQLDocument
	start, end       int
	name, password   string
	declaresPassword bool
}

// ReadSQLSchema reads the user statements of each document itself and the
// rest through the shared dialect, with the statements blanked so every other
// offset stays where it was. A user that declares a password carries it to a
// host that lists source.sql-role-passwords, and only says it declares one to
// any other.
func (p *roles) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	stripped := proto.CloneOf(request)
	var users []declaredUser
	for i, document := range request.GetDocuments() {
		text := []byte(document.GetText())
		for _, match := range userStatement.FindAllStringSubmatchIndex(document.GetText(), -1) {
			user := declaredUser{document: document, start: match[0], end: match[1], name: document.GetText()[match[2]:match[3]]}
			if match[4] >= 0 {
				user.password, user.declaresPassword = document.GetText()[match[4]:match[5]], true
			}
			users = append(users, user)
			for j := match[0]; j < match[1]; j++ {
				if text[j] != '\n' {
					text[j] = ' '
				}
			}
		}
		stripped.Documents[i].Text = string(text)
	}
	result, err := p.Provider.ReadSQLSchema(ctx, stripped)
	if err != nil || !result.GetComplete() {
		return result, err
	}
	carries := slices.Contains(request.GetTargetFeatures(), provider.SQLRolePasswordsFeature)
	for _, user := range users {
		role := &protocol.SQLRole{Name: user.name, Login: true, PasswordDeclared: user.declaresPassword}
		if carries {
			role.Password = user.password
		}
		result.Traces = append(result.Traces, &protocol.SQLObjectTrace{Collection: protocol.SQLObjectTrace_ROLES, Index: uint32(len(result.Roles)), // #nosec G115 -- the index counts roles of one fixture reading
			Source: &protocol.SourceTrace{DocumentId: user.document.GetId(), Span: userSpan(user)}})
		result.Roles = append(result.Roles, role)
	}
	return result, nil
}

// userSpan is the statement's span, with one-based lines and byte columns.
func userSpan(user declaredUser) *protocol.SourceSpan {
	text := user.document.GetText()
	position := func(offset int) *protocol.SourcePosition {
		// #nosec G115 -- offsets index a fixture document
		return &protocol.SourcePosition{ByteOffset: uint64(offset), Line: uint32(1 + strings.Count(text[:offset], "\n")),
			Column: uint32(offset-strings.LastIndex(text[:offset], "\n")-1) + 1}
	}
	return &protocol.SourceSpan{Start: position(user.start), End: position(user.end)}
}

// Open adds the capability the comparison reads to plan a role.
func (p *roles) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if session.Capabilities == nil {
		session.Capabilities = make(map[string]bool)
	}
	session.Capabilities["role_management"] = true
	return session, nil
}

// Inspect reports the tables as the base fixture does and, to a host that
// lists the common-object wire, no role, read completely: SQLite has none.
func (p *roles) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil || !slices.Contains(provider.HostFeatures(ctx), provider.CommonObjectsFeature) {
		return snapshot, err
	}
	snapshot.CommonObjects = &protocol.ObservedCommonObjects{Coverage: []*protocol.CommonObjectCoverage{
		{Kind: protocol.CommonObjectSupport_ROLE, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
	}}
	return snapshot, nil
}

// PrepareCommonObjects spells a declaration as the program reads it back: a
// role that inherits nothing, which is all a fixture role does.
func (p *roles) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prepared := proto.CloneOf(request.GetDeclared())
	for _, role := range prepared.GetRoles() {
		role.Inherit = false
	}
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: prepared}, nil
}

// Render writes a user's creation as one statement that names the execution
// input of its password, and leaves every other change to the base fixture.
func (p *roles) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		creation := change.GetCommonObject().GetCreateRole()
		if creation == nil {
			rendered, err := p.Provider.Render(ctx, &protocol.RenderRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
				Capabilities: request.GetCapabilities(), Form: request.GetForm(), Changes: []*protocol.Change{change}})
			if err != nil {
				return nil, err
			}
			result.Units = append(result.Units, rendered.GetUnits()...)
			continue
		}
		statement := "CREATE USER " + creation.GetRole().GetName()
		if input := creation.GetPasswordInput(); input != nil {
			statement += " PASSWORD $" + input.GetName()
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: statement + ";"}})
	}
	return result, nil
}

// AnalyzeSQL reads a statement that creates a user as one behavioral
// statement and leaves every other source to the base fixture.
func (p *roles) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	result := &protocol.SQLAnalysis{Complete: true}
	for _, source := range request.GetSources() {
		start := strings.Index(source, "CREATE USER ")
		if start < 0 {
			analyzed, err := p.Provider.AnalyzeSQL(ctx, &protocol.AnalyzeSQLRequest{Engine: request.GetEngine(), Sources: []string{source}})
			if err != nil {
				return nil, err
			}
			result.Files = append(result.Files, analyzed.GetFiles()...)
			continue
		}
		end := strings.LastIndex(source, ";") + 1
		words := []string{"CREATE", "USER"}
		result.Files = append(result.Files, &protocol.SQLFile{Statements: []*protocol.SQLStatement{{
			Start: uint32(start), End: uint32(end), Line: uint32(1 + strings.Count(source[:start], "\n")), // #nosec G115 -- a unit is far below 4 GiB
			Words: words, SourceWords: words, Canonical: "CREATE USER", Safety: protocol.SQLStatement_BEHAVIORAL, Reason: "The fixture creates a user"}}})
	}
	return result, nil
}

// inputReference is how a unit names an execution input.
var inputReference = regexp.MustCompile(`\$(PTAH_SECRET_[A-Z0-9_]+)`)

// DescribeExecutionInputs reports the input each unit names, once.
func (p *roles) DescribeExecutionInputs(ctx context.Context, request *protocol.DescribeExecutionInputsRequest) (*protocol.ExecutionInputs, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.ExecutionInputs{Complete: true}
	var names []string
	for _, unit := range request.GetUnits() {
		for _, match := range inputReference.FindAllStringSubmatch(unit.GetSql(), -1) {
			if !slices.Contains(names, match[1]) {
				names = append(names, match[1])
			}
		}
	}
	slices.Sort(names)
	for _, name := range names {
		result.Inputs = append(result.Inputs, &protocol.ExecutionInput{Source: protocol.ExecutionInput_ENVIRONMENT, Name: name})
	}
	return result, nil
}
