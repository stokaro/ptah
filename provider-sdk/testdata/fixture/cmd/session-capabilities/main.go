// Package main serves the fixture engine as an engine whose provider states
// no capabilities to plan it with offline (it lists no
// engine.default-capabilities), while each session states the server's:
// transactional_ddl false. Its creation projection refuses a request that
// establishes no answer for transactional_ddl, as the YDB provider refuses a
// Serial column against unknown facts, and appends the answer it projected
// against to the file FIXTURE_PROJECTION_LOG names, so a command that
// connects and then converts its source shows which facts the host converted
// it with. FIXTURE_SESSION_SILENT is the control: the session states nothing,
// and the projection is refused.
//
// The engine owns its table identity, as the projection requires, and plans
// no change, so a command fails after the conversion; the projection is the
// evidence.
//
// The engine is fixture-session-facts, or the control's
// fixture-session-silent: the host keeps one set of facts per engine for the
// life of a process, so the two cases cannot share an engine within one test
// process.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

func main() {
	engine, silent := "fixture-session-facts", os.Getenv("FIXTURE_SESSION_SILENT") != ""
	if silent {
		engine = "fixture-session-silent"
	}
	owner := &sessionFacts{Provider: &backend.Provider{Engine: engine}, silent: silent}
	description := backend.Description()
	description.SemanticIdentity = "fixture-session-capabilities-1"
	// The schema lock is what schema apply asks for before it writes. An
	// engine that projects its tables owns their identity, so it prepares
	// and validates them too.
	features := []string{provider.ProjectTableCreationsFeature, provider.PrepareTablesFeature, provider.ValidateSchemaFeature, provider.LocksFeature}
	description.Features = append(description.Features, features...)
	served := description.Engines[0]
	served.Id = engine
	served.Features = slices.DeleteFunc(served.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	served.Features = append(served.Features, features...)
	err := runtime.Serve(runtime.Server{Description: description, Locks: owner, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, TableCreations: owner, TablePreparer: owner, SchemaValidator: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type sessionFacts struct {
	*backend.Provider
	silent bool
}

// Open opens a fixture session that states transactional_ddl false, or no
// capability at all for the control.
func (p *sessionFacts) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	opened, err := p.Provider.Open(ctx, request)
	if err != nil || p.silent {
		return opened, err
	}
	opened.Capabilities = map[string]bool{"transactional_ddl": false}
	return opened, nil
}

// ProjectTableCreations refuses a request that establishes no answer for
// transactional_ddl. Otherwise it records the answer and completes each table
// with the key its declaration names and no facet.
func (*sessionFacts) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	transactional, established := request.GetCapabilities()["transactional_ddl"]
	if !established {
		return nil, status.Error(codes.FailedPrecondition, "the fixture projects a table only against an established transactional_ddl")
	}
	if path := os.Getenv("FIXTURE_PROJECTION_LOG"); path != "" {
		if err := appendLine(path, fmt.Sprintf("transactional_ddl=%t", transactional)); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	result := &protocol.ProjectedTableCreations{Complete: true}
	for _, input := range request.GetTables() {
		result.Tables = append(result.Tables, &protocol.ProjectedTableCreation{Subject: proto.CloneOf(input.GetSubject()),
			ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: slices.Clone(input.GetDeclaration().GetPrimaryKey())})
	}
	return result, nil
}

// ValidateSchema accepts every declaration.
func (*sessionFacts) ValidateSchema(ctx context.Context, _ *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &protocol.SchemaValidation{Complete: true}, nil
}

// PrepareTables completes each table with the key it declares.
func (*sessionFacts) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	result := &protocol.PreparedTables{Complete: true}
	for _, input := range request.GetTables() {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		result.Tables = append(result.Tables, &protocol.PreparedTable{Subject: proto.CloneOf(input.GetSubject()), ColumnPrimaryKeysPrepared: true,
			ColumnPrimaryKeys: slices.Clone(input.GetDesired().GetPrimaryKey())})
	}
	return result, nil
}

// appendLine appends line to the file at path, which the test that selects
// the fixture names in its private directory.
func appendLine(path, line string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) // #nosec G304 G703 -- the host test names this log file in its private directory
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(file, line)
	return errors.Join(err, file.Close())
}
