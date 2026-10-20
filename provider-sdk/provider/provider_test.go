package provider_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func TestExecutionRequiresConfirmedTerminalOutcome(t *testing.T) {
	tests := []struct {
		name   string
		result *protocol.Execution
		want   error
	}{
		{"applied", &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 2}, nil},
		{"refused", &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED}, provider.ErrExecution},
		{"rolled back", &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED}, provider.ErrExecution},
		{"partial", &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: 1}, provider.ErrExecution},
		{"unknown prefix", &protocol.Execution{Outcome: protocol.Execution_UNKNOWN, AppliedCount: 1}, provider.ErrOutcomeUnknown},
		{"missing reply", nil, provider.ErrOutcomeUnknown},
		{"missing outcome", &protocol.Execution{}, provider.ErrOutcomeUnknown},
		{"still running", &protocol.Execution{Outcome: protocol.Execution_RUNNING}, provider.ErrOutcomeUnknown},
		{"short success", &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 1}, provider.ErrOutcomeUnknown},
		{"false refusal", &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED, AppliedCount: 1}, provider.ErrOutcomeUnknown},
		{"empty partial", &protocol.Execution{Outcome: protocol.Execution_PARTIAL}, provider.ErrOutcomeUnknown},
		{"complete partial", &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: 2}, provider.ErrOutcomeUnknown},
		{"excessive count", &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: 3}, provider.ErrOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.CheckExecution(2, test.result), qt.ErrorIs, test.want)
		})
	}
}

func TestExecutionErrorKeepsOnlyConfirmedPrefix(t *testing.T) {
	c := qt.New(t)
	err := provider.CheckExecution(3, &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: 1, Message: "password=secret"})
	outcome, ok := errors.AsType[*provider.ExecutionError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(outcome.Outcome, qt.Equals, protocol.Execution_PARTIAL)
	c.Assert(outcome.Applied, qt.Equals, uint32(1))
	c.Assert(err.Error(), qt.Equals, "provider execution: PARTIAL (1 units confirmed applied)")
	malformed := provider.CheckExecution(3, &protocol.Execution{Outcome: protocol.Execution_PARTIAL, AppliedCount: 4})
	unknown, ok := errors.AsType[*provider.ExecutionError](malformed)
	c.Assert(ok, qt.IsTrue)
	c.Assert(unknown.Applied, qt.Equals, uint32(0))
	c.Assert(provider.CheckExecution(0, &protocol.Execution{Outcome: protocol.Execution_APPLIED}), qt.ErrorIs, provider.ErrOutcomeUnknown)
}

func TestInspectionCompletenessAndStructuredIdentity(t *testing.T) {
	tests := []struct {
		name   string
		schema *protocol.Schema
		want   error
	}{
		{"absent", nil, provider.ErrIncomplete},
		{"unobserved", &protocol.Schema{}, provider.ErrIncomplete},
		{"observed empty", &protocol.Schema{Complete: true}, nil},
		{"missing facts", &protocol.Schema{Complete: true, Dispositions: []*protocol.Disposition{{State: protocol.Disposition_NOT_INSPECTED, FieldPath: "tables"}}}, provider.ErrIncomplete},
		{"unknown disposition", &protocol.Schema{Complete: true, Dispositions: []*protocol.Disposition{nil}}, provider.ErrIncomplete},
		{"nil table", &protocol.Schema{Complete: true, Tables: []*protocol.Table{nil}}, provider.ErrProtocol},
		{"duplicate identity", &protocol.Schema{Complete: true, Tables: []*protocol.Table{table("a", "b"), table("a", "b")}}, provider.ErrProtocol},
		{"case retained", &protocol.Schema{Complete: true, Tables: []*protocol.Table{table("a", "B"), table("a", "b")}}, nil},
		{"components retained", &protocol.Schema{Complete: true, Tables: []*protocol.Table{table("a.b", "c"), table("a", "b.c")}}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateSchema(test.schema), qt.ErrorIs, test.want)
		})
	}
}

func table(schema, name string) *protocol.Table {
	return &protocol.Table{Identity: &protocol.Reference{Schema: schema, Name: name, Kind: "table"}, Columns: []*protocol.Column{{Name: "id", Type: "INTEGER"}}}
}

func TestRenderRequiresWholeOrderedBatch(t *testing.T) {
	request := &protocol.RenderRequest{Changes: []*protocol.Change{{Id: "a"}, {Id: "b"}}}
	tests := []struct {
		name   string
		result *protocol.RenderResult
		want   error
	}{
		{"complete", &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{sql("a"), sql("b")}}, nil},
		{"partial", &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{sql("a")}}, provider.ErrProtocol},
		{"unconfirmed", &protocol.RenderResult{Units: []*protocol.Unit{sql("a"), sql("b")}}, provider.ErrProtocol},
		{"reordered", &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{sql("b"), sql("a")}}, provider.ErrProtocol},
		{"missing unit", &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{sql("a"), nil}}, provider.ErrProtocol},
		{"missing body", &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{sql("a"), {ChangeId: "b"}}}, provider.ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateRender(request, test.result), qt.ErrorIs, test.want)
		})
	}
}

func sql(id string) *protocol.Unit {
	return &protocol.Unit{ChangeId: id, Body: &protocol.Unit_Sql{Sql: "CREATE TABLE example (id INTEGER);"}}
}

func TestNativeUnitRequiresVersionedEnvelope(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateUnit(&protocol.Unit{ChangeId: "a", Body: (*protocol.Unit_Native)(nil)}), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateUnit(&protocol.Unit{ChangeId: "a", Body: (*protocol.Unit_Sql)(nil)}), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateUnit(&protocol.Unit{ChangeId: "a", Body: &protocol.Unit_Native{Native: &protocol.Envelope{ProviderId: "example.org/provider", Kind: "native", Encoding: "protobuf"}}}), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateUnit(&protocol.Unit{ChangeId: "a", Body: &protocol.Unit_Native{Native: &protocol.Envelope{ProviderId: "example.org/provider", Kind: "native", Encoding: "protobuf", SchemaVersion: 1}}}), qt.IsNil)
}

func TestDescriptionNegotiatesExactRevisionAndRequiredFeatures(t *testing.T) {
	missingRules := description()
	missingRules.Engines[0].Identifiers = nil
	previousRevision := description()
	previousRevision.Revision--
	tests := []struct {
		name        string
		description *protocol.Description
		required    []string
		want        error
	}{
		{"available", description(), []string{provider.InspectFeature}, nil},
		{"missing identifier rules", missingRules, nil, provider.ErrProtocol},
		{"previous revision", previousRevision, nil, provider.ErrProtocol},
		{"required service missing", description(), []string{provider.ExecuteFeature}, provider.ErrProtocol},
		{"missing identity", &protocol.Description{Revision: provider.Revision}, nil, provider.ErrProtocol},
		{"different revision", &protocol.Description{Revision: provider.Revision + 1}, nil, provider.ErrProtocol},
		{"missing reply", nil, nil, provider.ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDescription(test.description, test.required), qt.ErrorIs, test.want)
		})
	}
	c := qt.New(t)
	conflicted := description()
	conflicted.Engines = append(conflicted.Engines, &protocol.Engine{Id: "other", Aliases: []string{"fixture"}})
	c.Assert(provider.ValidateDescription(conflicted, nil), qt.ErrorIs, provider.ErrProtocol)
}

func description() *protocol.Description {
	return &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/fixture", Version: "1", SemanticIdentity: "table-1", Engines: []*protocol.Engine{{Id: "fixture", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.InspectFeature}}
}

func TestIdentifierRulesRequireExplicitSupportedSemantics(t *testing.T) {
	tests := []struct {
		name  string
		rules *protocol.IdentifierRules
		want  error
	}{
		{"missing", nil, provider.ErrProtocol},
		{"exact", rules(protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_TABLE), nil},
		{"ascii", rules(protocol.IdentifierRules_ASCII_CASE_INSENSITIVE, protocol.IdentifierRules_ASCII_CASE_INSENSITIVE, protocol.IdentifierRules_ASCII_CASE_INSENSITIVE, protocol.IdentifierRules_SCHEMA), nil},
		{"missing table comparison", rules(0, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_TABLE), provider.ErrProtocol},
		{"missing column comparison", rules(protocol.IdentifierRules_EXACT, 0, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_TABLE), provider.ErrProtocol},
		{"unknown index comparison", rules(protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, 99, protocol.IdentifierRules_TABLE), provider.ErrProtocol},
		{"missing namespace", rules(protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, 0), provider.ErrProtocol},
		{"unknown namespace", rules(protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, protocol.IdentifierRules_EXACT, 99), provider.ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateIdentifierRules(test.rules), qt.ErrorIs, test.want)
		})
	}
}

func rules(table, column, index protocol.IdentifierRules_Comparison, namespace protocol.IdentifierRules_IndexNamespace) *protocol.IdentifierRules {
	return &protocol.IdentifierRules{TableNames: table, ColumnNames: column, IndexNames: index, IndexNamespace: namespace}
}
