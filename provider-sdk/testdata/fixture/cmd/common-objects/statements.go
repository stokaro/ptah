package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// statementPrefix opens every statement the program's executor reads.
const statementPrefix = "FIXTURE "

// statement is one change to a common object, as the program writes it in
// a unit: `FIXTURE {...};`.
type statement struct {
	Op         string   `json:"op"`
	Kind       string   `json:"kind,omitempty"`
	Table      string   `json:"table,omitempty"`
	Name       string   `json:"name"`
	Body       string   `json:"body,omitempty"`
	Text       string   `json:"text,omitempty"`
	Login      bool     `json:"login,omitempty"`
	Member     string   `json:"member,omitempty"`
	Privileges []string `json:"privileges,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	WithOption bool     `json:"with_option,omitempty"`
}

// destructive are the statements that take state away, which the analyzer
// says so of.
var destructive = []string{"drop_view", "drop_role", "revoke", "remove_member"}

// Render writes each common object change as one statement and leaves every
// other change to the base fixture. The units are text in every form.
func (p *commonObjects) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		common := change.GetCommonObject()
		if common == nil {
			rendered, err := p.Provider.Render(ctx, &protocol.RenderRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Changes: []*protocol.Change{change}})
			if err != nil {
				return nil, err
			}
			result.Units = append(result.Units, rendered.GetUnits()...)
			continue
		}
		written, err := writeStatement(common)
		if err != nil {
			return nil, err
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: written}})
	}
	return result, nil
}

// writeStatement spells one change. The program keeps login alone of a
// role's attributes, so it refuses a role that declares another, or a
// password, and the change of another. An engine with common table identity
// has no schema.validate of its own: the host validates a declaration by
// rendering it, so this refusal is the provider's judgment of the role.
func writeStatement(change *protocol.CommonObjectChange) (string, error) {
	var written statement
	switch operation := change.GetOperation().(type) {
	case *protocol.CommonObjectChange_CreateView:
		view := operation.CreateView.GetView()
		written = statement{Op: "create_view", Name: view.GetName(), Body: view.GetBody(), Text: view.GetComment()}
		if operation.CreateView.GetReplace() {
			written.Op = "replace_view"
		}
	case *protocol.CommonObjectChange_DropView:
		written = statement{Op: "drop_view", Name: operation.DropView.GetName()}
	case *protocol.CommonObjectChange_CreateRole:
		role := operation.CreateRole.GetRole()
		if operation.CreateRole.GetPassword() != "" || role.GetPasswordDeclared() || role.GetSuperuser() || role.GetCreateDb() || role.GetCreateRole() || role.GetReplication() {
			return "", fmt.Errorf("role %s: A fixture role has a login and nothing else", role.GetName())
		}
		written = statement{Op: "create_role", Kind: principalKinds[role.GetGroup()], Name: role.GetName(), Login: role.GetLogin(), Text: role.GetComment()}
	case *protocol.CommonObjectChange_AlterRole:
		if !slices.Equal(operation.AlterRole.GetChanged(), []string{"login"}) {
			return "", fmt.Errorf("a fixture role changes only its login")
		}
		written = statement{Op: "alter_role", Name: operation.AlterRole.GetDesired().GetName(), Login: operation.AlterRole.GetDesired().GetLogin()}
	case *protocol.CommonObjectChange_DropRole:
		written = statement{Op: "drop_role", Kind: operation.DropRole.GetKind(), Name: operation.DropRole.GetName()}
	case *protocol.CommonObjectChange_AddMember:
		written = statement{Op: "add_member", Name: operation.AddMember.GetGroup(), Member: operation.AddMember.GetMember()}
	case *protocol.CommonObjectChange_RemoveMember:
		written = statement{Op: "remove_member", Name: operation.RemoveMember.GetGroup(), Member: operation.RemoveMember.GetMember()}
	case *protocol.CommonObjectChange_Grant:
		written = grantStatement("grant", operation.Grant)
	case *protocol.CommonObjectChange_Revoke:
		written = grantStatement("revoke", operation.Revoke)
	case *protocol.CommonObjectChange_SetComment:
		object := operation.SetComment.GetObject()
		written = statement{Op: "comment", Kind: object.GetKind(), Name: object.GetName(), Text: operation.SetComment.GetText()}
		if object.GetKind() == "column" {
			written.Table = object.GetPath()[0]
		}
	default:
		return "", fmt.Errorf("the fixture has no %T", operation)
	}
	encoded, err := json.Marshal(written)
	if err != nil {
		return "", err
	}
	return statementPrefix + string(encoded) + ";", nil
}

// principalKinds names a principal by whether it is a group.
var principalKinds = map[bool]string{false: "role", true: "group"}

func grantStatement(op string, grant *protocol.SQLGrant) statement {
	return statement{Op: op, Kind: grant.GetTarget().GetKind(), Name: grant.GetTarget().GetName(), Member: grant.GetRole(),
		Privileges: slices.Clone(grant.GetPrivileges()), Columns: slices.Clone(grant.GetColumns()), WithOption: grant.GetWithGrantOption()}
}

// readStatement reads a unit the program wrote, and reports whether sql is one.
func readStatement(sql string) (statement, bool, error) {
	text, found := strings.CutPrefix(strings.TrimSpace(sql), statementPrefix)
	if !found {
		return statement{}, false, nil
	}
	text, terminated := strings.CutSuffix(text, ";")
	if !terminated {
		return statement{}, true, fmt.Errorf("an unterminated fixture statement")
	}
	var read statement
	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	decoder.DisallowUnknownFields()
	return read, true, decoder.Decode(&read)
}

// AnalyzeSQL reads a source that is one of the program's statements as one
// statement whose safety its operation says, and leaves every other source to
// the base fixture.
func (p *commonObjects) AnalyzeSQL(ctx context.Context, request *protocol.AnalyzeSQLRequest) (*protocol.SQLAnalysis, error) {
	result := &protocol.SQLAnalysis{Complete: true}
	for _, source := range request.GetSources() {
		read, own, err := readStatement(source)
		if err != nil {
			return nil, err
		}
		if !own {
			analyzed, err := p.Provider.AnalyzeSQL(ctx, &protocol.AnalyzeSQLRequest{Engine: request.GetEngine(), Sources: []string{source}})
			if err != nil {
				return nil, err
			}
			result.Files = append(result.Files, analyzed.GetFiles()...)
			continue
		}
		start := strings.Index(source, statementPrefix)
		end := strings.LastIndex(source, ";") + 1
		safety := protocol.SQLStatement_BEHAVIORAL
		if slices.Contains(destructive, read.Op) {
			safety = protocol.SQLStatement_DESTRUCTIVE
		}
		words := []string{"FIXTURE", strings.ToUpper(read.Op)}
		result.Files = append(result.Files, &protocol.SQLFile{Statements: []*protocol.SQLStatement{{
			Start: uint32(start), End: uint32(end), Line: uint32(1 + strings.Count(source[:start], "\n")), // #nosec G115 -- a unit is far below 4 GiB
			Words: words, SourceWords: []string{"FIXTURE", read.Op}, Canonical: strings.Join(words, " "), Safety: safety,
			Reason: "The fixture changes a common object"}}})
	}
	return result, nil
}

// Execute runs the units in order. A statement of the program changes the
// database's views or its kept state; every other unit runs on the base
// fixture. A failure stops the batch after the units it applied.
func (p *commonObjects) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	var applied uint32
	for _, unit := range request.GetUnits() {
		read, own, err := readStatement(unit.GetSql())
		if err != nil {
			return &protocol.Execution{Outcome: outcomeAfter(applied), AppliedCount: applied, DiagnosticCode: "fixture.statement"}, nil
		}
		if !own {
			single := &protocol.ExecuteRequest{SessionId: request.GetSessionId(), RequestId: request.GetRequestId(), TransactionId: request.GetTransactionId(),
				Units: []*protocol.Unit{unit}, StatementTimeoutNanoseconds: request.GetStatementTimeoutNanoseconds()}
			execution, err := p.Provider.Execute(ctx, single)
			if err != nil || execution.GetOutcome() != protocol.Execution_APPLIED {
				return &protocol.Execution{Outcome: outcomeAfter(applied), AppliedCount: applied, DiagnosticCode: execution.GetDiagnosticCode()}, err
			}
			applied++
			continue
		}
		err = p.WithSession(ctx, request.GetSessionId(), request.GetTransactionId(), func(access backend.Access) error { return apply(ctx, access, read) })
		if err != nil {
			return &protocol.Execution{Outcome: outcomeAfter(applied), AppliedCount: applied, DiagnosticCode: "fixture.common-object"}, nil
		}
		applied++
	}
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: applied}, nil
}

func outcomeAfter(applied uint32) protocol.Execution_Outcome {
	if applied == 0 {
		return protocol.Execution_NOT_STARTED
	}
	return protocol.Execution_PARTIAL
}

// apply performs one statement: a view on the database, everything else on
// the kept state, which it writes back.
func apply(ctx context.Context, access backend.Access, read statement) error {
	held, err := loadState(access.Path)
	if err != nil {
		return err
	}
	switch read.Op {
	case "create_view", "replace_view":
		if read.Op == "replace_view" {
			if _, err := access.Querier.ExecContext(ctx, "DROP VIEW IF EXISTS "+quote(read.Name)); err != nil {
				return err
			}
		}
		if _, err := access.Querier.ExecContext(ctx, "CREATE VIEW "+quote(read.Name)+" AS "+read.Body); err != nil {
			return err
		}
		held.setComment(stateComment{Kind: "view", Name: read.Name, Text: read.Text})
	case "drop_view":
		if _, err := access.Querier.ExecContext(ctx, "DROP VIEW "+quote(read.Name)); err != nil {
			return err
		}
		held.setComment(stateComment{Kind: "view", Name: read.Name})
	case "create_role":
		if held.role(read.Name) >= 0 {
			return fmt.Errorf("role %s exists", read.Name)
		}
		held.Roles = append(held.Roles, stateRole{Name: read.Name, Login: read.Login, Group: read.Kind == "group"})
		held.setComment(stateComment{Kind: "role", Name: read.Name, Text: read.Text})
	case "alter_role":
		i := held.role(read.Name)
		if i < 0 {
			return fmt.Errorf("no role %s", read.Name)
		}
		held.Roles[i].Login = read.Login
	case "drop_role":
		held.Roles = slices.DeleteFunc(held.Roles, func(role stateRole) bool { return role.Name == read.Name })
		held.setComment(stateComment{Kind: "role", Name: read.Name})
	case "add_member", "remove_member":
		i := held.role(read.Member)
		if i < 0 || held.role(read.Name) < 0 {
			return fmt.Errorf("no role %s or group %s", read.Member, read.Name)
		}
		held.Roles[i].MemberOf = slices.DeleteFunc(held.Roles[i].MemberOf, func(group string) bool { return group == read.Name })
		if read.Op == "add_member" {
			held.Roles[i].MemberOf = append(held.Roles[i].MemberOf, read.Name)
		}
	case "grant", "revoke":
		applyGrant(&held, read)
	case "comment":
		held.setComment(stateComment{Kind: read.Kind, Table: read.Table, Name: read.Name, Text: read.Text})
	default:
		return fmt.Errorf("unknown fixture statement %s", read.Op)
	}
	return saveState(access.Path, held)
}

// applyGrant adds or takes away each privilege of a statement on its object,
// or only the grant option of a revocation that names it.
func applyGrant(held *state, read statement) {
	for _, privilege := range read.Privileges {
		same := func(grant stateGrant) bool {
			return grant.Role == read.Member && grant.Privilege == privilege && grant.Kind == read.Kind && grant.Name == read.Name && slices.Equal(grant.Columns, read.Columns)
		}
		i := slices.IndexFunc(held.Grants, same)
		switch {
		case read.Op == "revoke" && read.WithOption && i >= 0:
			held.Grants[i].WithOption = false
		case read.Op == "revoke":
			held.Grants = slices.DeleteFunc(held.Grants, same)
		case i >= 0:
			held.Grants[i].WithOption = held.Grants[i].WithOption || read.WithOption
		default:
			held.Grants = append(held.Grants, stateGrant{Role: read.Member, Privilege: privilege, Kind: read.Kind, Name: read.Name,
				Columns: slices.Clone(read.Columns), WithOption: read.WithOption})
		}
	}
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
