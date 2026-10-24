package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
)

// PlanNative is a provider-native operation recorded in a saved plan.
//
// Operation is the owner's own encoding of the operation, wrapped in the codec
// envelope that names its owner, version and definition. A plan that holds one
// also records the provider binding, so the operation is replayed only through
// the provider release and target profile that produced it. Kind and Subject are
// copies for the person reviewing the file; apply decodes Operation and
// refuses a plan whose copies disagree with it.
type PlanNative struct {
	Kind      schemaext.Kind     `json:"kind"`
	Subject   string             `json:"subject,omitempty"`
	Operation schemaext.Envelope `json:"operation"`
}

// HasNative reports whether any statement of the plan is a provider-native
// operation. Such a plan has no complete SQL projection.
func (p PlanFile) HasNative() bool {
	return slices.ContainsFunc(p.Statements, func(statement PlanStatement) bool { return statement.Native != nil })
}

// errPlanNativeSQL refuses a path that can only run SQL over a plan holding a
// native operation. Skipping the operation would apply a partial plan.
func (p PlanFile) requireSQL() error {
	if p.HasNative() {
		return fmt.Errorf("%w: plan %q holds a provider-native operation; apply it with the provider's runtime",
			renderer.ErrNativeSQL, p.Name)
	}
	return nil
}

// UnitReports describes the plan's statements in order, a SQL text or the
// summary of a native operation. A native report carries no operation bytes.
func (p PlanFile) UnitReports() []ApplyUnitReport {
	if len(p.Statements) == 0 {
		return nil
	}
	units := make([]ApplyUnitReport, len(p.Statements))
	for i, statement := range p.Statements {
		units[i].SQL = statement.SQL
		if statement.Native != nil {
			units[i].Native = &NativeUnitReport{
				Kind: statement.Native.Kind, Subject: statement.Native.Subject,
				Severity: statement.Severity, Reason: statement.Reason,
				Access: statement.Access, AccessReason: statement.AccessReason,
			}
		}
	}
	return units
}

// classifyPlanUnits is the plan statement list of a captured batch that holds
// native operations. SQL units are judged as [classifyPlanStatements] judges
// them, and each native operation is judged by the effects its owner captured.
// The operations are encoded with the runtime's codecs, so the plan carries
// what apply decodes and nothing else.
func classifyPlanUnits(
	ctx context.Context,
	runtime PlanRuntime,
	units []renderer.Fragment,
	dialect string,
	owned []safety.StatementAssessment,
) (statements []PlanStatement, destructive bool, err error) {
	var sqlText []string
	var sqlOwned []safety.StatementAssessment
	var natives []renderer.Fragment
	var nativeOwned []safety.StatementAssessment
	var payloads []schemaext.Payload
	for i, unit := range units {
		var verdict safety.StatementAssessment
		if i < len(owned) {
			verdict = owned[i]
		}
		if unit.Native == nil {
			sqlText = append(sqlText, unit.SQL)
			sqlOwned = append(sqlOwned, verdict)
			continue
		}
		natives = append(natives, unit)
		nativeOwned = append(nativeOwned, verdict)
		payloads = append(payloads, unit.Native)
	}
	sqlStatements, _, err := classifyPlanStatements(ctx, runtime, sqlText, dialect, sqlOwned)
	if err != nil {
		return nil, false, err
	}
	assessments, err := safety.AssessUnits(ctx, runtime, dialect, natives, nativeOwned)
	if err != nil {
		return nil, false, err
	}
	envelopes, err := runtime.Codecs().Encode(ctx, schemaext.Operation, payloads)
	if err != nil {
		return nil, false, fmt.Errorf("encode native operations: %w", err)
	}
	statements = make([]PlanStatement, 0, len(units))
	sqlIndex, nativeIndex := 0, 0
	for _, unit := range units {
		if unit.Native == nil {
			statements = append(statements, sqlStatements[sqlIndex])
			sqlIndex++
			continue
		}
		assessment := assessments[nativeIndex]
		statements = append(statements, PlanStatement{
			Native: &PlanNative{
				Kind: unit.Native.Kind(), Subject: assessment.Subject, Operation: envelopes[nativeIndex],
			},
			Severity: assessment.Severity, Reason: assessment.Reason,
			Owned: true, Access: assessment.Access, AccessReason: assessment.AccessReason,
		})
		nativeIndex++
	}
	for _, statement := range statements {
		destructive = destructive || statement.Severity == safety.Destructive
	}
	return statements, destructive, nil
}

// validatePlanNative refuses a native statement that `schema plan` could not
// have written: one beside SQL text, one in a plan with no provider binding,
// one without an owner verdict, or one whose copies of the operation's kind
// disagree with the envelope.
func validatePlanNative(plan PlanFile, statement PlanStatement) error {
	native := statement.Native
	if native == nil {
		if strings.TrimSpace(statement.SQL) == "" {
			return errors.New("has empty sql")
		}
		return nil
	}
	switch {
	case statement.SQL != "":
		return errors.New("records both sql and a native operation")
	case plan.Provider == nil:
		return errors.New("records a native operation, which needs the provider binding of the release that produced it")
	case !statement.Owned:
		return errors.New("records a native operation without its owner verdict")
	case !native.Kind.Valid():
		return fmt.Errorf("native operation kind %q is not namespaced", native.Kind)
	case native.Operation.Kind != native.Kind:
		return fmt.Errorf("native operation kind %q differs from its envelope kind %q", native.Kind, native.Operation.Kind)
	case native.Operation.Representation != schemaext.Operation:
		return fmt.Errorf("native operation envelope has representation %q, want %q", native.Operation.Representation, schemaext.Operation)
	}
	return nil
}

// ExecutionPlan decodes the plan's statements into the captured form apply
// executes. Native operations are decoded through the runtime's codecs, so the
// selected provider must own the same operation definitions the plan was
// written with. The plan's provider binding is the apply plan's, so preflight
// refuses a connection served by anything else.
func (p PlanFile) ExecutionPlan(ctx context.Context, conn *dbschema.DatabaseConnection, runtime schemaext.ModelRuntime) (ApplyPlan, error) {
	if conn == nil {
		return ApplyPlan{}, errors.New("schema apply execution requires database connection")
	}
	var envelopes []schemaext.Envelope
	for _, statement := range p.Statements {
		if statement.Native != nil {
			envelopes = append(envelopes, statement.Native.Operation)
		}
	}
	var decoded []schemaext.Payload
	if len(envelopes) > 0 {
		var err error
		if decoded, err = runtime.Codecs().Decode(ctx, envelopes); err != nil {
			return ApplyPlan{}, fmt.Errorf("decode native operations of plan %q: %w", p.Name, err)
		}
	}
	info := conn.Info()
	units := make([]renderer.Fragment, len(p.Statements))
	next := 0
	for i, statement := range p.Statements {
		if statement.Native == nil {
			units[i] = renderer.Fragment{SQL: statement.SQL}
			continue
		}
		payload, ok := decoded[next].(ast.ExtensionPayload)
		if !ok || payload.Kind() != statement.Native.Kind {
			return ApplyPlan{}, fmt.Errorf("%w: native operation %d of plan %q decodes to another kind than %q",
				renderer.ErrInvalidResult, i+1, p.Name, statement.Native.Kind)
		}
		units[i] = renderer.Fragment{Native: payload}
		next++
	}
	transactions := make([][]plangraph.Transaction, len(p.Statements))
	for i, statement := range p.Statements {
		transactions[i] = slices.Clone(statement.Transactions)
	}
	return ApplyPlan{
		target: info.Dialect, capabilities: info.Capabilities, provider: p.Provider,
		units: units, transactions: transactions,
	}, nil
}

// ExecuteUnits runs a saved plan that holds native operations, in statement
// order, under the transaction mode and the owners' requirements. It decodes and
// preflights the whole plan before the first statement runs. The caller must
// verify the target and approve the plan first.
func (p PlanFile) ExecuteUnits(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode, runtime schemaext.ModelRuntime) error {
	execution, err := p.ExecutionPlan(ctx, conn, runtime)
	if err != nil {
		return err
	}
	return execution.Execute(ctx, conn, mode)
}
