package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// PlanFeatures plans the tables the plan keeps and changes: each is rebuilt
// with its declared foreign keys and CHECKs. It refuses every other change,
// among them an index whose key states a NULLS order SQLite cannot build, in
// a diagnostic.
func (o *owner) PlanFeatures(ctx context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	result := &protocol.PlannedFeatures{Complete: true}
	contribution := &protocol.PlanningContribution{Owner: o.description.GetProviderId()}
	for _, table := range request.GetTables() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if table.GetAction() == protocol.PlanningTable_CONTEXT {
			continue
		}
		steps, refusal := o.planTable(request, table)
		if refusal != "" {
			return &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{{
				Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, Kind: "table", Message: refusal}}}, nil
		}
		contribution.Steps = append(contribution.Steps, steps...)
		var ids []*protocol.StepIdentity
		for _, step := range steps {
			ids = append(ids, proto.CloneOf(step.GetId()))
		}
		for _, kind := range request.GetParentKinds() {
			result.Parents = append(result.Parents, &protocol.PlanningParentReceipt{Action: table.GetAction(), Receipt: &protocol.PlanningReceipt{
				Subject: proto.CloneOf(table.GetSubject()), Kind: kind, Strategy: "Rebuild the table with its declared constraints", Steps: ids}})
		}
	}
	if len(contribution.Steps) != 0 {
		result.Contributions = append(result.Contributions, contribution)
	}
	return result, nil
}

// planTable plans one altered table, or says why it cannot.
func (o *owner) planTable(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable) ([]*protocol.PlanningStep, string) {
	declared, held := table.GetDesired(), table.GetCurrent()
	if table.GetAction() != protocol.PlanningTable_ALTER || declared == nil || held == nil {
		return nil, "the constraints fixture plans only the tables a plan keeps"
	}
	for _, index := range declared.GetIndexes() {
		for _, part := range keyParts(index) {
			if nullsOrder(part) != "" {
				return nil, fmt.Sprintf("SQLite keeps no NULLS order in an index key, so the fixture cannot build index %s with NULLS %s", index.GetName(), nullsOrder(part))
			}
		}
	}
	for _, common := range request.GetCommonSteps() {
		if proto.Equal(common.GetParent(), table.GetSubject()) {
			return nil, fmt.Sprintf("the constraints fixture plans no change of table %s but its constraints", declared.GetName())
		}
	}
	// The host hands the fixture a table it keeps only for a change of its
	// constraints, which the host compared, so the fixture rebuilds it
	// without comparing them again: a plan shows what the host handed.
	wanted, err := declaredConstraints(declared)
	if err != nil {
		return nil, err.Error()
	}
	value, err := o.statements(request.GetEngine(), declared.GetName(), rebuildStatements(declared, held, wanted))
	if err != nil {
		return nil, err.Error()
	}
	step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: o.description.GetProviderId(), Name: "rebuild/" + declared.GetName()},
		Metadata: &protocol.PlanningMetadata{Transaction: protocol.PlanningMetadata_FORBIDDEN, Impact: value.GetOperationSemantics().GetImpact(),
			Reason: value.GetOperationSemantics().GetReason(), Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(table.GetSubject()), Action: protocol.PlanningEffect_ALTER}}},
		Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}
	return []*protocol.PlanningStep{step}, ""
}

// constraint is a foreign key or CHECK as the fixture writes it.
type constraint struct {
	name, check                         string
	column, table, referenced, onDelete string
}

// text is the constraint as a table element.
func (c constraint) text() string {
	if c.check != "" {
		return fmt.Sprintf(`CONSTRAINT %s CHECK (%s)`, quote(c.name), c.check)
	}
	text := fmt.Sprintf(`CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)`, quote(c.name), quote(c.column), quote(c.table), quote(c.referenced))
	if c.onDelete != "" && c.onDelete != "NO ACTION" {
		text += " ON DELETE " + c.onDelete
	}
	return text
}

// declaredConstraints are the CHECKs and foreign keys a declaration states on
// its columns and as constraints of the table, each named.
func declaredConstraints(declared *protocol.DeclaredTable) ([]constraint, error) {
	var result []constraint
	for _, column := range declared.GetColumns() {
		if column.GetCheck() != "" {
			result = append(result, constraint{name: column.GetCheckName(), check: column.GetCheck()})
		}
		if reference := column.GetForeignReference(); reference != "" {
			table, referenced, found := strings.Cut(strings.TrimSuffix(reference, ")"), "(")
			if !found {
				return nil, fmt.Errorf("the fixture reads no reference %q", reference)
			}
			result = append(result, constraint{name: column.GetForeignKeyName(), column: column.GetName(), table: table, referenced: referenced,
				onDelete: strings.ToUpper(column.GetOnDelete())})
		}
	}
	for _, declaredConstraint := range declared.GetConstraints() {
		switch strings.ToUpper(declaredConstraint.GetType()) {
		case "CHECK":
			result = append(result, constraint{name: declaredConstraint.GetName(), check: declaredConstraint.GetCheckExpression()})
		case "FOREIGN KEY":
			if len(declaredConstraint.GetColumns()) != 1 {
				return nil, fmt.Errorf("the fixture writes a foreign key of one column, and %s has %d", declaredConstraint.GetName(), len(declaredConstraint.GetColumns()))
			}
			referenced := declaredConstraint.GetForeignColumn()
			if len(declaredConstraint.GetForeignColumns()) == 1 {
				referenced = declaredConstraint.GetForeignColumns()[0]
			}
			result = append(result, constraint{name: declaredConstraint.GetName(), column: declaredConstraint.GetColumns()[0], table: declaredConstraint.GetForeignTable(),
				referenced: referenced, onDelete: strings.ToUpper(declaredConstraint.GetOnDelete())})
		default:
			return nil, fmt.Errorf("the fixture writes no %s constraint", declaredConstraint.GetType())
		}
	}
	if len(declared.GetChecks()) != 0 {
		return nil, fmt.Errorf("the fixture writes named checks only")
	}
	for _, value := range result {
		if value.name == "" {
			return nil, fmt.Errorf("the fixture needs a name for each constraint of table %s", declared.GetName())
		}
	}
	return result, nil
}

// rebuildStatements write the declared table under a scratch name with the
// declared constraints, copy the rows of the columns both hold, drop the held
// table, give the new one its name and build the held indexes again.
func rebuildStatements(declared *protocol.DeclaredTable, held *protocol.ObservedTable, wanted []constraint) []string {
	name, scratch := declared.GetName(), declared.GetName()+"__rebuild"
	var elements, copied []string
	for _, column := range declared.GetColumns() {
		primary := column.GetPrimary() || slices.Contains(declared.GetPrimaryKey(), column.GetName())
		element := quote(column.GetName()) + " " + strings.ToUpper(column.GetType())
		if !column.GetNullable() || primary {
			element += " NOT NULL"
		}
		if primary {
			element += " PRIMARY KEY"
		}
		elements = append(elements, element)
		if slices.ContainsFunc(held.GetColumns(), func(observed *protocol.ObservedColumn) bool { return observed.GetName() == column.GetName() }) {
			copied = append(copied, quote(column.GetName()))
		}
	}
	for _, value := range wanted {
		elements = append(elements, value.text())
	}
	statements := []string{fmt.Sprintf("CREATE TABLE %s (%s);", quote(scratch), strings.Join(elements, ", "))}
	if len(copied) != 0 {
		list := strings.Join(copied, ", ")
		statements = append(statements, fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;", quote(scratch), list, list, quote(name)))
	}
	statements = append(statements, "DROP TABLE "+quote(name)+";", fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", quote(scratch), quote(name)))
	for _, index := range held.GetIndexes() {
		var keys []string
		for _, part := range index.GetParts() {
			key := quote(part.GetColumn())
			if part.GetDescending() {
				key += " DESC"
			}
			keys = append(keys, key)
		}
		unique := ""
		if index.GetUnique() {
			unique = "UNIQUE "
		}
		statements = append(statements, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);", unique, quote(index.GetName()), quote(name), strings.Join(keys, ", ")))
	}
	return statements
}

func quote(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
