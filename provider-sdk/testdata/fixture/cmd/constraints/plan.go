package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// PlanFeatures plans the tables the plan keeps and changes: each is rebuilt
// with its declared foreign keys and CHECKs, or has the indexes the host adds
// and drops built and dropped in place of the host's steps, or, where its
// constraints stay, the columns the host adds and drops added and dropped in
// place of the host's steps. In the text mode it also creates and drops
// tables, in place of the host's steps. It refuses every other change, among
// them an index whose key states a NULLS order SQLite cannot build, in a
// diagnostic.
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
		refusal := blockedRemoval(table)
		var steps []*protocol.PlanningStep
		var rewrites []*protocol.PlanningRewrite
		if refusal == "" {
			steps, rewrites, refusal = o.planLifecycle(request, table)
		}
		if refusal != "" {
			return &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{{
				Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, Kind: "table", Message: refusal}}}, nil
		}
		contribution.Steps = append(contribution.Steps, steps...)
		result.Rewrites = append(result.Rewrites, rewrites...)
		var ids []*protocol.StepIdentity
		for _, step := range steps {
			ids = append(ids, proto.CloneOf(step.GetId()))
		}
		kinds := slices.Clone(request.GetParentKinds())
		if o.ownsTables {
			kinds = append(kinds, provider.TableReceiptKind)
		}
		for _, kind := range kinds {
			result.Parents = append(result.Parents, &protocol.PlanningParentReceipt{Action: table.GetAction(), Receipt: &protocol.PlanningReceipt{
				Subject: proto.CloneOf(table.GetSubject()), Kind: kind, Strategy: "Rebuild the table with its declared constraints", Steps: ids}})
		}
	}
	if len(contribution.Steps) != 0 {
		result.Contributions = append(result.Contributions, contribution)
	}
	return result, nil
}

// planLifecycle plans one table: in the text mode a created table is one
// CREATE TABLE with its declared constraints and a dropped one a DROP TABLE,
// each in place of the host's step, and an altered table is rebuilt.
func (o *owner) planLifecycle(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, string) {
	if !o.text || table.GetAction() == protocol.PlanningTable_ALTER {
		return o.planTable(request, table)
	}
	var statement, name string
	switch table.GetAction() {
	case protocol.PlanningTable_CREATE:
		declared := table.GetDesired()
		wanted, err := declaredConstraints(declared)
		if err != nil {
			return nil, nil, err.Error()
		}
		name = declared.GetName()
		statement = createStatement(quote(name), declared, wanted)
	case protocol.PlanningTable_DROP:
		name = table.GetCurrent().GetName()
		statement = "DROP TABLE " + quote(name) + ";"
	default:
		return nil, nil, "the constraints fixture creates, drops and rebuilds tables only"
	}
	common := slices.IndexFunc(request.GetCommonSteps(), func(step *protocol.CommonPlanningStep) bool { return proto.Equal(step.GetParent(), table.GetSubject()) })
	if common < 0 {
		return nil, nil, fmt.Sprintf("the host planned no step for table %s", name)
	}
	id := &protocol.StepIdentity{Owner: o.description.GetProviderId(), Name: "lifecycle/" + name}
	value, err := o.statements(request.GetEngine(), id.GetName(), name, []string{statement})
	if err != nil {
		return nil, nil, err.Error()
	}
	// The step stands for the host's, so it carries that step's effects.
	metadata := proto.CloneOf(request.GetCommonSteps()[common].GetMetadata())
	metadata.Transaction = o.transaction()
	metadata.Impact, metadata.Reason = value.GetOperationSemantics().GetImpact(), value.GetOperationSemantics().GetReason()
	step := &protocol.PlanningStep{Id: id, Metadata: metadata, Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}
	rewrite := &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(request.GetCommonSteps()[common].GetId())}, Replacement: proto.CloneOf(id)}
	return []*protocol.PlanningStep{step}, []*protocol.PlanningRewrite{rewrite}, ""
}

// planTable plans one altered table, or says why it cannot.
func (o *owner) planTable(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, string) {
	declared, held := table.GetDesired(), table.GetCurrent()
	if table.GetAction() != protocol.PlanningTable_ALTER || declared == nil || held == nil {
		return nil, nil, "the constraints fixture plans only the tables a plan keeps"
	}
	for _, index := range declared.GetIndexes() {
		for _, part := range keyParts(index) {
			if nullsOrder(part) != "" {
				return nil, nil, fmt.Sprintf("SQLite keeps no NULLS order in an index key, so the fixture cannot build index %s with NULLS %s", index.GetName(), nullsOrder(part))
			}
		}
	}
	// The host hands the fixture a table it keeps for a change of its
	// constraints or of its indexes, which the host compared, so the fixture
	// plans it without comparing them again: a plan shows what the host
	// handed.
	wanted, err := declaredConstraints(declared)
	if err != nil {
		return nil, nil, err.Error()
	}
	var common, columns []*protocol.CommonPlanningStep
	for _, step := range request.GetCommonSteps() {
		if !proto.Equal(step.GetParent(), table.GetSubject()) {
			continue
		}
		switch {
		case columnName(step) != "":
			columns = append(columns, step)
		case indexName(step) != "":
			common = append(common, step)
		default:
			return nil, nil, fmt.Sprintf("the constraints fixture plans no change of table %s but its columns, constraints and indexes", declared.GetName())
		}
	}
	if len(common) != 0 {
		if len(columns) != 0 || !sameConstraints(wanted, held) {
			return nil, nil, fmt.Sprintf("the constraints fixture plans the indexes of table %s alone, not with its columns or constraints", declared.GetName())
		}
		return o.planIndexes(request.GetEngine(), declared, common)
	}
	// The fixture decides from the table the host says it holds: where its
	// constraints are the declared ones, a column is added or dropped in
	// place, and SQLite refuses to drop one a constraint the table holds
	// still names. Otherwise the table is rebuilt with its columns.
	if len(columns) != 0 && sameConstraints(wanted, held) {
		return o.planColumns(request.GetEngine(), declared, columns)
	}
	statements := append(rebuildStatements(declared, held, wanted), regrants(declared.GetName(), table.GetCurrentGrants())...)
	value, err := o.statements(request.GetEngine(), "rebuild/"+declared.GetName(), declared.GetName(), statements)
	if err != nil {
		return nil, nil, err.Error()
	}
	step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: o.description.GetProviderId(), Name: "rebuild/" + declared.GetName()},
		Metadata: &protocol.PlanningMetadata{Transaction: o.transaction(), Impact: value.GetOperationSemantics().GetImpact(),
			Reason: value.GetOperationSemantics().GetReason(), Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(table.GetSubject()), Action: protocol.PlanningEffect_ALTER}}},
		Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}
	if len(columns) == 0 {
		return []*protocol.PlanningStep{step}, nil, ""
	}
	// The rebuild stands for the host's column steps, so it carries their
	// effects on the columns beside its own on the table.
	rewrite := &protocol.PlanningRewrite{Replacement: proto.CloneOf(step.GetId())}
	for _, source := range columns {
		rewrite.Sources = append(rewrite.Sources, proto.CloneOf(source.GetId()))
		for _, effect := range source.GetMetadata().GetEffects() {
			if !proto.Equal(effect.GetSubject(), table.GetSubject()) {
				step.Metadata.Effects = append(step.Metadata.Effects, proto.CloneOf(effect))
			}
		}
	}
	return []*protocol.PlanningStep{step}, []*protocol.PlanningRewrite{rewrite}, ""
}

// planColumns plans one statement for each host step that adds or drops a
// column of declared, with the step's footprint, in place of the step: an
// ALTER TABLE ... ADD COLUMN of the declared column, or an ALTER TABLE ...
// DROP COLUMN.
func (o *owner) planColumns(engine string, declared *protocol.DeclaredTable, columns []*protocol.CommonPlanningStep) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, string) {
	var steps []*protocol.PlanningStep
	var rewrites []*protocol.PlanningRewrite
	for _, source := range columns {
		name := columnName(source)
		statement := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", quote(declared.GetName()), quote(name))
		id := "column/" + declared.GetName() + "/drop/" + name
		if added := source.GetAddedColumn(); added != nil {
			definition := quote(name) + " " + columnType(added.GetType())
			if !added.GetNullable() {
				definition += " NOT NULL"
			}
			statement, id = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quote(declared.GetName()), definition), "column/"+declared.GetName()+"/add/"+name
		}
		value, err := o.statements(engine, id, declared.GetName(), []string{statement})
		if err != nil {
			return nil, nil, err.Error()
		}
		var effects []*protocol.PlanningEffect
		for _, effect := range source.GetMetadata().GetEffects() {
			effects = append(effects, proto.CloneOf(effect))
		}
		step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: o.description.GetProviderId(), Name: id},
			Metadata: &protocol.PlanningMetadata{Transaction: o.transaction(), Impact: value.GetOperationSemantics().GetImpact(),
				Reason: value.GetOperationSemantics().GetReason(), Effects: effects},
			Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}
		steps = append(steps, step)
		rewrites = append(rewrites, &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(source.GetId())}, Replacement: proto.CloneOf(step.GetId())})
	}
	return steps, rewrites, ""
}

// columnName is the column a host step adds or drops, or empty for a step
// that changes no column. A column step reads its table and creates or drops
// one column of it.
func columnName(common *protocol.CommonPlanningStep) string {
	if added := common.GetAddedColumn(); added != nil {
		return added.GetName()
	}
	for _, effect := range common.GetMetadata().GetEffects() {
		if effect.GetAction() == protocol.PlanningEffect_DROP && effect.GetSubject().GetKind() == "column" {
			return effect.GetSubject().GetName().GetSource()
		}
	}
	return ""
}

// planIndexes plans one statement for each host step that adds or drops an
// index of declared, with the step's footprint, in place of the step: a DROP
// INDEX for a removal, and a CREATE INDEX of the declared index for an
// addition, a partial one with its predicate in the fixture's spelling. Each
// takes the lifecycle phase, which lets the host order the drop and the
// creation of a replaced index together.
func (o *owner) planIndexes(engine string, declared *protocol.DeclaredTable, common []*protocol.CommonPlanningStep) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, string) {
	var steps []*protocol.PlanningStep
	var rewrites []*protocol.PlanningRewrite
	for _, source := range common {
		name := indexName(source)
		statement := "DROP INDEX " + quote(name) + ";"
		id := "index/" + declared.GetName() + "/drop/" + name
		if indexCreated(source) {
			position := slices.IndexFunc(declared.GetIndexes(), func(index *protocol.DeclaredIndex) bool { return index.GetName() == name })
			if position < 0 {
				return nil, nil, fmt.Sprintf("the host adds index %s, which table %s does not declare", name, declared.GetName())
			}
			statement, id = createIndex(declared.GetName(), declared.GetIndexes()[position]), "index/"+declared.GetName()+"/create/"+name
		}
		value, err := o.statements(engine, id, declared.GetName(), []string{statement})
		if err != nil {
			return nil, nil, err.Error()
		}
		var effects []*protocol.PlanningEffect
		for _, effect := range source.GetMetadata().GetEffects() {
			effects = append(effects, proto.CloneOf(effect))
		}
		step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: o.description.GetProviderId(), Name: id},
			Metadata: &protocol.PlanningMetadata{Transaction: o.transaction(), Impact: value.GetOperationSemantics().GetImpact(),
				Reason: value.GetOperationSemantics().GetReason(), Effects: effects},
			Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT, Phase: protocol.PlanningOperation_LIFECYCLE}}
		steps = append(steps, step)
		rewrites = append(rewrites, &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(source.GetId())}, Replacement: proto.CloneOf(step.GetId())})
	}
	return steps, rewrites, ""
}

// indexName is the index a host step adds or drops, or empty for a step that
// changes no index: one with an operand of its own, or with no index among
// its effects.
func indexName(common *protocol.CommonPlanningStep) string {
	if common.GetAddedColumn() != nil || common.GetRenamedIndex() != nil || common.GetCommonObject() != nil {
		return ""
	}
	for _, effect := range common.GetMetadata().GetEffects() {
		if effect.GetSubject().GetKind() == "index" {
			return effect.GetSubject().GetName().GetSource()
		}
	}
	return ""
}

// indexCreated reports whether a host index step adds its index.
func indexCreated(common *protocol.CommonPlanningStep) bool {
	return slices.ContainsFunc(common.GetMetadata().GetEffects(), func(effect *protocol.PlanningEffect) bool {
		return effect.GetSubject().GetKind() == "index" && effect.GetAction() == protocol.PlanningEffect_CREATE
	})
}

// createIndex is the statement that builds a declared index of table.
func createIndex(table string, index *protocol.DeclaredIndex) string {
	var keys []string
	for _, part := range keyParts(index) {
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
	predicate := ""
	if strings.TrimSpace(index.GetCondition()) != "" {
		predicate = " WHERE " + predicateSpelling(index.GetCondition())
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)%s;", unique, quote(index.GetName()), quote(table), strings.Join(keys, ", "), predicate)
}

// sameConstraints reports whether a table holds the constraints wanted
// declares, by name, and each CHECK by the fixture's reading of its text.
func sameConstraints(wanted []constraint, held *protocol.ObservedTable) bool {
	if len(wanted) != len(held.GetConstraints()) {
		return false
	}
	for _, value := range wanted {
		position := slices.IndexFunc(held.GetConstraints(), func(observed *protocol.ObservedConstraint) bool { return observed.GetName() == value.name })
		if position < 0 || value.check != "" && checkText(value.check) != checkText(held.GetConstraints()[position].GetCheckClause()) {
			return false
		}
	}
	return true
}

// constraint is a foreign key, CHECK or UNIQUE as the fixture writes it.
type constraint struct {
	name, check                         string
	column, table, referenced, onDelete string
	unique                              []string
}

// text is the constraint as a table element.
func (c constraint) text() string {
	if c.check != "" {
		return fmt.Sprintf(`CONSTRAINT %s CHECK (%s)`, quote(c.name), c.check)
	}
	if len(c.unique) != 0 {
		columns := make([]string, len(c.unique))
		for i, column := range c.unique {
			columns[i] = quote(column)
		}
		return fmt.Sprintf(`CONSTRAINT %s UNIQUE (%s)`, quote(c.name), strings.Join(columns, ", "))
	}
	text := fmt.Sprintf(`CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)`, quote(c.name), quote(c.column), quote(c.table), quote(c.referenced))
	if c.onDelete != "" && c.onDelete != "NO ACTION" {
		text += " ON DELETE " + c.onDelete
	}
	return text
}

// declaredConstraints are the CHECKs and foreign keys a declaration states on
// its columns, and the CHECKs, foreign keys and UNIQUE constraints it states
// as constraints of the table, each named. A column's CHECK without a name
// takes the name the host compares it under, <table>_<column>_check.
func declaredConstraints(declared *protocol.DeclaredTable) ([]constraint, error) {
	var result []constraint
	for _, column := range declared.GetColumns() {
		if column.GetCheck() != "" {
			name := column.GetCheckName()
			if name == "" {
				name = declared.GetName() + "_" + column.GetName() + "_check"
			}
			result = append(result, constraint{name: name, check: column.GetCheck()})
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
		case "UNIQUE":
			result = append(result, constraint{name: declaredConstraint.GetName(), unique: slices.Clone(declaredConstraint.GetColumns())})
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

// createStatement writes CREATE TABLE of the declared table's columns and
// the constraints wanted under the quoted name.
func createStatement(name string, declared *protocol.DeclaredTable, wanted []constraint) string {
	var elements []string
	for _, column := range declared.GetColumns() {
		primary := column.GetPrimary() || slices.Contains(declared.GetPrimaryKey(), column.GetName())
		element := quote(column.GetName()) + " " + columnType(column.GetType())
		if !column.GetNullable() || primary {
			element += " NOT NULL"
		}
		if primary {
			element += " PRIMARY KEY"
		}
		elements = append(elements, element)
	}
	for _, value := range wanted {
		elements = append(elements, value.text())
	}
	return fmt.Sprintf("CREATE TABLE %s (%s);", name, strings.Join(elements, ", "))
}

// rebuildStatements write the declared table under a scratch name with the
// declared constraints, copy the rows of the columns both hold, drop the held
// table, give the new one its name and build the held indexes again.
func rebuildStatements(declared *protocol.DeclaredTable, held *protocol.ObservedTable, wanted []constraint) []string {
	name, scratch := declared.GetName(), declared.GetName()+"__rebuild"
	var copied []string
	for _, column := range declared.GetColumns() {
		if slices.ContainsFunc(held.GetColumns(), func(observed *protocol.ObservedColumn) bool { return observed.GetName() == column.GetName() }) {
			copied = append(copied, quote(column.GetName()))
		}
	}
	statements := []string{createStatement(quote(scratch), declared, wanted)}
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

// transaction is how a step of the fixture takes a transaction: never, as
// its statements run, unless the mode states a session's schema changes as
// ones a transaction holds.
func (o *owner) transaction() protocol.PlanningMetadata_Transaction {
	if o.transactional {
		return protocol.PlanningMetadata_ALLOWED
	}
	return protocol.PlanningMetadata_FORBIDDEN
}

func quote(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

// columnType is a declared type as the fixture writes it: in upper case, and
// as written where it quotes values, as enum('a','b') does.
func columnType(declared string) string {
	if strings.ContainsRune(declared, '\'') {
		return declared
	}
	return strings.ToUpper(declared)
}

// regrants give a rebuilt table the grants the old one held, which the
// request carries under planning.table-grants: SQLite keeps no grant, and the
// drop of the old table takes its own, as on an engine that keeps them in a
// table's access list. A request without them, from a host that did not
// negotiate the feature, gets none back. The statements are text a
// migration file holds; the fixture never runs them.
func regrants(table string, grants *protocol.PlanningTableGrants) []string {
	var statements []string
	for _, grant := range grants.GetGrants() {
		if grant.GetImplicit() {
			continue
		}
		statements = append(statements, "GRANT "+grant.GetPrivilege()+" ON "+quote(table)+" TO "+quote(grant.GetRole())+";")
	}
	return statements
}
