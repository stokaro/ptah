package providerhost

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
)

// sqlSchemaResult maps a complete reading into the host's model. The common
// objects map field by field; the provider's own objects are normalized by
// their owner and placed as feature objects; the claim the reading makes is
// the load's coverage. A field the host model has no home for is refused by
// name rather than dropped.
func (t *target) sqlSchemaResult(ctx context.Context, request sqlext.GrammarRequest, reply *protocol.SQLSchema) (sqlext.GrammarResult, error) {
	database, err := sqlSchemaCommon(reply)
	if err != nil {
		return sqlext.GrammarResult{}, err
	}
	database.FeatureObjects, err = t.sqlNativeObjects(ctx, reply.GetNativeObjects())
	if err != nil {
		return sqlext.GrammarResult{}, err
	}
	claim, err := t.sqlCoverage(reply.GetCoverage())
	if err != nil {
		return sqlext.GrammarResult{}, err
	}
	result := sqlext.GrammarResult{Database: database, Coverage: claim}
	for i, declared := range reply.GetDeclarations() {
		result.Files = append(result.Files, sqlFileDeclarations(request.Files[i].Path, declared))
	}
	return result, nil
}

// sqlSchemaCommon maps the common objects of a reading: tables with their
// columns, keys, indexes, constraints and triggers, namespaces, enums, views,
// sequences, roles, and grants and revocations.
func sqlSchemaCommon(reply *protocol.SQLSchema) (schemamodel.Database, error) {
	var database schemamodel.Database
	for _, table := range reply.GetTables() {
		if err := addSQLTable(&database, table); err != nil {
			return schemamodel.Database{}, err
		}
	}
	for _, namespace := range reply.GetNamespaces() {
		database.Schemas = append(database.Schemas, schemamodel.Schema{Name: namespace.GetName(), Comment: namespace.GetComment(), Charset: namespace.GetCharset(), Collate: namespace.GetCollation()})
	}
	for _, declared := range reply.GetEnums() {
		if err := addSQLEnum(&database, declared); err != nil {
			return schemamodel.Database{}, err
		}
	}
	for _, view := range reply.GetViews() {
		if err := addSQLView(&database, view); err != nil {
			return schemamodel.Database{}, err
		}
	}
	for _, sequence := range reply.GetSequences() {
		database.Sequences = append(database.Sequences, schemamodel.Sequence{Schema: sequence.GetSchema(), Name: sequence.GetName(), AsType: sequence.GetAsType(),
			Start: copyPlanningPointer(sequence.Start), Increment: copyPlanningPointer(sequence.Increment), MinValue: copyPlanningPointer(sequence.MinValue),
			MaxValue: copyPlanningPointer(sequence.MaxValue), Cache: copyPlanningPointer(sequence.Cache), Cycle: sequence.GetCycle(), OwnedBy: sequence.GetOwnedBy(), Comment: sequence.GetComment()})
	}
	for _, role := range reply.GetRoles() {
		database.Roles = append(database.Roles, schemamodel.Role{Name: role.GetName(), Login: role.GetLogin(), Superuser: role.GetSuperuser(), CreateDB: role.GetCreateDb(),
			CreateRole: role.GetCreateRole(), Inherit: role.GetInherit(), Replication: role.GetReplication(), Comment: role.GetComment(), Group: role.GetGroup(), MemberOf: slices.Clone(role.GetMemberOf())})
	}
	for _, declared := range reply.GetGrants() {
		grant, err := sqlGrant(declared)
		if err != nil {
			return schemamodel.Database{}, err
		}
		if declared.GetRevoked() {
			database.RevokedGrants = append(database.RevokedGrants, grant)
		} else {
			database.Grants = append(database.Grants, grant)
		}
	}
	return database, nil
}

// addSQLTable places a declared table and its children. The children name the
// table by its qualified name, and by a struct name that keys them to it
// alone, as the shared reader keys a table's children.
func addSQLTable(database *schemamodel.Database, declared *protocol.DeclaredTable) error {
	if proto.Size(declared.GetFeatures()) != 0 {
		return unsupported(fmt.Sprintf("facets or owned objects of table %q in a SQL schema reading", declared.GetName()))
	}
	qualified := schemamodel.QualifyTableName(declared.GetSchema(), declared.GetName())
	table := schemamodel.Table{StructName: qualified, Name: declared.GetName(), Schema: declared.GetSchema(), Engine: declared.GetEngine(), Collate: declared.GetCollation(),
		Comment: declared.GetComment(), PrimaryKey: slices.Clone(declared.GetPrimaryKey()), PrimaryKeyName: declared.GetPrimaryKeyName(),
		PrimaryKeyInclude: slices.Clone(declared.GetPrimaryKeyInclude()), PrimaryKeyMethod: declared.GetPrimaryKeyMethod(), PrimaryKeyComment: declared.GetPrimaryKeyComment(),
		PrimaryKeyDeferrable: declared.GetPrimaryKeyDeferrable(), PrimaryKeyInitially: declared.GetPrimaryKeyInitially(), Checks: slices.Clone(declared.GetChecks()),
		CustomSQL: declared.GetCustomSql(), DependsOn: slices.Clone(declared.GetDependsOn())}
	for _, part := range declared.GetPrimaryKeyParts() {
		table.PrimaryKeyParts = append(table.PrimaryKeyParts, schemamodel.PrimaryKeyPart{Name: part.GetColumn(), Prefix: part.GetPrefix(), Desc: part.GetDescending()})
	}
	database.Tables = append(database.Tables, table)
	for _, column := range declared.GetColumns() {
		database.Fields = append(database.Fields, sqlField(qualified, column))
	}
	for _, declaredEnum := range declared.GetEnums() {
		if err := addSQLEnum(database, declaredEnum); err != nil {
			return err
		}
	}
	for _, constraint := range declared.GetConstraints() {
		database.Constraints = append(database.Constraints, sqlConstraint(qualified, constraint))
	}
	for _, index := range declared.GetIndexes() {
		database.Indexes = append(database.Indexes, sqlIndex(qualified, index))
	}
	for _, trigger := range declared.GetTriggers() {
		if trigger.GetSchema() != "" && trigger.GetSchema() != declared.GetSchema() {
			return unsupported(fmt.Sprintf("trigger %q of table %q in another schema", trigger.GetName(), qualified))
		}
		database.Triggers = append(database.Triggers, schemamodel.Trigger{Name: trigger.GetName(), Table: orDefault(trigger.GetTable(), qualified), Timing: trigger.GetTiming(),
			Event: trigger.GetEvent(), ForEach: trigger.GetForEach(), Body: trigger.GetBody(), Comment: trigger.GetComment(), When: trigger.GetWhen(), OldTable: trigger.GetOldTable(),
			NewTable: trigger.GetNewTable(), ExecuteFunction: trigger.GetExecuteFunction(), Dialects: slices.Clone(trigger.GetTargets())})
	}
	return nil
}

func sqlField(structName string, column *protocol.DeclaredColumn) schemamodel.Field {
	return schemamodel.Field{
		StructName: structName, Name: column.GetName(), Type: column.GetType(), TypeRawSQL: column.GetTypeRawSql(), TypeIsDeclaredText: column.GetTypeIsDeclaredText(),
		Nullable: column.GetNullable(), Primary: column.GetPrimary(), AutoInc: column.GetAutoIncrement(), IdentityGeneration: column.GetIdentityGeneration(),
		IdentityStart: column.GetIdentityStart(), IdentityIncrement: column.GetIdentityIncrement(), IdentityOptions: column.GetIdentityOptions(), Unique: column.GetUnique(),
		UniqueExpr: column.GetUniqueExpression(), Default: column.GetDefaultValue(), DefaultSet: column.GetDefaultSet(), DefaultExpr: column.GetDefaultExpression(),
		Foreign: column.GetForeignReference(), ForeignKeyName: column.GetForeignKeyName(), OnDelete: column.GetOnDelete(), OnUpdate: column.GetOnUpdate(),
		Deferrable: column.GetDeferrable(), Initially: column.GetInitially(), ForeignKeyMatch: column.GetForeignKeyMatch(), ForeignKeyNotEnforced: column.GetForeignKeyNotEnforced(),
		Enum: slices.Clone(column.GetEnumValues()), Check: column.GetCheck(), CheckName: column.GetCheckName(), CheckNotEnforced: column.GetCheckNotEnforced(),
		NotNullConstraintName: column.GetNotNullConstraintName(), GeneratedExpression: column.GetGeneratedExpression(), GeneratedKind: column.GetGeneratedKind(),
		Collate: column.GetCollation(), Comment: column.GetComment(),
	}
}

func sqlConstraint(table string, constraint *protocol.DeclaredConstraint) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: table, Name: constraint.GetName(), Type: constraint.GetType(), Table: orDefault(constraint.GetTable(), table), UsingMethod: constraint.GetUsingMethod(),
		CheckExpression: constraint.GetCheckExpression(), Columns: slices.Clone(constraint.GetColumns()), IncludeColumns: slices.Clone(constraint.GetIncludeColumns()),
		NullsDistinct: copyPlanningPointer(constraint.NullsDistinct), ForeignTable: constraint.GetForeignTable(), ForeignColumn: constraint.GetForeignColumn(),
		ForeignColumns: slices.Clone(constraint.GetForeignColumns()), OnDelete: constraint.GetOnDelete(), OnUpdate: constraint.GetOnUpdate(), Deferrable: constraint.GetDeferrable(),
		Initially: constraint.GetInitially(), Match: constraint.GetMatch(), NotEnforced: constraint.GetNotEnforced(), NotValid: constraint.GetNotValid(),
		Requires: boundIdentities(constraint.GetRequires()), Comment: constraint.GetComment(),
	}
}

func sqlIndex(table string, index *protocol.DeclaredIndex) schemamodel.Index {
	result := schemamodel.Index{
		StructName: table, Name: index.GetName(), Fields: slices.Clone(index.GetFields()), Unique: index.GetUnique(), Comment: index.GetComment(), Invisible: index.GetInvisible(),
		NullsDistinct: copyPlanningPointer(index.NullsDistinct), Type: index.GetType(), Condition: index.GetCondition(), Concurrently: index.GetConcurrently(),
		Operator: index.GetOperator(), IncludeColumns: slices.Clone(index.GetIncludeColumns()), StorageParams: maps.Clone(index.GetStorageParameters()),
		TableName: orDefault(index.GetTableName(), table), Requires: boundIdentities(index.GetRequires()),
	}
	for _, part := range index.GetParts() {
		result.Parts = append(result.Parts, schemamodel.IndexPart{Name: part.GetColumn(), Expr: part.GetExpression(), Operator: part.GetOperatorClass(), Prefix: part.GetPrefix(),
			Desc: part.GetDescending(), NullsOrder: part.GetNulls()})
	}
	return result
}

// addSQLEnum adds an enum the reading declares once. A table repeats the enums
// its columns use, so a repetition with the same values is the same enum; one
// with other values is two declarations of one name.
func addSQLEnum(database *schemamodel.Database, declared *protocol.DeclaredEnum) error {
	enum := schemamodel.Enum{Name: declared.GetName(), Schema: declared.GetSchema(), Values: slices.Clone(declared.GetValues()), Comment: declared.GetComment()}
	for _, existing := range database.Enums {
		if existing.Name != enum.Name || existing.Schema != enum.Schema {
			continue
		}
		if !slices.Equal(existing.Values, enum.Values) || existing.Comment != enum.Comment {
			return fmt.Errorf("%w: the SQL schema reading declares enum %q twice, differently", schemaext.ErrDuplicate, enum.QualifiedName())
		}
		return nil
	}
	database.Enums = append(database.Enums, enum)
	return nil
}

func addSQLView(database *schemamodel.Database, view *protocol.SQLView) error {
	name := schemamodel.QualifyTableName(view.GetSchema(), view.GetName())
	if !view.GetMaterialized() {
		database.Views = append(database.Views, schemamodel.View{Name: name, Body: view.GetBody(), WithCheck: view.GetWithCheck(), Comment: view.GetComment(),
			Attributes: slices.Clone(view.GetAttributes()), DependsOn: slices.Clone(view.GetDependsOn())})
		return nil
	}
	if view.GetWithCheck() || len(view.GetAttributes()) != 0 {
		return unsupported(fmt.Sprintf("CHECK OPTION or attributes of materialized view %q", name))
	}
	database.MaterializedViews = append(database.MaterializedViews, schemamodel.MaterializedView{Name: name, Body: view.GetBody(), Comment: view.GetComment(),
		DependsOn: slices.Clone(view.GetDependsOn())})
	return nil
}

// sqlGrant places a grant by the kind of its target, compared without regard
// to case. A target the host model names by a field of its own takes that
// field; an object kind of [schemamodel.GrantObjectKinds], such as
// "coordination node", takes OnObject with the kind as the model spells it.
func sqlGrant(declared *protocol.SQLGrant) (schemamodel.Grant, error) {
	target := declared.GetTarget()
	grant := schemamodel.Grant{Role: declared.GetRole(), Privileges: slices.Clone(declared.GetPrivileges()), WithOption: declared.GetWithGrantOption(), Comment: declared.GetComment()}
	if target.GetDatabase() != "" || target.GetCatalog() != "" || len(target.GetPath()) != 0 {
		return schemamodel.Grant{}, unsupported(fmt.Sprintf("a grant target %q qualified by a database, catalog or path", target.GetName()))
	}
	qualified := schemamodel.QualifyTableName(target.GetSchema(), target.GetName())
	if len(target.GetOverload()) != 0 && !isRoutineKind(target.GetKind()) {
		return schemamodel.Grant{}, unsupported(fmt.Sprintf("argument types of grant target %q, which is not a routine", qualified))
	}
	switch kind := strings.ToLower(target.GetKind()); {
	case kind == "table":
		grant.OnTable = qualified
	case kind == "schema":
		grant.OnSchema = target.GetName()
	case kind == "sequence":
		grant.OnSequence = qualified
	case kind == "database":
		grant.OnDatabase = true
	case isRoutineKind(kind):
		grant.OnRoutine, grant.RoutineArguments, grant.RoutineKind = qualified, strings.Join(target.GetOverload(), ", "), strings.ToUpper(kind)
	default:
		objects := schemamodel.GrantObjectKinds()
		index := slices.IndexFunc(objects, func(object schemamodel.GrantObjectKind) bool { return strings.EqualFold(object.Kind, kind) })
		if index < 0 {
			return schemamodel.Grant{}, unsupported(fmt.Sprintf("a grant on a %q target", target.GetKind()))
		}
		grant.OnObject, grant.ObjectKind = qualified, objects[index].Kind
	}
	return grant, nil
}

func isRoutineKind(kind string) bool {
	return slices.Contains([]string{"function", "procedure", "routine"}, strings.ToLower(kind))
}

func sqlFileDeclarations(path string, declared *protocol.SQLDocumentDeclarations) sqlext.FileDeclarations {
	result := sqlext.FileDeclarations{Path: path}
	for _, object := range declared.GetObjects() {
		result.Objects = append(result.Objects, sqlext.Declaration{Kind: object.GetKind(), Name: object.GetName()})
	}
	for _, object := range declared.GetGuards() {
		result.Guards = append(result.Guards, sqlext.Declaration{Kind: object.GetKind(), Name: object.GetName()})
	}
	for _, statement := range declared.GetTables() {
		result.Tables = append(result.Tables, sqlext.TableStatement{Name: statement.GetName(), Drop: statement.GetDrop(), Guarded: statement.GetGuarded()})
	}
	return result
}

// sqlNativeObjects normalizes the provider's own objects of a reading with
// their owner, as every captured value is, and places each as a feature object
// of the engine.
func (t *target) sqlNativeObjects(ctx context.Context, envelopes []*protocol.Envelope) (schemaext.Objects, error) {
	var objects schemaext.Objects
	if len(envelopes) == 0 {
		return objects, nil
	}
	features := featureSet(t.engine.GetFeatures())
	request := &protocol.NormalizeValuesRequest{Engine: t.engine.GetId(), TargetFeatures: features}
	for i, envelope := range envelopes {
		request.Values = append(request.Values, &protocol.ValueInput{Id: strconv.Itoa(i), Representation: protocol.ValueInput_DESIRED, Value: proto.CloneOf(envelope)})
	}
	normalized, err := t.host.client.NormalizeValues(ctx, request)
	if err != nil {
		return schemaext.Objects{}, err
	}
	for _, value := range normalized.GetValues() {
		contract := t.host.models[modelKey{kind: schemaext.Kind(value.GetValue().GetKind()), representation: schemaext.Desired}]
		if contract == nil {
			return schemaext.Objects{}, fmt.Errorf("SQL schema model is not registered")
		}
		resolved, err := contract.resolved(modelWire{Format: 1, Engine: t.engine.GetId(), Features: features}, value)
		if err != nil {
			return schemaext.Objects{}, err
		}
		subject, err := sourceIdentity(value.GetValue().GetOwner(), value.GetNormalizedOwner())
		if err != nil {
			return schemaext.Objects{}, err
		}
		objects, err = objects.With(schemaext.Object{Ref: subject, Value: resolved, Targets: []string{t.engine.GetId()}})
		if err != nil {
			return schemaext.Objects{}, err
		}
	}
	return objects, nil
}

// sqlCoverage is the claim a reading makes about the provider's models. It
// keeps the knowledge the provider states, including what a header leaves
// undescribed, and records a model another engine of the provider cannot write
// in SQL as uninspected, as the claim of every other source format does.
func (t *target) sqlCoverage(source *protocol.ModelCoverage) (schemaext.Coverage, error) {
	h := t.host
	var kinds []schemaext.KindCoverage
	for _, record := range source.GetKinds() {
		if len(record.GetRegions()) != 0 {
			return schemaext.Coverage{}, unsupported("namespace regions in the coverage of a SQL schema reading")
		}
		kind := schemaext.Kind(record.GetModel().GetKind())
		contract := h.models[modelKey{kind: kind, representation: schemaext.Desired}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("SQL schema coverage model is not registered")
		}
		knowledge := boundKnowledge(record.GetKnowledge())
		if format := modelKnowledge(contract, protocol.SourceDocument_SQL); format.State != schemaext.Complete {
			knowledge = format
		}
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Desired, Version: 1, Definition: contract.signature}, Knowledge: schemaext.GlobalKnowledge(knowledge)})
	}
	var subjects []schemaext.SubjectCoverage
	for _, record := range source.GetSubjects() {
		subject, err := h.sourceSubject(t.engine.GetId(), record.GetSubject(), record.GetNormalizedSubject())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: schemaext.Kind(record.GetKind()), Subject: subject, Targets: []string{t.engine.GetId()},
			Knowledge: boundKnowledge(record.GetKnowledge())})
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, subjects)
}

func boundIdentities(subjects []*protocol.ComparisonIdentity) []objectidentity.ID {
	if len(subjects) == 0 {
		return nil
	}
	result := make([]objectidentity.ID, len(subjects))
	for i, subject := range subjects {
		result[i] = boundIdentity(subject)
	}
	return result
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
