// Package identifiervalidation validates generated identifiers before schema
// comparison or migration planning.
package identifiervalidation

import (
	"fmt"
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/offlinenames"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
)

// ValidateCoverage rejects catalog-resolved snapshots that omit a candidate.
func ValidateCoverage(
	semantics identifier.Semantics,
	names []string,
) error {
	for _, name := range names {
		if name != "" && !semantics.Resolves(name) {
			return fmt.Errorf(
				"%w: identifier semantics snapshot does not resolve %q",
				ptaherr.ErrInvalidSchemaDiff,
				name,
			)
		}
	}
	return nil
}

// ValidateTarget rejects unresolved or colliding target identifiers.
func ValidateTarget(
	desired *schemamodel.Database,
	dialect string,
	semantics identifier.Semantics,
) error {
	if desired == nil {
		return nil
	}
	if err := ValidateCoverage(
		semantics,
		targetIdentifierNames(desired, semantics.DefaultSchema),
	); err != nil {
		return err
	}
	if err := validateTablesAndColumns(desired, semantics); err != nil {
		return err
	}
	if err := validateSchemaObjects(desired, semantics); err != nil {
		return err
	}
	if err := validateSharedNamespace(desired, dialect, semantics); err != nil {
		return err
	}
	if err := validateRoles(desired, dialect, semantics); err != nil {
		return err
	}
	return indexscope.ValidateDeclared(dialect, semantics, difftypes.IndexDeclarationsOf(desired))
}

// validateRoles refuses two declared roles the target may read as one name,
// where it compares a role name under its catalog's rules
// ([offlinenames.RolesByCatalog]). The comparison pairs each declared role
// with the role the database holds under those rules, so of two such
// declarations it compares one and the plan creates the other: measured on
// SQL Server 2025 under a case-insensitive collation, the roles App and APP
// are one role, and CREATE ROLE [APP] beside App fails with error 15023.
// Two roles spelled alike are one declaration.
func validateRoles(desired *schemamodel.Database, dialect string, semantics identifier.Semantics) error {
	if !offlinenames.RolesByCatalog(dialect) {
		return nil
	}
	conflict := indexscope.ConflictSemantics(semantics)
	roles := make([]spelledName, 0, len(desired.Roles))
	for _, role := range desired.Roles {
		key := conflict.TableConflictKey(role.Name)
		roles = append(roles, spelledName{spelling: role.Name, namesake: Namesake{Key: key, NameKey: key}})
	}
	if pair, found := firstConflict(roles); found {
		return conflictError("roles", pair, semantics)
	}
	return nil
}

// ObjectNames lists the names of the views, materialized views, routines,
// sequences and triggers desired declares, with the table each trigger is on,
// and the names of the constraints it declares ([DeclaredConstraints]). A
// schema-qualified name contributes its schema and its name, each without its
// quotes. A nil desired lists nothing.
//
// It is the one list of these names. The comparison asks a server how it
// compares them, and [ValidateTarget] checks that the server was asked about
// every one, so the two cannot disagree about which names those are.
func ObjectNames(desired *schemamodel.Database) []string {
	if desired == nil {
		return nil
	}
	var names []string
	for _, view := range desired.Views {
		names = appendQualifiedIdentifier(names, view.Name)
	}
	for _, view := range desired.MaterializedViews {
		names = appendQualifiedIdentifier(names, view.Name)
	}
	for _, function := range desired.Functions {
		names = appendQualifiedIdentifier(names, function.Name)
	}
	for _, sequence := range desired.Sequences {
		if sequence.Schema != "" {
			names = append(names, sequence.Schema)
		}
		names = appendQualifiedIdentifier(names, sequence.Name)
	}
	for _, trigger := range desired.Triggers {
		names = appendQualifiedIdentifier(names, trigger.Table)
		names = appendQualifiedIdentifier(names, trigger.Name)
	}
	for _, constraint := range DeclaredConstraints(desired) {
		names = append(names, constraint.Name)
	}
	return names
}

func targetIdentifierNames(
	desired *schemamodel.Database,
	defaultSchema string,
) []string {
	names := []string{defaultSchema}
	for _, field := range desired.Fields {
		names = append(names, field.Name)
	}
	for _, table := range desired.Tables {
		names = append(names, table.Schema, table.Name)
		for _, field := range generatedschema.FieldsForTable(desired, table) {
			names = append(names, field.Name)
		}
	}
	for _, index := range desired.Indexes {
		names = append(names, index.Name)
		names = appendQualifiedIdentifier(names, index.TableName)
		names = append(names, index.Fields...)
		names = append(names, index.IncludeColumns...)
		for _, part := range index.Parts {
			names = append(names, part.Name)
		}
	}
	return append(names, ObjectNames(desired)...)
}

func appendQualifiedIdentifier(names []string, value string) []string {
	ref, ok := tableref.Parse(value)
	if !ok {
		return append(names, value)
	}
	if !ref.Qualified {
		return append(names, ref.Name)
	}
	return append(names, ref.Schema, ref.Name)
}

func validateTablesAndColumns(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) error {
	rules := semantics
	semantics = indexscope.ConflictSemantics(semantics)
	tables := make([]spelledName, 0, len(desired.Tables))
	for _, table := range desired.Tables {
		tables = append(tables, spelledName{spelling: table.QualifiedName(), namesake: NamesakeOf(table.QualifiedName(), semantics)})
	}
	if pair, conflict := firstConflict(tables); conflict {
		return conflictError("tables", pair, rules)
	}

	for _, table := range desired.Tables {
		if previous, current, conflict := conflictingColumn(semantics, desired, table); conflict {
			return fmt.Errorf(
				"%w: target columns %s.%s and %s.%s may have the same catalog identity",
				ptaherr.ErrInvalidSchemaDiff,
				table.QualifiedName(),
				previous,
				table.QualifiedName(),
				current,
			)
		}
	}
	return nil
}

// unresolvedTableKey is the part every conflict key of a table name the target
// cannot place contains, whichever of the schema and the name is unresolved.
var unresolvedTableKey = identifier.ComparisonCatalogUnknown.ConflictKey("")

// spelledName is a declared name as a conflict check reads it: spelling
// says which declaration it is, and namesake is how it is compared.
type spelledName struct {
	spelling string
	namesake Namesake
}

// conflictPair is two declarations of one kind the target may read as one
// object, in declaration order. unresolved is true when one of them has no
// equivalence class under the rules the check read, so the pair is a pair
// because nothing said the two are apart.
type conflictPair struct {
	previous, current string
	unresolved        bool
}

// firstConflict returns the first pair of names the target cannot keep
// apart, in declaration order. Two names spelled alike are one declaration,
// not a pair.
//
// A name that has no equivalence class is compared against every name
// already seen, and a later name against every such name, for the reason
// [conflictingColumn] gives: under an accent-insensitive collation `örders`
// is the table `orders`, and the two conflict keys are different. Each such
// pair is a pair only where [Namesake.MayBe] says the two may be one name,
// so a name in one schema is never paired with a name in another.
func firstConflict(names []spelledName) (conflictPair, bool) {
	byKey := make(map[string]string, len(names))
	var seen, unresolved []spelledName
	for _, name := range names {
		earlier := unresolved
		if name.namesake.Unresolved() {
			earlier = seen
		}
		for _, other := range earlier {
			if other.spelling != name.spelling && other.namesake.MayBe(name.namesake) {
				return conflictPair{previous: other.spelling, current: name.spelling, unresolved: true}, true
			}
		}
		seen = append(seen, name)
		if name.namesake.Unresolved() {
			unresolved = append(unresolved, name)
			continue
		}
		if other, exists := byKey[name.namesake.Key]; exists && other != name.spelling {
			return conflictPair{previous: other, current: name.spelling}, true
		}
		byKey[name.namesake.Key] = name.spelling
	}
	return conflictPair{}, false
}

// validateSchemaObjects refuses two declared views, materialized views,
// routines, sequences or triggers of one kind that the target may read as
// one object.
//
// The comparison keys each of these kinds by the table rule, so two such
// declarations share one key, and one of them is compared while the other is
// dropped in silence. Measured on SQL Server 2025 under a case-insensitive
// collation, the views MixedView and MIXEDVIEW are one view: a plan created
// one of them, and the next comparison found nothing to do.
//
// A routine declared twice under one name is two overloads, which is how
// PostgreSQL holds them, so two routines are one declaration when they are
// in one schema and spelled alike. Triggers are named per table, as the
// comparison names them.
func validateSchemaObjects(desired *schemamodel.Database, semantics identifier.Semantics) error {
	rules := semantics
	semantics = indexscope.ConflictSemantics(semantics)
	kinds := []struct {
		noun  string
		names []string
	}{
		{noun: "views", names: nameList(desired.Views, func(view schemamodel.View) string { return view.Name })},
		{noun: "materialized views", names: nameList(desired.MaterializedViews, func(view schemamodel.MaterializedView) string { return view.Name })},
		{noun: "routines", names: nameList(desired.Functions, func(function schemamodel.Function) string { return function.Name })},
		{noun: "sequences", names: nameList(desired.Sequences, schemamodel.Sequence.QualifiedName)},
	}
	for _, kind := range kinds {
		spelled := make([]spelledName, 0, len(kind.names))
		for _, name := range kind.names {
			spelled = append(spelled, spelledName{spelling: schemaSpelling(name, semantics), namesake: NamesakeOf(name, semantics)})
		}
		if pair, conflict := firstConflict(spelled); conflict {
			return conflictError(kind.noun, pair, rules)
		}
	}
	triggers := make([]spelledName, 0, len(desired.Triggers))
	for _, trigger := range desired.Triggers {
		table, name := semantics.QualifiedTableConflictKey(trigger.Table), semantics.TableConflictKey(trigger.Name)
		triggers = append(triggers, spelledName{
			spelling: schemaSpelling(trigger.Table, semantics) + " trigger " + trigger.Name,
			namesake: Namesake{Key: table + "\x00" + name, SchemaKey: table, NameKey: name},
		})
	}
	if pair, conflict := firstConflict(triggers); conflict {
		return conflictError("triggers", pair, rules)
	}
	return nil
}

// kindedName is a declared object as [validateSharedNamespace] reads it: the
// noun a message names it by, which is also its kind, so a function and a
// procedure are two kinds, and for a constraint or a trigger the table it
// belongs to.
type kindedName struct {
	noun string
	host string
	spelledName
}

// phrase is how a message names the object: `view dbo.Report`, or
// `constraint dbo.Report on dbo.t1`.
func (k kindedName) phrase() string {
	if k.host == "" {
		return k.noun + " " + k.spelling
	}
	return k.noun + " " + k.spelling + " on " + k.host
}

// validateSharedNamespace refuses two declared objects that the target keeps
// in one namespace in a schema
// ([descriptor.Comparison.SharedObjectNamespace]) whose names it may read as
// one name, spelled alike or not. Measured on SQL Server 2025: CREATE OR
// ALTER PROCEDURE [dbo].[REPORT] beside the view dbo.Report fails with error
// 2010 under a case-insensitive collation, and so does [dbo].[Report] under
// any collation (stokaro/ptah#6741). So does the procedure dbo.x beside the
// function dbo.x. A constraint and a trigger are in the schema of their
// table: CREATE VIEW [dbo].[REPORT] beside the primary key Report of dbo.t1
// fails with error 2714, and so does CREATE TRIGGER [Audit] beside the
// procedure dbo.AUDIT (stokaro/ptah#6767). Two constraints or two triggers of
// two tables are two objects there too: measured on SQL Server 2022, the
// primary key pk of dbo.t2 beside the primary key pk of dbo.t1 fails with
// error 2714. Any other two objects of one kind are [validateSchemaObjects]'s
// to check ([Apart]).
//
// [descriptor.Comparison.SharedObjectNamespace]: ptah.run/core/platform/descriptor.Comparison
func validateSharedNamespace(desired *schemamodel.Database, dialect string, semantics identifier.Semantics) error {
	described, _ := platform.Lookup(dialect)
	namespace := described.Comparison.SharedObjectNamespace
	rules := semantics
	semantics = indexscope.ConflictSemantics(semantics)
	var objects []kindedName
	add := func(shared bool, noun, name string) {
		if shared {
			objects = append(objects, kindedName{noun: noun, spelledName: spelledName{
				spelling: schemaSpelling(name, semantics), namesake: NamesakeOf(name, semantics),
			}})
		}
	}
	hosted := func(shared bool, noun string, object Hosted) {
		add(shared, noun, object.QualifiedName(semantics.DefaultSchema))
		if shared {
			objects[len(objects)-1].host = schemaSpelling(object.Table, semantics)
		}
	}
	for _, table := range desired.Tables {
		add(namespace.Tables, "table", table.QualifiedName())
	}
	for _, view := range desired.Views {
		add(namespace.Views, "view", view.Name)
	}
	for _, view := range desired.MaterializedViews {
		add(namespace.MaterializedViews, "materialized view", view.Name)
	}
	for _, function := range desired.Functions {
		noun := schemamodel.FunctionKindFunction
		if function.IsProcedure() {
			noun = schemamodel.FunctionKindProcedure
		}
		add(namespace.Routines, noun, function.Name)
	}
	for _, sequence := range desired.Sequences {
		add(namespace.Sequences, "sequence", sequence.QualifiedName())
	}
	for _, constraint := range DeclaredConstraints(desired) {
		hosted(namespace.Constraints, "constraint", constraint)
	}
	for _, trigger := range desired.Triggers {
		hosted(namespace.Triggers, "trigger", HostedTrigger(trigger.Name, trigger.Table))
	}
	byKey := make(map[string][]kindedName, len(objects))
	var seen, unresolved []kindedName
	for _, object := range objects {
		earlier := append(slices.Clone(byKey[object.namesake.Key]), unresolved...)
		if object.namesake.Unresolved() {
			earlier = seen
		}
		for _, other := range earlier {
			if Apart(other.noun, other.host, object.noun, object.host) && other.namesake.MayBe(object.namesake) {
				return sharedNamespaceError(other, object, dialect, rules)
			}
		}
		seen = append(seen, object)
		if object.namesake.Unresolved() {
			unresolved = append(unresolved, object)
			continue
		}
		byKey[object.namesake.Key] = append(byKey[object.namesake.Key], object)
	}
	return nil
}

// sharedNamespaceError names two declared objects that dialect keeps in one
// namespace, which it may read as one name, or which are one
// name where they are spelled alike; see [conflictError] for the offline
// wording.
func sharedNamespaceError(previous, current kindedName, dialect string, rules identifier.Semantics) error {
	pair := fmt.Sprintf("target %s and %s", previous.phrase(), current.phrase())
	namespace := NamespaceClause(previous.noun, current.noun, dialect)
	if previous.spelling == current.spelling {
		return fmt.Errorf("%w: %s are one name, and %s", ptaherr.ErrInvalidSchemaDiff, pair, namespace)
	}
	if offlinenames.Unanswered(rules) && (previous.namesake.Unresolved() || current.namesake.Unresolved()) {
		return fmt.Errorf(
			"%w: without a connection Ptah cannot tell whether %s are one name under the database collation, and %s",
			ptaherr.ErrInvalidSchemaDiff, pair, namespace,
		)
	}
	return fmt.Errorf("%w: %s may have the same catalog identity, and %s", ptaherr.ErrInvalidSchemaDiff, pair, namespace)
}

// schemaSpelling is name with the schema it is in written out, so that
// `orders`, `dbo.orders` and `[dbo].[orders]` read as one spelling on a
// target whose default schema is dbo.
func schemaSpelling(name string, semantics identifier.Semantics) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	if !ref.Qualified {
		ref.Schema = semantics.DefaultSchema
	}
	return tableref.Canonical(ref.Schema, ref.Name)
}

// nameList lists the name of every element.
func nameList[T any](elements []T, name func(T) string) []string {
	listed := make([]string, 0, len(elements))
	for _, element := range elements {
		listed = append(listed, name(element))
	}
	return listed
}

// conflictError names two declarations of one kind the target may read as
// one object. Where rules are the offline ones of a target whose catalog
// compares its names (see [offlinenames.Unanswered]), a pair that is a pair
// only because one of its names has no class says that Ptah cannot tell,
// since no rule put the two names together.
func conflictError(kind string, pair conflictPair, rules identifier.Semantics) error {
	if offlinenames.Unanswered(rules) && pair.unresolved {
		return fmt.Errorf(
			"%w: without a connection Ptah cannot tell whether target %s %s and %s are one name under the database collation",
			ptaherr.ErrInvalidSchemaDiff, kind, pair.previous, pair.current,
		)
	}
	return fmt.Errorf(
		"%w: target %s %s and %s may have the same catalog identity",
		ptaherr.ErrInvalidSchemaDiff, kind, pair.previous, pair.current,
	)
}

// conflictingColumn returns the first pair of column names one table cannot
// keep apart, in declaration order.
//
// A name whose equivalence class the target cannot resolve is compared against
// every column already seen rather than against its own conflict bucket. That
// is not a refinement: MySQL treats `İ` and ASCII `i` as one column, and one
// side of that pair is ASCII, so bucketing the unresolved names together
// leaves the two in different buckets and misses the collision the server
// actually reports (stokaro/ptah#2771).
func conflictingColumn(
	semantics identifier.Semantics,
	desired *schemamodel.Database,
	table schemamodel.Table,
) (previous, current string, conflict bool) {
	columns := make(map[string]string)
	seen := make([]string, 0, len(desired.Fields))
	unresolved := ""
	for _, field := range generatedschema.FieldsForTable(desired, table) {
		if semantics.ColumnConflictUnresolved(field.Name) {
			if len(seen) > 0 {
				return seen[0], field.Name, true
			}
			unresolved = field.Name
			seen = append(seen, field.Name)
			continue
		}
		if unresolved != "" {
			return unresolved, field.Name, true
		}
		conflictKey := semantics.ColumnConflictKey(field.Name)
		if earlier, exists := columns[conflictKey]; exists && earlier != field.Name {
			return earlier, field.Name, true
		}
		columns[conflictKey] = field.Name
		seen = append(seen, field.Name)
	}
	return "", "", false
}
