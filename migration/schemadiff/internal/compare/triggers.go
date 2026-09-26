package compare

import (
	"fmt"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/planner/objectlookup"
	"ptah.run/internal/triggerdef"
	"ptah.run/migration/schemadiff/difftypes"
)

// Triggers compares trigger definitions between generated and database schemas.
func Triggers(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	TriggersWithDialect(desired, current, diff, "")
}

// TriggersWithDialect compares triggers with the dialect's own idea of which
// schema an unqualified table belongs to.
//
// The dialect matters because the two sides spell the owning table
// differently: the desired schema carries the table's schema explicitly while
// the database reports it as empty wherever the engine treats it as implicit.
// Without the fill-in, every trigger on such a table read as removed and
// re-added (stokaro/ptah#1232).
func TriggersWithDialect(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	TriggersWithSemantics(desired, database, diff, identifier.ForDialect(dialect))
}

// TriggersWithSemantics compares trigger identity using the live database's
// resolved default schema and identifier rules.
func TriggersWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	TriggersWithSemanticsAndConditions(desired, database, diff, semantics, nil, nil)
}

// TriggersWithSemanticsAndConditions is [TriggersWithSemantics] with each
// declared WHEN condition as the server prints it, where a server was asked
// (see [config.CompareOptions.TriggerConditions]), and with the target's
// capabilities.
//
// A condition is compared only where caps says the read can report one,
// [capability.CatalogTriggerDefinitions]. Without it the read has every
// trigger without its condition, so comparing would plan the declared one
// again on every run and still leave the server's unread. Empty caps compare
// it, which is what a comparison with no target to ask assumes.
func TriggersWithSemanticsAndConditions(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	conditions map[string]config.TriggerCondition,
	caps capability.Capabilities,
) {
	readsConditions := len(caps) == 0 || caps.Has(capability.CatalogTriggerDefinitions)
	semantics = semantics.Normalize("")

	// Paired by the candidate set rather than by a map key.
	//
	// A key is one string. It folds a case difference and it resolves a default
	// schema the semantics know, and there it stops -- it has no tier saying
	// "an unqualified name and a qualified one are the same object when only
	// one candidate accepts it", because that tier needs the whole candidate
	// set and a key does not have one.
	//
	// MySQL is where that bit: its reader reports the database name for
	// everything, a Go annotation leaves it bare, and the database name is
	// whatever the connection points at, so no static default schema can join
	// them. A trigger on `orders` and the same trigger read back as
	// `app.orders` came out as one addition and one removal, and the plan
	// dropped and recreated it on every run -- succeeding each time, and
	// leaving a window with no trigger on the table (stokaro/ptah#2436).
	//
	// Views never had it: they resolve through objectlookup, which applies the
	// three tiers. This applies the same ones.
	matched, paired := pairTriggers(desired, database, semantics)

	for position, declared := range desired.Triggers {
		canonical := declared
		canonical.Canonicalize()

		index := matched[position]
		if index < 0 {
			diff.TriggersAdded = append(diff.TriggersAdded, difftypes.TriggerRef{
				TriggerName: canonical.Name,
				TableName:   canonical.Table,
				Desired:     declared,
			})
			continue
		}
		if resolved := conditions[exprkey.Trigger(semantics, declared.Table, declared.Name)]; resolved.Resolved {
			canonical.When = resolved.Condition
		}
		triggerDiff := TriggerDefinitions(canonical, database.Triggers[index], semantics)
		if !readsConditions {
			delete(triggerDiff.Changes, "when")
		}
		if len(triggerDiff.Changes) > 0 {
			triggerDiff.Desired = declared
			diff.TriggersModified = append(diff.TriggersModified, triggerDiff)
		}
	}
	for index, trigger := range database.Triggers {
		if paired[index] {
			continue
		}
		removal := difftypes.TriggerRef{TriggerName: trigger.Name, TableName: trigger.QualifiedTable()}
		// The catalog reports every function a trigger runs, the generated
		// one included; the removal carries it only when it is declared on its
		// own, which is what a DROP must leave in place.
		if (schemamodel.Trigger{Name: trigger.Name, Table: removal.TableName}).RunsDeclaredFunction(trigger.ExecuteFunction) {
			removal.ExecuteFunction = trigger.ExecuteFunction
		}
		diff.TriggersRemoved = append(diff.TriggersRemoved, removal)
	}

	sortTriggerRefs(diff.TriggersAdded)
	sortTriggerRefs(diff.TriggersRemoved)
	sort.Slice(diff.TriggersModified, func(i, j int) bool {
		if diff.TriggersModified[i].TableName == diff.TriggersModified[j].TableName {
			return diff.TriggersModified[i].TriggerName < diff.TriggersModified[j].TriggerName
		}
		return diff.TriggersModified[i].TableName < diff.TriggersModified[j].TableName
	})
}

// matchingDatabaseTrigger returns the index of the database trigger that is the
// same object as a declared one, or -1.
//
// The trigger's own name is folded first and the table half is put through the
// tiers, which is how objectlookup.Trigger reads the same pair: a trigger is
// identified by its own name plus the table it hangs on, and it is the table
// that carries the schema.
//
// A trigger already paired is not offered again. Two declarations that both
// accept one database trigger name one object between them, and the second is
// an addition rather than a second claim on the first -- iterating the declared
// slice in order makes which is which deterministic.
// exactDatabaseTrigger returns the index of a database trigger spelled exactly
// as the declaration spells it, or -1.
//
// The first of objectlookup's tiers, applied to every declaration before any of
// them reaches the later ones. A schema that spells a name the way the reader
// does is never re-interpreted, and doing that pass first is what makes the
// answer independent of the order the declarations happen to be in.
func exactDatabaseTrigger(
	triggers []catalog.Trigger,
	paired []bool,
	declared schemamodel.Trigger,
	semantics identifier.Semantics,
) int {
	for index, trigger := range triggers {
		if paired[index] {
			continue
		}
		if semantics.TableIdentityKey(trigger.Name) != semantics.TableIdentityKey(declared.Name) {
			continue
		}
		if trigger.QualifiedTable() == declared.Table {
			return index
		}
	}
	return -1
}

func matchingDatabaseTrigger(
	triggers []catalog.Trigger,
	paired []bool,
	declared schemamodel.Trigger,
	semantics identifier.Semantics,
) int {
	wanted := semantics.TableIdentityKey(declared.Name)
	candidates := make([]catalog.Trigger, 0, len(triggers))
	indexes := make([]int, 0, len(triggers))
	for index, trigger := range triggers {
		if paired[index] || semantics.TableIdentityKey(trigger.Name) != wanted {
			continue
		}
		candidates = append(candidates, trigger)
		indexes = append(indexes, index)
	}

	match := objectlookup.Find(candidates, declared.Table, semantics,
		func(trigger catalog.Trigger) string { return trigger.QualifiedTable() })
	if match == nil {
		return -1
	}
	for position := range candidates {
		if &candidates[position] == match {
			return indexes[position]
		}
	}
	return -1
}

func sortTriggerRefs(refs []difftypes.TriggerRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].TableName == refs[j].TableName {
			return refs[i].TriggerName < refs[j].TriggerName
		}
		return refs[i].TableName < refs[j].TableName
	})
}

// TriggerDefinitions performs detailed comparison between generated and database trigger definitions.
func TriggerDefinitions(
	genTrigger schemamodel.Trigger,
	dbTrigger catalog.Trigger,
	semantics identifier.Semantics,
) difftypes.TriggerDiff {
	genTrigger.Canonicalize()

	triggerDiff := difftypes.TriggerDiff{
		TriggerName: genTrigger.Name,
		TableName:   genTrigger.Table,
		Changes:     make(map[string]string),
	}

	if genTrigger.Timing != strings.ToUpper(dbTrigger.Timing) {
		triggerDiff.Changes["timing"] = fmt.Sprintf("%s -> %s", dbTrigger.Timing, genTrigger.Timing)
	}
	if !sameTriggerEvent(semantics, genTrigger.Event, dbTrigger.Event) {
		triggerDiff.Changes["event"] = fmt.Sprintf("%s -> %s", dbTrigger.Event, genTrigger.Event)
	}
	dbForEach := strings.ToUpper(strings.TrimSpace(dbTrigger.ForEach))
	if dbForEach == "" {
		dbForEach = "ROW"
	}
	if genTrigger.ForEach != dbForEach {
		triggerDiff.Changes["for"] = fmt.Sprintf("%s -> %s", dbForEach, genTrigger.ForEach)
	}
	if !sameTriggerCondition(genTrigger.When, dbTrigger.When) {
		triggerDiff.Changes["when"] = fmt.Sprintf("%s -> %s", describeCondition(dbTrigger.When), describeCondition(genTrigger.When))
	}
	// A transition table's name is compared exactly: the renderer quotes it,
	// so the server keeps the case it was declared in, and `OldRows` and
	// `oldrows` are two names the trigger function would have to spell apart.
	if genTrigger.OldTable != strings.TrimSpace(dbTrigger.OldTable) ||
		genTrigger.NewTable != strings.TrimSpace(dbTrigger.NewTable) {
		triggerDiff.Changes["referencing"] = fmt.Sprintf("old=%s new=%s -> old=%s new=%s",
			dbTrigger.OldTable, dbTrigger.NewTable, genTrigger.OldTable, genTrigger.NewTable)
	}

	// A trigger either carries a body Ptah owns or names a function somebody
	// else wrote, and the two are compared differently: the body is a copy of
	// the function's source, so holding an external reference against it always
	// differs. Comparing them made a declaration that names an existing
	// function plan CREATE OR REPLACE TRIGGER on every run, over a database
	// that already matched it (stokaro/ptah#2210).
	//
	dbExecuteFunction := strings.TrimSpace(dbTrigger.ExecuteFunction)
	// BOTH sides, not either. A desired side carrying a body against a database
	// side naming an external function is the HCL surface's limitation rather
	// than a rebinding request -- that surface cannot name a function, so it
	// always describes the body -- and reading it as a change would plan one on
	// every run for every trigger a live database inspected through HCL.
	genExecuteFunction := strings.TrimSpace(genTrigger.ExecuteFunction)
	if genExecuteFunction != "" && dbExecuteFunction != "" {
		if !sameExecuteFunction(genExecuteFunction, dbExecuteFunction) {
			triggerDiff.Changes["function"] = fmt.Sprintf("%s -> %s", dbExecuteFunction, genExecuteFunction)
		}
		return triggerDiff
	}

	genBody := normalizeTriggerBody(genTrigger.Body)
	dbBody := normalizeTriggerBody(dbTrigger.Body)
	if genBody != dbBody {
		triggerDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbTrigger.Body), strings.TrimSpace(genTrigger.Body))
	}

	return triggerDiff
}

// sameTriggerEvent reports whether two event lists name the same events. The
// members are compared in PostgreSQL's order, and each UPDATE column is folded
// before its identity key is taken: an unquoted name to lower case and a quoted
// one to what is inside the quotes. PostgreSQL reports a column the way
// quote_ident prints it, `b` or `"Total"`, and a declaration may write `B` for
// the first.
func sameTriggerEvent(semantics identifier.Semantics, desired, database string) bool {
	column := func(name string) string {
		return semantics.ColumnIdentityKey(foldTriggerColumn(name))
	}
	return triggerdef.Canonical(desired, column) == triggerdef.Canonical(database, column)
}

func foldTriggerColumn(name string) string {
	if unquoted, quoted := strings.CutPrefix(name, `"`); quoted {
		return strings.ReplaceAll(strings.TrimSuffix(unquoted, `"`), `""`, `"`)
	}
	return strings.ToLower(name)
}

// sameTriggerCondition reports whether two WHEN conditions are the same.
//
// The declared side arrives here in the server's own spelling where a server
// was asked, and the two are then compared as text after the folding a CHECK
// gets. Without a server the folding is all there is: it drops case, spaces
// and one pair of outer parentheses, which makes a single comparison match its
// read-back and leaves a compound condition, which the server parenthesizes
// operand by operand, reported as changed.
//
// CockroachDB cannot be asked -- the probe needs a temporary table, which it
// refuses unless an experimental setting is on -- and it prints each constant
// with a type annotation: `WHEN ((NEW).a > 0)` reads back as `(new).a >
// 0:::INT8` on v26.2.7 and v26.3.1. The annotation asserts the type the
// constant already has, so it is dropped from both sides before they are
// folded; without that, every such trigger is replaced on every plan.
func sameTriggerCondition(declared, observed string) bool {
	return normalizeCheckExpression(withoutTypeAnnotations(declared)) ==
		normalizeCheckExpression(withoutTypeAnnotations(observed))
}

// withoutTypeAnnotations removes each CockroachDB type annotation, `:::` and
// the type name after it, outside string literals and quoted identifiers.
func withoutTypeAnnotations(expression string) string {
	if !strings.Contains(expression, ":::") {
		return expression
	}
	var out strings.Builder
	var quote byte
	for i := 0; i < len(expression); i++ {
		c := expression[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case strings.HasPrefix(expression[i:], ":::"):
			i += 3
			for i < len(expression) && isTypeNameByte(expression[i]) {
				i++
			}
			i--
			continue
		}
		out.WriteByte(c)
	}
	return out.String()
}

// isTypeNameByte reports whether c can appear in a type name CockroachDB
// writes after `:::`, such as INT8, DECIMAL or STRING[].
func isTypeNameByte(c byte) bool {
	return c == '_' || c == '[' || c == ']' ||
		('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// describeCondition names an absent condition in a change description.
func describeCondition(condition string) string {
	if strings.TrimSpace(condition) == "" {
		return "(none)"
	}
	return strings.TrimSpace(condition)
}

func normalizeTriggerBody(body string) string {
	body = normalizeSQLBodyPreservingQualifiers(body, "")
	body = strings.TrimPrefix(body, "begin ")
	body = strings.TrimPrefix(body, "begin")
	body = strings.TrimSpace(body)
	body = strings.TrimSuffix(body, " end")
	body = strings.TrimSuffix(body, "end")
	body = strings.TrimSpace(body)
	body = strings.TrimSuffix(body, ";")
	return strings.TrimSpace(body)
}

// sameExecuteFunction reports whether two spellings name one function.
//
// The two sides qualify differently. A declaration names the function the way
// its author wrote it -- `app.touch` in a SQL file, or the qualified name an
// HCL reference resolves to -- while the catalog reports pg_proc.proname, which
// carries no schema. Compared as text, a trigger nobody had touched was planned
// for replacement on every run (stokaro/ptah#3113).
//
// The comparison uses the more specific spelling BOTH sides carry. Two
// qualified names are compared whole, so `app.touch` and `other.touch` stay two
// functions; a bare name on either side compares against the other's bare part,
// because that side simply does not say which schema.
func sameExecuteFunction(declared, observed string) bool {
	declaredSchema, declaredName := splitExecuteFunction(declared)
	observedSchema, observedName := splitExecuteFunction(observed)
	if !strings.EqualFold(declaredName, observedName) {
		return false
	}
	if declaredSchema == "" || observedSchema == "" {
		return true
	}
	return strings.EqualFold(declaredSchema, observedSchema)
}

// splitExecuteFunction separates a function reference into its schema and name.
func splitExecuteFunction(reference string) (schema, name string) {
	trimmed := strings.TrimSpace(reference)
	if qualifier, bare, qualified := strings.Cut(trimmed, "."); qualified {
		return qualifier, bare
	}
	return "", trimmed
}

// pairTriggers matches each declared trigger to the database trigger it is,
// and reports which database triggers were claimed. matched holds the index
// into database.Triggers for each declared trigger, or -1.
//
// It is the one pairing every comparison of triggers uses, the definition and
// the comment alike, so a trigger the definition comparison treats as the same
// object is the one whose comment is compared.
func pairTriggers(
	desired *schemamodel.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) (matched []int, paired []bool) {
	// Two passes, exact spellings first.
	//
	// A declaration that names the table exactly as the reader does gets that
	// trigger before one relying on a tier is offered it. Without the ordering,
	// a schema spelling the same trigger both ways would hand the database's
	// one to whichever declaration came first in the slice -- the coin toss
	// objectlookup declines to make elsewhere.
	paired = make([]bool, len(database.Triggers))
	matched = make([]int, len(desired.Triggers))
	for position := range matched {
		matched[position] = -1
	}
	for position, declared := range desired.Triggers {
		matched[position] = exactDatabaseTrigger(database.Triggers, paired, declared, semantics)
		if matched[position] >= 0 {
			paired[matched[position]] = true
		}
	}
	for position, declared := range desired.Triggers {
		if matched[position] >= 0 {
			continue
		}
		matched[position] = matchingDatabaseTrigger(database.Triggers, paired, declared, semantics)
		if matched[position] >= 0 {
			paired[matched[position]] = true
		}
	}

	return matched, paired
}
