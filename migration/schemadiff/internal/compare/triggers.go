package compare

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
	// Two passes, exact spellings first.
	//
	// A declaration that names the table exactly as the reader does gets that
	// trigger before one relying on a tier is offered it. Without the ordering,
	// a schema spelling the same trigger both ways would hand the database's
	// one to whichever declaration came first in the slice -- the coin toss
	// objectlookup declines to make elsewhere.
	paired := make([]bool, len(database.Triggers))
	matched := make([]int, len(desired.Triggers))
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
		triggerDiff := TriggerDefinitions(canonical, database.Triggers[index])
		if len(triggerDiff.Changes) > 0 {
			triggerDiff.Desired = declared
			diff.TriggersModified = append(diff.TriggersModified, triggerDiff)
		}
	}
	for index, trigger := range database.Triggers {
		if paired[index] {
			continue
		}
		diff.TriggersRemoved = append(diff.TriggersRemoved, difftypes.TriggerRef{
			TriggerName: trigger.Name,
			TableName:   trigger.QualifiedTable(),
		})
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
func TriggerDefinitions(genTrigger schemamodel.Trigger, dbTrigger catalog.Trigger) difftypes.TriggerDiff {
	genTrigger.Canonicalize()

	triggerDiff := difftypes.TriggerDiff{
		TriggerName: genTrigger.Name,
		TableName:   genTrigger.Table,
		Changes:     make(map[string]string),
	}

	if genTrigger.Timing != strings.ToUpper(dbTrigger.Timing) {
		triggerDiff.Changes["timing"] = fmt.Sprintf("%s -> %s", dbTrigger.Timing, genTrigger.Timing)
	}
	if genTrigger.Event != strings.ToUpper(dbTrigger.Event) {
		triggerDiff.Changes["event"] = fmt.Sprintf("%s -> %s", dbTrigger.Event, genTrigger.Event)
	}
	dbForEach := strings.ToUpper(strings.TrimSpace(dbTrigger.ForEach))
	if dbForEach == "" {
		dbForEach = "ROW"
	}
	if genTrigger.ForEach != dbForEach {
		triggerDiff.Changes["for"] = fmt.Sprintf("%s -> %s", dbForEach, genTrigger.ForEach)
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
		if !strings.EqualFold(genExecuteFunction, dbExecuteFunction) {
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
