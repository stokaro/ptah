package compare

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// Triggers compares trigger definitions between generated and database schemas.
func Triggers(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	TriggersWithDialect(generated, database, diff, "")
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
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	semantics := identifier.ForDialect(dialect)
	generatedTriggers := make(map[tableMemberKey]goschema.Trigger)
	for _, trigger := range generated.Triggers {
		trigger.Canonicalize()
		generatedTriggers[triggerKey(trigger.Table, trigger.Name, semantics)] = trigger
	}

	databaseTriggers := make(map[tableMemberKey]types.DBTrigger)
	for _, trigger := range database.Triggers {
		databaseTriggers[triggerKey(trigger.QualifiedTable(), trigger.Name, semantics)] = trigger
	}

	for key, trigger := range generatedTriggers {
		if _, exists := databaseTriggers[key]; !exists {
			diff.TriggersAdded = append(diff.TriggersAdded, difftypes.TriggerRef{
				TriggerName: trigger.Name,
				TableName:   trigger.Table,
			})
		}
	}
	for key, trigger := range databaseTriggers {
		if _, exists := generatedTriggers[key]; !exists {
			diff.TriggersRemoved = append(diff.TriggersRemoved, difftypes.TriggerRef{
				TriggerName: trigger.Name,
				TableName:   trigger.QualifiedTable(),
			})
		}
	}
	for key, generatedTrigger := range generatedTriggers {
		if databaseTrigger, exists := databaseTriggers[key]; exists {
			triggerDiff := TriggerDefinitions(generatedTrigger, databaseTrigger)
			if len(triggerDiff.Changes) > 0 {
				diff.TriggersModified = append(diff.TriggersModified, triggerDiff)
			}
		}
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

func triggerKey(tableName, triggerName string, semantics identifier.Semantics) tableMemberKey {
	return newTableMemberKey(tableName, triggerName, semantics)
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
func TriggerDefinitions(genTrigger goschema.Trigger, dbTrigger types.DBTrigger) difftypes.TriggerDiff {
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
