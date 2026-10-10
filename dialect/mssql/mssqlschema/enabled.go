package mssqlschema

// NamedPolicy is a policy with its schema-qualified name, as a check across
// policies needs it.
type NamedPolicy struct {
	Name   ObjectName
	Policy *DesiredSecurityPolicy
}

// TableConflict is a table two enabled policies bind: SQL Server enables one
// policy with a predicate on a table and refuses another with Msg 33264,
// whichever of the two is created or enabled second.
type TableConflict struct {
	Table         ObjectName
	First, Second ObjectName
}

// EnabledTableConflicts returns each table that more than one enabled policy
// binds, in policy order and then canonical predicate order, naming the first
// policy that bound it and each later one. A disabled policy binds nothing for
// this rule. Tables are matched by [ObjectName.ConflictKey], the spellings a
// case-insensitive database treats as one table.
func EnabledTableConflicts(policies []NamedPolicy) []TableConflict {
	bound := make(map[ObjectName]ObjectName)
	var conflicts []TableConflict
	for _, named := range policies {
		if named.Policy == nil {
			continue
		}
		if enabled, _, _ := named.Policy.Resolved(); !enabled {
			continue
		}
		seen := make(map[ObjectName]bool)
		for _, predicate := range SortedPredicates(named.Policy.Predicates) {
			key := predicate.Table.ConflictKey()
			if seen[key] {
				continue
			}
			seen[key] = true
			first, found := bound[key]
			if !found {
				bound[key] = named.Name
				continue
			}
			conflicts = append(conflicts, TableConflict{Table: predicate.Table, First: first, Second: named.Name})
		}
	}
	return conflicts
}
