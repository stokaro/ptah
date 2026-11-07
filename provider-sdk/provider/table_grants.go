package provider

// TableGrantsFeature enables PlanningTable.current_grants: the host sends a
// planning service the grants each held table holds, so a plan that
// replaces the table can give the new one its grants. A two-way feature:
// the provider lists it, and so does each engine whose planning reads
// them. Contract 25.33.
const TableGrantsFeature = "planning.table-grants"
