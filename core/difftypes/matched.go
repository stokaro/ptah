package difftypes

// MatchedCounts counts the pairs a comparison matched, one field per kind of
// object it pairs one by one. A pair is an object the desired schema declares
// and an object the database holds that the comparison takes to be the same
// object, whether the two differ or not. An index the plan renames is one
// pair.
//
// An object the comparison drops and creates again, as it does an index or a
// constraint whose definition changed, is not a pair: it is counted as the
// removal and the addition the plan carries out. An object the comparison
// leaves out is counted nowhere: a table an ignore rule excludes, a removal in
// a schema the read did not describe, an addition it withholds as undecided.
// So the pairs and the additions of a kind are what the desired schema holds
// of it, the pairs and the removals are what the database holds of it, and two
// sides with no difference count the same.
//
// Columns are paired only in the tables both sides hold, and enum values only
// in the enums both sides hold: a table or an enum that one side alone holds
// counts under its own kind, and its members are not compared one by one.
//
// The zero value is a comparison that paired nothing, which is also what a
// diff assembled by hand carries.
type MatchedCounts struct {
	// Schemas counts the databases a whole-server MySQL or MariaDB comparison
	// pairs; see [SchemaDiff.SchemasAdded]. Every other comparison leaves it
	// zero.
	Schemas int
	// Tables counts the tables both sides hold, including a table an owner
	// such as SQLite's virtual-table owner answers for.
	Tables int
	// Columns counts the columns both sides hold, in the tables both sides
	// hold.
	Columns int
	// Enums counts the enum types both sides hold.
	Enums int
	// EnumValues counts the values both sides hold, in the enum types both
	// sides hold.
	EnumValues int
	// Indexes counts the indexes both sides hold with a definition the plan
	// keeps, and the indexes the plan renames.
	Indexes int
	// Functions counts the routines both sides hold, functions and procedures
	// alike, one per overload.
	Functions int
	// RLSPolicies counts the row-level security policies the shared
	// comparison pairs.
	RLSPolicies int
	// RLSEnabledTables counts the tables the declaration enables row-level
	// security on and the database already has it enabled on.
	RLSEnabledTables int
	// Roles counts the declared roles the database holds.
	Roles int
	// Constraints counts the table constraints both sides hold with a
	// definition the plan keeps.
	Constraints int
	// Sequences counts the standalone sequences both sides hold.
	Sequences int
	// Views counts the views both sides hold.
	Views int
	// MaterializedViews counts the materialized views both sides hold,
	// including one the plan drops and creates again because its definition
	// changed: the diff reports that as a modification of the view, not as a
	// removal and an addition.
	MaterializedViews int
	// Triggers counts the triggers both sides hold.
	Triggers int
	// Grants counts the privileges the desired schema grants and the database
	// holds, one per role, privilege and object, or column of an object. A
	// declared ALL that the database holds as one row per privilege is one
	// pair, and so is a grant whose WITH GRANT OPTION differs: the option is a
	// setting of the grant.
	//
	// A grant is the one kind whose removals can name an object the database
	// does not hold. A REVOKE the declaration states for an object the plan
	// creates is planned with the creation, because PostgreSQL grants some
	// privileges the moment the object exists, so the database side counts it
	// as well.
	Grants int
	// RoleMemberships counts the memberships of a role in another that the
	// desired schema declares and the database holds.
	RoleMemberships int
}
