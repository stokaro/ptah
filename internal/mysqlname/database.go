package mysqlname

// foldsNames is true on a server that folds database and table names, which
// holds no two databases that differ in case: lower_case_table_names 1 stores
// names in lower case, and 2 stores them as written and looks them up in lower
// case.
const foldsNames = "@@lower_case_table_names <> 0"

// SameDatabase is a SQL predicate that left and right name the same database,
// where each is a column or a SQL expression; a bind in either is bound twice,
// as the predicate repeats both.
//
// A server started with lower_case_table_names=0 keeps r_t and R_T apart, and
// some information_schema views compare a database name without case. Measured
// with both databases present, `= 'r_t'` matches the rows of R_T as well in
// SCHEMA_PRIVILEGES, TABLE_PRIVILEGES and COLUMN_PRIVILEGES on MySQL 8.4.13
// and 26.10.0, and in those three, TRIGGERS and EVENTS on MariaDB 11.8.9 and
// 12.3.3. TABLES, COLUMNS, SCHEMATA, ROUTINES, STATISTICS, KEY_COLUMN_USAGE,
// TABLE_CONSTRAINTS, REFERENTIAL_CONSTRAINTS and VIEWS compare exactly on all
// four. Without the byte comparison a read of r_t describes the triggers,
// events and grants of R_T as its own, and a plan built on it revokes a grant
// on R_T.
//
// The plain equality stays first, so a server can still look the database up
// by it; the byte comparison after it keeps the rows of that one database. A
// server that folds names compares as it would without this predicate, and
// must: measured on MariaDB 11.8.9 and 12.3.3 with lower_case_table_names=2,
// DATABASE() answers R_LC for a session that selected R_LC while the privilege
// views spell it r_lc, so a byte comparison there would find no grant at all.
func SameDatabase(left, right string) string {
	return "(" + left + " = " + right +
		" AND (" + foldsNames + " OR CONVERT(" + left + " USING utf8mb4) COLLATE utf8mb4_bin = CONVERT(" +
		right + " USING utf8mb4)))"
}

// ContainsName is a SQL predicate that the text holds name, where each is a
// column or a SQL expression; a bind in either is bound twice, as the predicate
// repeats both. It compares the way [SameDatabase] does: as the text's
// collation does, and as bytes on a server that keeps names apart by case.
//
// MariaDB writes every table reference in VIEWS.VIEW_DEFINITION qualified and
// quoted, as `R_T`.`items`, and the column compares without case. Measured on
// MariaDB 11.8.9 and 12.3.3 with lower_case_table_names=0, INSTR of `r_t`.
// finds the reference of a view in R_T to R_T.items, so a search for the
// views of other databases that read r_t counts it.
func ContainsName(text, name string) string {
	return "(INSTR(" + text + ", " + name + ") > 0" +
		" AND (" + foldsNames + " OR INSTR(CONVERT(" + text + " USING utf8mb4) COLLATE utf8mb4_bin, CONVERT(" +
		name + " USING utf8mb4) COLLATE utf8mb4_bin) > 0))"
}
