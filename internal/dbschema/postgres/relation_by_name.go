package postgres

// relationByName renders the join condition that finds, in the pg_class alias
// class, the relation an information_schema view names by its namespace oid
// and its name. Every read that keys pg_class by name builds the condition
// here, so no read can find a row the others do not.
//
// A namespace and a name do not identify one pg_class row on every engine this
// reader serves. PostgreSQL and YugabyteDB keep tables, views, sequences and
// indexes in one namespace per schema, so the pair is unique there. CockroachDB
// names an index per table but lists it in pg_class under the table's schema.
// Measured on v26.3.2, `CREATE TABLE t (a int); CREATE INDEX i ON t (a);
// CREATE TABLE i (a int)` leaves two rows named i in public, relkind 'i' and
// 'r', and a table named p_pkey beside a table p does the same with p's
// primary key index. Joined by name alone, the table i is read twice, with its
// columns and grants, and the read refuses its own duplicate
// (stokaro/ptah#5043).
//
// information_schema never names an index, so leaving the index kinds out
// finds the one relation it means on every engine, and changes nothing where
// names are unique.
func relationByName(class, namespace, name string) string {
	return class + ".relnamespace = " + namespace +
		" AND " + class + ".relname = " + name +
		" AND " + class + ".relkind NOT IN ('i', 'I')"
}
