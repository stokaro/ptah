// Package pgcatalogview holds the rows of the information_schema views a schema
// read needs, read from pg_catalog with no privilege condition.
//
// information_schema answers for the role that asks. Measured on PostgreSQL
// 18.6: a role with no table privileges sees no row of tables, columns or
// table_constraints, and a role holding pg_read_all_data sees tables and
// columns and no constraint, because table_constraints leaves SELECT out of the
// privileges it counts. A comparison reads that silence as absence, so two
// databases that differ report as synced for such a role. The catalog tables
// those views are built on list every object to every role, so a read through
// them does not depend on who connects.
//
// Each statement here returns the columns of the view it replaces, under the
// same names, so a query keeps its shape and changes only what it selects from.
// A view's expression is copied as PostgreSQL defines it, and its privilege
// condition is the one part left out. The statements are subqueries: a caller
// writes `FROM (` + statement + `) alias`.
//
// They run on PostgreSQL and on YugabyteDB. Neither of the other targets of the
// reader needs them. CockroachDB's information_schema does not filter this way:
// measured on v26.3.5, a role refused SELECT on a table still finds it there,
// with its columns and constraints. Spanner's pg_catalog is an emulation over
// information_schema.
package pgcatalogview

// notTemporary leaves out temporary relations, as the views do: another
// session's by pg_is_other_temp_schema, and this session's because the views
// report one as LOCAL TEMPORARY, which no read asks for. nc is the relation's
// pg_namespace row.
const notTemporary = `NOT pg_is_other_temp_schema(nc.oid)
		AND nc.oid <> pg_my_temp_schema()`

// Tables is information_schema.tables for its base tables, with the columns a
// schema read uses: table_schema, table_name and table_type. A partitioned
// parent is a base table there too.
const Tables = `SELECT
			nc.nspname AS table_schema,
			c.relname AS table_name,
			'BASE TABLE' AS table_type
		FROM pg_class c
		JOIN pg_namespace nc ON nc.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p')
		AND ` + notTemporary

// columnsBeforeDefault and columnsAfterDefault are information_schema.columns
// for the columns of base tables, with the columns a schema read uses, around
// the default's expression, which [Columns] and [ColumnsWithoutGenerated] differ by.
//
// The numeric facts come from the bodies of information_schema's helper
// functions, as PostgreSQL 18 defines them, rather than from calls to them, so
// a read needs no USAGE on that schema: _pg_truetypid and _pg_truetypmod become
// the true_type row, and _pg_char_max_length, _pg_numeric_precision,
// _pg_numeric_scale and _pg_datetime_precision the expressions over it. The
// numbers are the built-in type OIDs those functions name, which never change:
// 1042 char, 1043 varchar, 1560 bit, 1562 varbit, 21 int2, 23 int4, 20 int8,
// 1700 numeric, 700 float4, 701 float8, 1082 date, 1083 time, 1114 timestamp,
// 1184 timestamptz, 1266 timetz, 1186 interval.
const columnsBeforeDefault = `SELECT
			nc.nspname AS table_schema,
			c.relname AS table_name,
			a.attname AS column_name,
			a.attnum AS ordinal_position,
			`

const columnsAfterDefault = ` AS column_default,
			CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END AS is_nullable,
			CASE WHEN t.typtype = 'd' THEN
				CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
					WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, NULL)
					ELSE 'USER-DEFINED' END
			ELSE
				CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
					WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, NULL)
					ELSE 'USER-DEFINED' END
			END AS data_type,
			CASE WHEN true_type.typmod = -1 THEN NULL
				WHEN true_type.typid IN (1042, 1043) THEN true_type.typmod - 4
				WHEN true_type.typid IN (1560, 1562) THEN true_type.typmod
			END AS character_maximum_length,
			CASE true_type.typid
				WHEN 21 THEN 16
				WHEN 23 THEN 32
				WHEN 20 THEN 64
				WHEN 1700 THEN CASE WHEN true_type.typmod = -1 THEN NULL
					ELSE ((true_type.typmod - 4) >> 16) & 65535 END
				WHEN 700 THEN 24
				WHEN 701 THEN 53
			END AS numeric_precision,
			CASE WHEN true_type.typid IN (21, 23, 20) THEN 0
				WHEN true_type.typid = 1700 THEN CASE WHEN true_type.typmod = -1 THEN NULL
					ELSE (true_type.typmod - 4) & 65535 END
			END AS numeric_scale,
			CASE WHEN true_type.typid = 1082 THEN 0
				WHEN true_type.typid IN (1083, 1114, 1184, 1266) THEN
					CASE WHEN true_type.typmod < 0 THEN 6 ELSE true_type.typmod END
				WHEN true_type.typid = 1186 THEN
					CASE WHEN true_type.typmod < 0 OR true_type.typmod & 65535 = 65535 THEN 6
						ELSE true_type.typmod & 65535 END
			END AS datetime_precision,
			co.collname AS collation_name,
			CASE WHEN t.typtype = 'd' THEN nt.nspname END AS domain_schema,
			CASE WHEN t.typtype = 'd' THEN t.typname END AS domain_name,
			COALESCE(nbt.nspname, nt.nspname) AS udt_schema,
			COALESCE(bt.typname, t.typname) AS udt_name
		FROM pg_attribute a
		LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace nc ON nc.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		JOIN pg_namespace nt ON nt.oid = t.typnamespace
		LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON nbt.oid = bt.typnamespace)
			ON t.typtype = 'd' AND bt.oid = t.typbasetype
		LEFT JOIN (pg_collation co JOIN pg_namespace nco ON nco.oid = co.collnamespace)
			ON co.oid = a.attcollation AND (nco.nspname <> 'pg_catalog' OR co.collname <> 'default')
		CROSS JOIN LATERAL (
			SELECT
				CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE a.atttypid END AS typid,
				CASE WHEN t.typtype = 'd' THEN t.typtypmod ELSE a.atttypmod END AS typmod
		) true_type
		WHERE a.attnum > 0
		AND NOT a.attisdropped
		AND c.relkind IN ('r', 'p')
		AND ` + notTemporary

// Columns is information_schema.columns for the columns of base tables. A
// generated column's expression is stored as a default, and the view reports no
// default for it.
const Columns = columnsBeforeDefault +
	"CASE WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid) END" + columnsAfterDefault

// ColumnsWithoutGenerated is [Columns] for a server without
// pg_attribute.attgenerated, where naming the column fails the statement. Such a
// server has no generated columns, so every stored expression is a default.
const ColumnsWithoutGenerated = columnsBeforeDefault +
	"pg_get_expr(ad.adbin, ad.adrelid)" + columnsAfterDefault

// TableConstraints is information_schema.table_constraints for the constraints
// pg_constraint records, with the columns a schema read uses: table_schema,
// table_name, constraint_name and constraint_type.
//
// The view keeps the same relation kinds and leaves out the same contypes. Below
// PostgreSQL 18 it also lists a row for each NOT NULL, under a name it makes up
// and no catalog holds, which is not read.
const TableConstraints = `SELECT
			nc.nspname AS table_schema,
			c.relname AS table_name,
			con.conname AS constraint_name,
			CASE con.contype
				WHEN 'c' THEN 'CHECK'
				WHEN 'n' THEN 'CHECK'
				WHEN 'f' THEN 'FOREIGN KEY'
				WHEN 'p' THEN 'PRIMARY KEY'
				WHEN 'u' THEN 'UNIQUE'
			END AS constraint_type
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace nc ON nc.oid = c.relnamespace
		WHERE con.contype NOT IN ('t', 'x')
		AND c.relkind IN ('r', 'p')
		AND ` + notTemporary

// TableGrants is information_schema.role_table_grants with the columns a schema
// read uses: grantee, privilege_type, table_schema, table_name, is_grantable and
// grantor. The view lists a grant only where its grantor or grantee is a role the
// connecting role is a member of, which is the condition left out.
//
// As in the view, an object with no ACL reads as holding its default one, the owner
// of an object can always grant what it holds, and only the SQL-standard privileges
// on the SQL-standard relation kinds are listed.
const TableGrants = `SELECT
			COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
			acl.privilege_type,
			nc.nspname AS table_schema,
			c.relname AS table_name,
			CASE WHEN acl.is_grantable OR (acl.grantee <> 0 AND pg_has_role(acl.grantee, c.relowner, 'USAGE'))
				THEN 'YES' ELSE 'NO' END AS is_grantable,
			grantor.rolname AS grantor
		FROM pg_class c
		JOIN pg_namespace nc ON nc.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) acl
		LEFT JOIN pg_roles grantee ON grantee.oid = acl.grantee
		JOIN pg_roles grantor ON grantor.oid = acl.grantor
		WHERE c.relkind IN ('r', 'v', 'f', 'p')
		AND acl.privilege_type IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
		AND (grantee.oid IS NOT NULL OR acl.grantee = 0)`

// ViewCheckOption is the check_option column of information_schema.views, read
// from class, a pg_class alias holding the view's row.
func ViewCheckOption(class string) string {
	return `CASE WHEN 'check_option=cascaded' = ANY (` + class + `.reloptions) THEN 'CASCADED'
				WHEN 'check_option=local' = ANY (` + class + `.reloptions) THEN 'LOCAL'
				ELSE 'NONE' END`
}

// OwnedSequence is what pg_get_serial_sequence answers for the column attnum of
// the relation relation, both SQL expressions: the owned sequence, spelled
// schema.name and quoted where the name needs it, or NULL.
//
// The function takes the table by name, and PostgreSQL resolves a qualified name
// only for a role with USAGE on its schema: measured on 18.6, the call fails with
// `permission denied for schema` for a role without it, which takes the whole
// column read with it. This reads what the function reads, pg_depend, and needs
// no privilege. Like the function it takes a serial column's auto dependency and
// an identity column's internal one.
func OwnedSequence(relation, attnum string) string {
	return `(
				SELECT format('%I.%I', sequence_schema.nspname, sequence_class.relname)
				FROM pg_depend sequence_dependency
				JOIN pg_class sequence_class ON sequence_class.oid = sequence_dependency.objid
					AND sequence_class.relkind = 'S'
				JOIN pg_namespace sequence_schema ON sequence_schema.oid = sequence_class.relnamespace
				WHERE sequence_dependency.classid = 'pg_class'::regclass
					AND sequence_dependency.refclassid = 'pg_class'::regclass
					AND sequence_dependency.refobjid = ` + relation + `
					AND sequence_dependency.refobjsubid = ` + attnum + `
					AND sequence_dependency.deptype IN ('a', 'i')
				LIMIT 1
			)`
}
