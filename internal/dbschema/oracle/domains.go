package oracle

import (
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// domainQuery reads the SQL domains this schema owns, one row per domain.
//
// USER_DOMAIN_COLS carries the base type and the nullability; USER_DOMAINS
// does not. An Oracle 23 domain may have several columns -- USER_DOMAINS.COLS
// counts them -- and Ptah models a single-column domain, so the query takes
// the one column and [Reader.readDomains] declines a domain that has more.
//
// The column's NAME is not stable and is not used as one. Measured on
// 23.26.2.0.0: `CREATE DOMAIN email_d AS VARCHAR2(255) NOT NULL` stores the
// column as EMAIL_D, while `CREATE DOMAIN score_d AS NUMBER(5,2) CHECK (VALUE
// BETWEEN 0 AND 100)` stores it as VALUE -- the declaration's own reference to
// VALUE decides it. That is why the NOT NULL constraint is recognized by
// comparing against the column name this query returns rather than against the
// literal "VALUE".
const domainQuery = `
SELECT d.name, c.column_name, c.data_type, c.data_length, c.data_precision,
       c.data_scale, c.nullable, c.data_default, d.cols
FROM all_domains d
JOIN all_domain_cols c
  ON c.owner = d.owner AND c.domain_name = d.name
WHERE d.owner = :1
ORDER BY d.name, c.column_id`

// domainConstraintQuery reads the CHECK constraints of this schema's domains.
//
// SEARCH_CONDITION is a CLOB here rather than the LONG the table constraints
// carry, so it is selected like any other column.
//
// The two views spell the owner differently -- ALL_DOMAIN_COLS calls it OWNER
// and ALL_DOMAIN_CONSTRAINTS calls it DOMAIN_OWNER -- which is why the join
// above and the predicate here do not read alike.
//
// Both the user's CHECK and Oracle's own NOT NULL restatement arrive through
// this view, and GENERATED does not separate them: measured on 23.26.2.0.0, an
// UNNAMED user CHECK is also `GENERATED NAME`. What separates them is the
// condition, which [notNullRestatement] recognizes.
const domainConstraintQuery = `
SELECT c.domain_name, c.name, c.search_condition
FROM all_domain_constraints c
WHERE c.domain_owner = :1 AND c.constraint_type = 'C'
ORDER BY c.domain_name, c.name`

// readDomains reads the schema's SQL domains.
//
// It is called only where the preset says this target has them, which is what
// keeps the query off Oracle 21: ALL_DOMAINS does not exist there, CREATE
// DOMAIN answers ORA-00901, and asking anyway would fail the statement -- and
// a failed statement aborts the enclosing transaction, so every later read
// would answer ORA-25P02's Oracle equivalent rather than the read that broke.
func (r *Reader) readDomains() ([]types.DBDomain, error) {
	checks, err := r.readDomainChecks()
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(domainQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var domains []types.DBDomain
	for rows.Next() {
		var (
			domain     types.DBDomain
			columnName string
			dataType   string
			length     sql.NullInt64
			precision  sql.NullInt64
			scale      sql.NullInt64
			nullable   string
			def        sql.NullString
			columns    int
		)
		if err := rows.Scan(&domain.Name, &columnName, &dataType, &length, &precision,
			&scale, &nullable, &def, &columns); err != nil {
			return nil, err
		}
		// A multi-column domain has no counterpart in the model, and reporting
		// its first column as the whole domain would describe something the
		// declaration could not have written. Left out rather than flattened.
		if columns != 1 {
			continue
		}
		domain.BaseType = formatColumnType(dataType, length, precision, scale)
		domain.NotNull = strings.EqualFold(strings.TrimSpace(nullable), "N")
		domain.Default = strings.TrimSpace(def.String)
		domain.CheckConstraints = declaredDomainChecks(checks[domain.Name], columnName)
		domain.Check = joinDomainChecks(domain.CheckConstraints)
		domains = append(domains, domain)
	}
	return domains, rows.Err()
}

// readDomainChecks groups the CHECK constraints by domain name.
func (r *Reader) readDomainChecks() (map[string][]types.DBDomainCheck, error) {
	rows, err := r.db.Query(domainConstraintQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	checks := make(map[string][]types.DBDomainCheck)
	for rows.Next() {
		var domainName string
		var check types.DBDomainCheck
		var condition sql.NullString
		if err := rows.Scan(&domainName, &check.Name, &condition); err != nil {
			return nil, err
		}
		check.Expression = strings.TrimSpace(condition.String)
		checks[domainName] = append(checks[domainName], check)
	}
	return checks, rows.Err()
}

// declaredDomainChecks drops the constraint Oracle writes for a NOT NULL.
//
// A domain declared NOT NULL grows a CHECK of its own, named by the server and
// numbered per database: measured on 23.26.2.0.0, `CREATE DOMAIN email_d AS
// VARCHAR2(255) NOT NULL` produced SYS_DOMAIN_C0043 with the condition
// `"EMAIL_D" IS NOT NULL`. Reporting it as a declared CHECK would compare
// against a declaration that has none, and the plan would carry the same
// change on every run with a different constraint name each time -- the defect
// withoutGeneratedKeys removes for primary keys.
//
// The nullability itself is not lost: it is read from the column, which is
// where it is a fact rather than a restatement.
func declaredDomainChecks(checks []types.DBDomainCheck, columnName string) []types.DBDomainCheck {
	kept := make([]types.DBDomainCheck, 0, len(checks))
	for _, check := range checks {
		if notNullRestatement(check.Expression, columnName) {
			continue
		}
		kept = append(kept, check)
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// notNullRestatement reports whether a condition is Oracle's own spelling of
// the domain's NOT NULL.
//
// It is matched against the column name the catalog reports rather than
// against "VALUE", because the column is named after the domain when the
// declaration never mentions VALUE.
func notNullRestatement(expression, columnName string) bool {
	folded := strings.Join(strings.Fields(strings.ToUpper(expression)), " ")
	return folded == fmt.Sprintf(`"%s" IS NOT NULL`, strings.ToUpper(columnName))
}

// joinDomainChecks renders the constraint set the way a declaration writes it.
func joinDomainChecks(checks []types.DBDomainCheck) string {
	if len(checks) == 0 {
		return ""
	}
	expressions := make([]string, 0, len(checks))
	for _, check := range checks {
		expressions = append(expressions, check.Expression)
	}
	return strings.Join(expressions, " AND ")
}
