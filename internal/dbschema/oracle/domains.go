package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
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
       c.data_scale, c.char_length, c.nullable, c.data_default, d.cols
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

// readDomains reads the schema's SQL domains into db as the Oracle type
// owner's observations, with the knowledge that the read looked.
//
// It is called only where the preset says this target has them, which is what
// keeps the query off Oracle 21: ALL_DOMAINS does not exist there, CREATE
// DOMAIN answers ORA-00901, and asking anyway would fail the statement -- and
// a failed statement aborts the enclosing transaction, so every later read
// would answer ORA-25P02's Oracle equivalent rather than the read that broke.
// The same gate keeps the coverage claim off that line: a target without
// domains records no knowledge of them.
func (r *Reader) readDomains(ctx context.Context, db *catalog.Database) error {
	checks, err := r.readDomainChecks(ctx)
	if err != nil {
		return err
	}

	rows, err := r.db.QueryContext(ctx, domainQuery, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	objects := db.FeatureObjects
	for rows.Next() {
		var (
			name       string
			columnName string
			dataType   string
			length     sql.NullInt64
			charLength sql.NullInt64
			precision  sql.NullInt64
			scale      sql.NullInt64
			nullable   string
			def        sql.NullString
			columns    int
		)
		if err := rows.Scan(
			&name, &columnName, &dataType, &length, &precision,
			&scale, &charLength, &nullable, &def, &columns,
		); err != nil {
			return err
		}
		// A multi-column domain has no counterpart in the model, and reporting
		// its first column as the whole domain would describe something the
		// declaration could not have written. Left out rather than flattened.
		if columns != 1 {
			continue
		}
		// The domain catalog carries no vector description, so a domain over a
		// vector reads as a bare VECTOR here.
		domain := &oratype.ObservedDomain{
			BaseType: formatDomainType(dataType, length, charLength, precision, scale),
			NotNull:  strings.EqualFold(strings.TrimSpace(nullable), "N"),
			Default:  strings.TrimSpace(def.String),
			Check:    joinDomainChecks(declaredDomainChecks(checks[name], columnName)),
		}
		object, err := oratype.ObservedObject(oratype.Ref(oratype.DomainKind, "", name), domain)
		if err != nil {
			return err
		}
		if objects, err = objects.With(object); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return recordTypes(db, objects, oratype.DomainKind)
}

// recordTypes stores the read's types of kind in db, with the claim that the
// read describes every one of them.
func recordTypes(db *catalog.Database, objects schemaext.Objects, kind schemaext.Kind) error {
	known, err := oratype.Coverage(kind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return err
	}
	if db.FeatureCoverage, err = db.FeatureCoverage.Combine(known); err != nil {
		return err
	}
	db.FeatureObjects = objects
	return nil
}

// formatDomainType keeps facts whose meaning differs from the column catalog.
// A scale without precision constrains NUMBER's scale. National character
// lengths count characters, while DATA_LENGTH counts their encoded bytes.
func formatDomainType(dataType string, length, charLength, precision, scale sql.NullInt64) string {
	if dataType == "NUMBER" && !precision.Valid && scale.Valid {
		return fmt.Sprintf("NUMBER(*,%d)", scale.Int64)
	}
	if dataType == "NCHAR" || dataType == "NVARCHAR2" {
		length = charLength
	}
	return formatColumnType(dataType, sql.NullString{}, length, precision, scale)
}

// domainCheck is one CHECK constraint of a domain: the name the catalog
// gives it and its condition.
type domainCheck struct {
	Name       string
	Expression string
}

// readDomainChecks groups the CHECK constraints by domain name.
func (r *Reader) readDomainChecks(ctx context.Context) (map[string][]domainCheck, error) {
	rows, err := r.db.QueryContext(ctx, domainConstraintQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	checks := make(map[string][]domainCheck)
	for rows.Next() {
		var domainName string
		var check domainCheck
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
func declaredDomainChecks(checks []domainCheck, columnName string) []domainCheck {
	kept := make([]domainCheck, 0, len(checks))
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
func joinDomainChecks(checks []domainCheck) string {
	if len(checks) == 0 {
		return ""
	}
	expressions := make([]string, 0, len(checks))
	for _, check := range checks {
		expressions = append(expressions, check.Expression)
	}
	return strings.Join(expressions, " AND ")
}
