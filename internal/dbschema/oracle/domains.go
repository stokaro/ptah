package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
)

// domainQuery reads the SQL domains this schema owns, one row per domain,
// with the facts that decide whether the domain model can describe it.
//
// The model describes a single-column domain: a base type with an optional NOT
// NULL, DEFAULT and CHECK. Oracle 23 also has domains of several columns,
// enumeration domains and flexible domains. Measured on 23.26.3.0.0,
// ALL_DOMAINS reports `CREATE DOMAIN d AS (lat AS NUMBER, lon AS NUMBER)` with
// COLS 2 and TYPE REGULAR, `CREATE DOMAIN d AS ENUM (red, green)` with COLS 1
// and TYPE ENUMERATED over a NUMBER column, and a flexible domain with COLS 1,
// DISCRIMINANTCOLS 1, TYPE FLEXIBLE and two rows in ALL_DOMAIN_COLS. So the
// column count alone does not separate them, and TYPE is read beside it.
const domainQuery = `
SELECT d.name, d.cols, d.type
FROM all_domains d
WHERE d.owner = :1
ORDER BY d.name`

// domainColumnQuery reads the columns of this schema's domains, in column
// order.
//
// ALL_DOMAIN_COLS carries the base type and the nullability; ALL_DOMAINS does
// not.
//
// The column's NAME is not stable and is not used as one. Measured on
// 23.26.2.0.0: `CREATE DOMAIN email_d AS VARCHAR2(255) NOT NULL` stores the
// column as EMAIL_D, while `CREATE DOMAIN score_d AS NUMBER(5,2) CHECK (VALUE
// BETWEEN 0 AND 100)` stores it as VALUE -- the declaration's own reference to
// VALUE decides it. That is why the NOT NULL constraint is recognized by
// comparing against the column name this query returns rather than against the
// literal "VALUE".
//
// The two views spell the owner the same way, OWNER, and the domain
// differently: NAME in ALL_DOMAINS, DOMAIN_NAME here.
const domainColumnQuery = `
SELECT c.domain_name, c.column_name, c.data_type, c.data_length, c.data_precision,
       c.data_scale, c.char_length, c.nullable, c.data_default
FROM all_domain_cols c
WHERE c.owner = :1
ORDER BY c.domain_name, c.column_id`

// domainConstraintQuery reads the CHECK constraints of this schema's domains.
//
// SEARCH_CONDITION is a CLOB here rather than the LONG the table constraints
// carry, so it is selected like any other column.
//
// The views spell the owner differently -- ALL_DOMAIN_COLS calls it OWNER
// and ALL_DOMAIN_CONSTRAINTS calls it DOMAIN_OWNER -- which is why the
// predicate above and the one here do not read alike.
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
//
// A domain the model cannot describe is recorded as unrepresentable rather than
// left out. Left out, the claim that the read describes every domain would say
// the domain is absent, and a declaration of that name would plan its creation
// over the one the schema holds.
func (r *Reader) readDomains(ctx context.Context, db *catalog.Database) error {
	checks, err := r.readDomainChecks(ctx)
	if err != nil {
		return err
	}
	columns, err := r.readDomainColumns(ctx)
	if err != nil {
		return err
	}

	rows, err := r.db.QueryContext(ctx, domainQuery, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	objects := db.FeatureObjects
	var limits []schemaext.SubjectCoverage
	for rows.Next() {
		var (
			name     string
			declared int
			shape    sql.NullString
		)
		if err := rows.Scan(&name, &declared, &shape); err != nil {
			return err
		}
		ref := oratype.Ref(oratype.DomainKind, "", name)
		if reason := undescribedDomain(shape.String, declared, len(columns[name])); reason != "" {
			limits = append(limits, unrepresentable(oratype.DomainKind, ref, reason))
			continue
		}
		column := columns[name][0]
		// The domain catalog carries no vector description, so a domain over a
		// vector reads as a bare VECTOR here.
		domain := &oratype.ObservedDomain{
			BaseType: formatDomainType(column.dataType, column.length, column.charLength, column.precision, column.scale),
			NotNull:  strings.EqualFold(strings.TrimSpace(column.nullable), "N"),
			Default:  strings.TrimSpace(column.def.String),
			Check:    joinDomainChecks(declaredDomainChecks(checks[name], column.name)),
		}
		object, err := oratype.ObservedObject(ref, domain)
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
	return recordTypes(db, objects, oratype.DomainKind, limits)
}

// undescribedDomain says why the domain model cannot describe a domain: its
// shape, the number of columns it declares and the number the column read
// returned. It is empty for a domain the model can describe.
func undescribedDomain(shape string, declared, found int) string {
	switch shape = strings.TrimSpace(shape); {
	case strings.EqualFold(shape, "ENUMERATED"):
		return "the domain is an enumeration domain, which the domain model cannot describe"
	case strings.EqualFold(shape, "FLEXIBLE"):
		return "the domain is a flexible domain, which the domain model cannot describe"
	case !strings.EqualFold(shape, "REGULAR"):
		return fmt.Sprintf("the domain is of type %q, which the domain model cannot describe", shape)
	case declared != 1:
		return fmt.Sprintf("the domain has %d columns, and the domain model describes a single-column domain", declared)
	case found != 1:
		return fmt.Sprintf("the read found %d columns of a single-column domain", found)
	}
	return ""
}

// unrepresentable is the claim that the read found the type ref names and
// cannot describe it, for the reason given.
func unrepresentable(kind schemaext.Kind, ref objectidentity.ID, reason string) schemaext.SubjectCoverage {
	return schemaext.SubjectCoverage{Kind: kind, Subject: ref,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason}}
}

// recordTypes stores the read's types of kind in db, with the claim that the
// read describes every one of them except the types limits names: the read
// found those and cannot describe them.
func recordTypes(db *catalog.Database, objects schemaext.Objects, kind schemaext.Kind, limits []schemaext.SubjectCoverage) error {
	known, err := oratype.Coverage(kind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, limits)
	if err != nil {
		return err
	}
	if db.FeatureCoverage, err = db.FeatureCoverage.Combine(known); err != nil {
		return err
	}
	db.FeatureObjects = objects
	return nil
}

// domainColumn is one column of a domain, as ALL_DOMAIN_COLS reports it.
type domainColumn struct {
	name       string
	dataType   string
	length     sql.NullInt64
	charLength sql.NullInt64
	precision  sql.NullInt64
	scale      sql.NullInt64
	nullable   string
	def        sql.NullString
}

// readDomainColumns groups the domain columns by domain name, in column order.
func (r *Reader) readDomainColumns(ctx context.Context) (map[string][]domainColumn, error) {
	rows, err := r.db.QueryContext(ctx, domainColumnQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string][]domainColumn)
	for rows.Next() {
		var domainName string
		var column domainColumn
		if err := rows.Scan(
			&domainName, &column.name, &column.dataType, &column.length, &column.precision,
			&column.scale, &column.charLength, &column.nullable, &column.def,
		); err != nil {
			return nil, err
		}
		columns[domainName] = append(columns[domainName], column)
	}
	return columns, rows.Err()
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
