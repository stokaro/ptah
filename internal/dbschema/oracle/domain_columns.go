package oracle

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/feature/oratype"
)

// domainTypedColumnQuery reads the columns a domain types, with the domain's
// owner and name.
//
// ALL_TAB_COLS reports such a column with the domain's base type in DATA_TYPE,
// so the type alone does not say that a domain typed it; DOMAIN_OWNER and
// DOMAIN_NAME do. Both exist on 23.26 and answer ORA-00904 on 21.3, which is
// why this read sits behind the preset key the domain read sits behind.
const domainTypedColumnQuery = `
SELECT c.table_name, c.column_name, c.domain_owner, c.domain_name
FROM all_tab_cols c
WHERE c.owner = :1
  AND c.domain_name IS NOT NULL
  AND c.hidden_column = 'NO'
  AND c.table_name NOT LIKE 'BIN$%'
ORDER BY c.table_name, c.column_id`

// domainConstraintCopyQuery reads the table constraints Oracle copied from a
// domain.
//
// Typing a column by a domain copies the domain's constraints onto the table,
// and each copy names the domain in DOMAIN_NAME. Measured on 23.26.3.0.0, a
// column SCORE typed by `dom_score AS NUMBER(5,2) CHECK (VALUE BETWEEN 0 AND
// 100)` carries a SYS_C constraint with the condition `"SCORE">=0 AND
// "SCORE"<=100`, and a column ADDR typed by `dom_email AS VARCHAR2(255) NOT
// NULL` carries `"ADDR" IS NOT NULL`. A NOT NULL the column states itself
// replaces the copy: `a dom_email NOT NULL` carries one NOT NULL constraint,
// and it names no domain.
const domainConstraintCopyQuery = `
SELECT k.constraint_name, k.table_name, kc.column_name, NVL(k.search_condition_vc, ' ')
FROM all_constraints k
JOIN all_cons_columns kc
     ON kc.owner = k.owner AND kc.constraint_name = k.constraint_name
WHERE k.owner = :1
  AND k.domain_name IS NOT NULL
  AND k.table_name NOT LIKE 'BIN$%'
ORDER BY k.table_name, k.constraint_name, kc.position`

// adoptDomainColumns reports each column a described domain types as that
// domain, and leaves out what the column holds only because the domain does.
//
// The column catalog reports such a column by its base type, NOT NULL when the
// domain is, with the domain's default, and with a table constraint for each
// of the domain's constraints. A declaration types the column by the domain and
// writes none of those, so reported as they stand they planned
// `MODIFY (addr dom_email NULL)` and the drop of the copied CHECK on every
// plan, measured on 23.26.3.0.0. Oracle refuses both: a column typed by a NOT
// NULL domain cannot be NULL (ORA-02258), and its default cannot differ from
// the domain's (ORA-11501).
//
// A column is reported as its domain only where the domain read describes the
// domain, which is a domain of the read's own schema. Any other domain is one a
// declaration cannot name, so the column keeps its base type. The copied
// constraints are left out for every domain: they belong to the domain, and a
// declaration of the table cannot hold them.
//
// It runs after the domain read, which it reads the described domains from.
func (r *Reader) adoptDomainColumns(ctx context.Context, db *catalog.Database) error {
	typed, err := r.readDomainTypedColumns(ctx)
	if err != nil {
		return err
	}
	copies, err := r.readDomainConstraintCopies(ctx)
	if err != nil {
		return err
	}
	domains, err := describedDomains(db)
	if err != nil {
		return err
	}
	for i := range db.Tables {
		for j := range db.Tables[i].Columns {
			column := &db.Tables[i].Columns[j]
			key := tableColumn{table: db.Tables[i].Name, column: column.Name}
			domain, found := typed[key]
			if !found || domain.owner != r.schema {
				continue
			}
			described, found := domains[domain.name]
			if !found {
				continue
			}
			column.DomainName, column.DomainSchema, column.FormattedType = domain.name, domain.owner, domain.name
			if copies.notNull[key] {
				column.IsNullable = "YES"
			}
			if column.ColumnDefault != nil && strings.TrimSpace(*column.ColumnDefault) == described.Default {
				column.ColumnDefault = nil
			}
		}
	}
	db.Constraints = slices.DeleteFunc(db.Constraints, func(constraint catalog.Constraint) bool {
		return copies.names[constraint.Name]
	})
	return nil
}

// tableColumn names one column of one table.
type tableColumn struct {
	table  string
	column string
}

// typingDomain is the domain that types a column.
type typingDomain struct {
	owner string
	name  string
}

// readDomainTypedColumns reads the domain each domain-typed column names.
func (r *Reader) readDomainTypedColumns(ctx context.Context) (map[tableColumn]typingDomain, error) {
	rows, err := r.db.QueryContext(ctx, domainTypedColumnQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	typed := make(map[tableColumn]typingDomain)
	for rows.Next() {
		var key tableColumn
		var domain typingDomain
		if err := rows.Scan(&key.table, &key.column, &domain.owner, &domain.name); err != nil {
			return nil, err
		}
		typed[key] = domain
	}
	return typed, rows.Err()
}

// domainConstraintCopies is what a read found of the constraints copied from
// domains: their names, and the columns whose NOT NULL is such a copy.
type domainConstraintCopies struct {
	names   map[string]bool
	notNull map[tableColumn]bool
}

// copiedNotNull matches the condition Oracle writes for a NOT NULL, and
// captures the column it names.
var copiedNotNull = regexp.MustCompile(`^"([^"]+)" IS NOT NULL$`)

// readDomainConstraintCopies reads the constraints Oracle copied from domains.
func (r *Reader) readDomainConstraintCopies(ctx context.Context) (domainConstraintCopies, error) {
	rows, err := r.db.QueryContext(ctx, domainConstraintCopyQuery, r.schema)
	if err != nil {
		return domainConstraintCopies{}, err
	}
	defer rows.Close()

	copies := domainConstraintCopies{names: make(map[string]bool), notNull: make(map[tableColumn]bool)}
	for rows.Next() {
		var name, condition string
		var key tableColumn
		if err := rows.Scan(&name, &key.table, &key.column, &condition); err != nil {
			return domainConstraintCopies{}, err
		}
		copies.names[name] = true
		if match := copiedNotNull.FindStringSubmatch(strings.TrimSpace(condition)); match != nil && match[1] == key.column {
			copies.notNull[key] = true
		}
	}
	return copies, rows.Err()
}

// describedDomains indexes the domains a read describes by name.
func describedDomains(db *catalog.Database) (map[string]*oratype.ObservedDomain, error) {
	objects, err := db.FeatureObjects.All()
	if err != nil {
		return nil, err
	}
	domains := make(map[string]*oratype.ObservedDomain)
	for _, object := range objects {
		if domain, ok := object.Value.(*oratype.ObservedDomain); ok {
			domains[object.Ref.Name.Source] = domain
		}
	}
	return domains, nil
}
