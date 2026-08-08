package postgres

// White-box testing required: readColumnsForSchema is unexported and the SQL it
// issues is the only place the declared type of a domain column enters the
// process. information_schema.columns.data_type for a column of domain
// positive_int is its base type, "integer", so the reader that trusts it hands
// the rest of the pipeline a column that never had the domain -- and no pure
// function downstream can notice, because there is nothing left to notice with.
//
// The fake server follows the same rule as the index guard next door: a
// projection is answered from the fixture catalog only when it actually reads
// the catalog column the answer comes from.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// pgColumnCatalog is one row of the column introspection query as PostgreSQL
// 17.10 reports it.
type pgColumnCatalog struct {
	tableName  string
	columnName string
	// dataType is information_schema.columns.data_type, which for a domain
	// column is the domain's base type rather than the domain.
	dataType string
	udtName  string
	// formattedType is format_type(a.atttypid, a.atttypmod), the server's own
	// spelling of the declared type. It is the only value in this query that
	// names the domain.
	formattedType string
	// domainName is information_schema.columns.domain_name: how the catalog
	// records that the declared type was a domain. Empty for a plain column.
	domainName string
	// domainSchema is information_schema.columns.domain_schema, the other half
	// of the domain's identity. public.status and other.status are two types,
	// and a reader that drops this hands the comparator a name both of them
	// answer to.
	domainSchema string
}

// domainColumnCatalog is CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)
// followed by a qty column of that domain, measured on PostgreSQL 17.10.
func domainColumnCatalog() pgColumnCatalog {
	return pgColumnCatalog{
		tableName:     "t",
		columnName:    "qty",
		dataType:      "integer",
		udtName:       "int4",
		formattedType: "positive_int",
		domainName:    "positive_int",
		domainSchema:  "public",
	}
}

// offSearchPathDomainColumnCatalog is the same column for a domain that lives
// in another schema: format_type qualifies it because the search path does not
// reach it, while information_schema reports the two halves separately.
func offSearchPathDomainColumnCatalog() pgColumnCatalog {
	catalog := domainColumnCatalog()
	catalog.formattedType = "other.positive_int"
	catalog.domainSchema = "other"
	return catalog
}

// plainColumnCatalog is a plain integer column: same base type, no domain. It
// is what keeps the assertion below from passing for the wrong reason -- the
// reader must leave FormattedType empty here, or every column in every database
// would start round-tripping through format_type.
func plainColumnCatalog() pgColumnCatalog {
	catalog := domainColumnCatalog()
	catalog.columnName = "id"
	catalog.formattedType = "integer"
	catalog.domainName = ""
	catalog.domainSchema = ""
	return catalog
}

// serveColumnQuery answers the column query. formatted_type and domain_name are
// answered from the catalog only when their projections actually read
// information_schema's domain_name; a projection that does not ask cannot
// distinguish the two fixtures above on a real server either, and gets the empty
// string the CASE's ELSE branch returns.
func serveColumnQuery(catalog pgColumnCatalog, query string) (dbtest.QueryResult, error) {
	asksAboutDomains, err := queryAsksAboutDomains(query)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	formattedType := ""
	if asksAboutDomains && catalog.domainName != "" {
		formattedType = catalog.formattedType
	}
	readsDomainName, err := queryReadsCatalogColumn(query, "domain_name")
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	domainName := ""
	if readsDomainName {
		domainName = catalog.domainName
	}
	readsDomainSchema, err := queryReadsCatalogColumn(query, "domain_schema")
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	domainSchema := ""
	if readsDomainSchema {
		domainSchema = catalog.domainSchema
	}
	return dbtest.QueryResult{
		Columns: []string{
			"table_name", "column_name", "data_type", "udt_name", "formatted_type",
			"domain_name", "domain_schema", "is_nullable", "column_default", "character_maximum_length",
			"numeric_precision", "numeric_scale", "ordinal_position",
			"generated_kind", "generated_expression", "identity_kind",
			"owned_sequence_name",
		},
		Rows: [][]driver.Value{{
			catalog.tableName, catalog.columnName, catalog.dataType, catalog.udtName, formattedType,
			domainName, domainSchema, "YES", nil, nil,
			nil, nil, int64(1),
			"", "", "",
			"",
		}},
	}, nil
}

// queryAsksAboutDomains reports whether the formatted_type projection reads
// information_schema's domain_name.
//
// It inspects the projection with comments stripped -- see selectListItem next
// door. The reader's comment above this CASE explains the domain case in prose
// and therefore contains the words "domain_name"; a check against the raw query
// text passes on a build that no longer reads the column, which is exactly the
// revert this guard exists to catch.
func queryAsksAboutDomains(query string) (bool, error) {
	projection, ok := selectListItem(query, "formatted_type", "FROM information_schema.columns")
	if !ok {
		return false, fmt.Errorf("query has no projection aliased formatted_type:\n%s", query)
	}
	return strings.Contains(projection, "domain_name"), nil
}

// queryReadsCatalogColumn reports whether the projection aliased column reads
// the information_schema column of that name rather than answering a constant.
//
// The alias is cut off before the expression is inspected, since the alias is
// spelled like the column: a projection of an empty SQL string literal aliased
// domain_name names the column without reading it, and a comparator handed that
// empty string cannot tell a domain column from a plain one of the same base
// type -- nor, for domain_schema, one domain from another of the same name in a
// different schema.
func queryReadsCatalogColumn(query, column string) (bool, error) {
	projection, ok := selectListItem(query, column, "FROM information_schema.columns")
	if !ok {
		return false, fmt.Errorf("query has no projection aliased %s:\n%s", column, query)
	}
	expression := projection
	if alias := strings.LastIndex(strings.ToLower(projection), " as "); alias >= 0 {
		expression = projection[:alias]
	}
	return strings.Contains(expression, column), nil
}

// TestReadColumnsForSchema_KeepsTheDeclaredDomainType guards the last of the
// #1242 introspection gaps. Measured on PostgreSQL 17.10, a reader that takes
// information_schema's data_type for a domain column emits
// `"qty" integer` for a column declared positive_int: it replays at psql exit
// 0 and leaves a column without the domain's CHECK, and re-diffing the result
// against the source with the pinned binary v1.3.0 then reports
// `ALTER TABLE "t" ALTER COLUMN "qty" TYPE positive_int;`.
func TestReadColumnsForSchema_KeepsTheDeclaredDomainType(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		catalog func() pgColumnCatalog
		// expectedFormattedType is what the reader must carry forward.
		// dbschematogo prefers a non-empty FormattedType over data_type when
		// it builds the field type, so this is the value that decides whether
		// the emitted DDL says positive_int or integer.
		expectedFormattedType string
		// expectedDomainName is the FACT that the declared type was a domain.
		// The comparator decides a domain column by identity instead of
		// normalizing its name, and a name is all it would otherwise have: a
		// domain named "waypoint" contains "int" and one named "context"
		// contains "text", so with this empty the name decides whether a
		// column changed at all (stokaro/ptah#1138).
		expectedDomainName string
		// expectedDomainSchema is the other half of that fact. Without it a
		// column of public.status and a desired other.status compare equal,
		// the required ALTER COLUMN ... TYPE is never planned, and the
		// DROP DOMAIN ... CASCADE the plan keeps takes the column and its data
		// (stokaro/ptah#1138).
		expectedDomainSchema string
	}{
		{
			name:                  "domain column keeps the domain",
			catalog:               domainColumnCatalog,
			expectedFormattedType: "positive_int",
			expectedDomainName:    "positive_int",
			expectedDomainSchema:  "public",
		},
		{
			name:                  "a domain off the search path keeps its schema",
			catalog:               offSearchPathDomainColumnCatalog,
			expectedFormattedType: "other.positive_int",
			expectedDomainName:    "positive_int",
			expectedDomainSchema:  "other",
		},
		{
			name:                  "plain column is left alone",
			catalog:               plainColumnCatalog,
			expectedFormattedType: "",
			expectedDomainName:    "",
			expectedDomainSchema:  "",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			catalog := test.catalog()
			db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				return serveColumnQuery(catalog, query)
			})

			columnsByTable, err := NewPostgreSQLReader(db.SQL, "public").readColumnsForSchema("public")
			c.Assert(err, qt.IsNil)
			c.Assert(columnsByTable["t"], qt.HasLen, 1)
			c.Assert(columnsByTable["t"][0].FormattedType, qt.Equals, test.expectedFormattedType)
			c.Assert(columnsByTable["t"][0].DomainName, qt.Equals, test.expectedDomainName)
			c.Assert(columnsByTable["t"][0].DomainSchema, qt.Equals, test.expectedDomainSchema)
			// The base type stays available; the fix adds a spelling rather
			// than replacing one.
			c.Assert(columnsByTable["t"][0].DataType, qt.Equals, "integer")
		})
	}
}
