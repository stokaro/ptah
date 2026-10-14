package oracle_test

import (
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/oracle"
)

// These catalog rows come from Oracle 23.26.3.0.0. Query projections are part
// of the fake so omitting a dimension fails through the public reader.
func TestReadSchema_PreservesNumericAndNationalTypeDimensions(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, nativeDatatypeCatalog)
	got, err := oracle.NewOracleReader(db.SQL, "APP").ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	domains, objectTypes := readTypes(c, got)
	c.Assert(domains, qt.DeepEquals, map[string]string{
		"INTEGER_DOMAIN":        "NUMBER(*,0)",
		"ROUNDED_DOMAIN":        "NUMBER(*,-2)",
		"NATIONAL_DOMAIN":       "NVARCHAR2(10)",
		"NATIONAL_FIXED_DOMAIN": "NCHAR(10)",
		"TEXT_DOMAIN":           "VARCHAR2(20)",
	})
	c.Assert(objectTypes, qt.DeepEquals, map[string][]oratype.Attribute{"RECORD_T": {
		{Name: "DECIMAL_N", Type: "DECIMAL(5,2)"},
		{Name: "DECIMAL_NEGATIVE", Type: "DECIMAL(5,-2)"},
		{Name: "DECIMAL_ZERO", Type: "DECIMAL(5)"},
		{Name: "DECIMAL_DEFAULT", Type: "DECIMAL"},
		{Name: "FLOAT_N", Type: "FLOAT"},
		{Name: "FLOAT_SIZED", Type: "FLOAT(10)"},
		{Name: "TEXT_N", Type: "NVARCHAR2(10)"},
		{Name: "TEXT_B", Type: "VARCHAR2(20)"},
	}})
}

// readTypes returns the base type of each domain and the attributes of each
// object type a read holds, by name.
func readTypes(c *qt.C, read *catalog.Database) (map[string]string, map[string][]oratype.Attribute) {
	objects, err := read.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	domains, objectTypes := make(map[string]string), make(map[string][]oratype.Attribute)
	for _, object := range objects {
		switch value := object.Value.(type) {
		case *oratype.ObservedDomain:
			domains[object.Ref.Name.Source] = value.BaseType
		case *oratype.ObservedObjectType:
			objectTypes[object.Ref.Name.Source] = value.Attributes
		}
	}
	return domains, objectTypes
}

func nativeDatatypeCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	switch folded := strings.ToLower(query); {
	case strings.Contains(folded, "from all_domains"):
		return projectedTypeRows(query, []string{"name", "cols", "type", "data_display", "data_order"}, [][]driver.Value{
			{"INTEGER_DOMAIN", int64(1), "REGULAR", nil, nil},
			{"ROUNDED_DOMAIN", int64(1), "REGULAR", nil, nil},
			{"NATIONAL_DOMAIN", int64(1), "REGULAR", nil, nil},
			{"NATIONAL_FIXED_DOMAIN", int64(1), "REGULAR", nil, nil},
			{"TEXT_DOMAIN", int64(1), "REGULAR", nil, nil},
		})
	case strings.Contains(folded, "from all_domain_cols"):
		return projectedTypeRows(query,
			[]string{"domain_name", "column_name", "data_type", "data_length", "data_precision", "data_scale", "char_length", "nullable", "data_default"},
			[][]driver.Value{
				{"INTEGER_DOMAIN", "INTEGER_DOMAIN", "NUMBER", int64(22), nil, int64(0), int64(0), "Y", nil},
				{"ROUNDED_DOMAIN", "ROUNDED_DOMAIN", "NUMBER", int64(22), nil, int64(-2), int64(0), "Y", nil},
				{"NATIONAL_DOMAIN", "NATIONAL_DOMAIN", "NVARCHAR2", int64(20), nil, nil, int64(10), "Y", nil},
				{"NATIONAL_FIXED_DOMAIN", "NATIONAL_FIXED_DOMAIN", "NCHAR", int64(20), nil, nil, int64(10), "Y", nil},
				{"TEXT_DOMAIN", "TEXT_DOMAIN", "VARCHAR2", int64(20), nil, nil, int64(20), "Y", nil},
			},
		)
	case strings.Contains(folded, "from all_type_attrs"):
		return projectedTypeRows(query,
			[]string{"type_name", "attr_name", "attr_type_name", "length", "precision", "scale", "char_used"},
			[][]driver.Value{
				{"RECORD_T", "DECIMAL_N", "DECIMAL", nil, int64(5), int64(2), "B"},
				{"RECORD_T", "DECIMAL_NEGATIVE", "DECIMAL", nil, int64(5), int64(-2), "B"},
				{"RECORD_T", "DECIMAL_ZERO", "DECIMAL", nil, int64(5), int64(0), "B"},
				{"RECORD_T", "DECIMAL_DEFAULT", "DECIMAL", nil, nil, int64(0), "B"},
				{"RECORD_T", "FLOAT_N", "FLOAT", nil, nil, nil, "B"},
				{"RECORD_T", "FLOAT_SIZED", "FLOAT", nil, int64(10), nil, "B"},
				{"RECORD_T", "TEXT_N", "NVARCHAR2", int64(10), nil, nil, "C"},
				{"RECORD_T", "TEXT_B", "VARCHAR2", int64(20), nil, nil, "B"},
			},
		)
	case strings.Contains(folded, "from all_types"):
		return projectedTypeRows(query, []string{"type_name", "typecode", "methods", "supertype_name", "incomplete", "final", "instantiable"},
			[][]driver.Value{{"RECORD_T", "OBJECT", int64(0), nil, "NO", "YES", "YES"}})
	default:
		return dbtest.QueryResult{Columns: []string{"unused"}}, nil
	}
}

// Only return columns the reader selects. Dropped or reordered projections
// affect this fake catalog just as they affect the server's result.
func projectedTypeRows(query string, columns []string, rows [][]driver.Value) (dbtest.QueryResult, error) {
	selectList, _, _ := strings.Cut(strings.Join(strings.Fields(strings.ToLower(query)), " "), " from ")
	selected := strings.Split(strings.TrimPrefix(selectList, "select "), ",")
	indices := make([]int, len(selected))
	for i, part := range selected {
		_, name, _ := strings.Cut(strings.TrimSpace(part), ".")
		indices[i] = slices.Index(columns, name)
		if indices[i] < 0 {
			return dbtest.QueryResult{}, fmt.Errorf("unexpected type catalog column %q", part)
		}
		selected[i] = name
	}
	result := dbtest.QueryResult{Columns: selected}
	for _, row := range rows {
		values := make([]driver.Value, len(indices))
		for i, index := range indices {
			values[i] = row[index]
		}
		result.Rows = append(result.Rows, values)
	}
	return result, nil
}
